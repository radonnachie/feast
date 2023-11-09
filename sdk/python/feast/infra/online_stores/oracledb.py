# Copyright 2021 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import itertools
import os
from datetime import datetime
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import oracledb
from pydantic import StrictStr
from pydantic.schema import Literal
from string import Template

from feast import Entity
from feast.feature_view import FeatureView
# from feast.infra.infra_object import (ORACLEDB_INFRA_OBJECT_CLASS_TYPE,
#                                       InfraObject)
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.usage import log_exceptions_and_usage, tracing_span
from feast.utils import to_naive_utc

# returns strings or bytes instead of a locator
# will have to revise if LOBS are > 1 GB
oracledb.defaults.fetch_lobs = False


class OracleDBOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for local (SQLite-based) store"""

    type: Literal[
        "oracledb", "feast.infra.online_stores.oracledb.OracleDBOnlineStore"
    ] = "oracledb"
    """ Online store type selector"""

    config_dir: Optional[str] = "/opt/oracle/config"
    """ Directory containing the tnsnames.ora file """

    dsn: StrictStr = "data source name"
    """ Oracle DB """

    username: StrictStr = "username"
    """ Username for connection to Oracle DB """

    password: StrictStr = "password"
    """ Password for connection to Oracle DB """

    alter_table_option: Optional[str] = None
    """ If provided, defines the ALTER TABLE that will be executed on each table created """
    
    write_batch_statement_size: Optional[int] = 50
    """ Specifies the number of records that are inserted in a single statement of the write batch procedure """
    
    write_batch_size: Optional[int] = 100
    """ Specifies how many statements to submit at a time for the write batch procedure """
    
    connection_pool_max_size: Optional[int] = 1
    """ Specifies the upper limit of the connection pool """

    connection_pool_increment_size: Optional[int] = 1
    """ Specifies the increment size of the connection pool """


class OracleDBOnlineStore(OnlineStore):
    """
    Oracle DB implementation of the online store interface.

    Attributes:
        _conn: Oracle DB connection.
    """

    _connpool: Optional[oracledb.ConnectionPool] = None

    def __del__(self):
        if self._connpool:
            self._connpool.close(force=True)
    
    def _get_conn(self, config: RepoConfig):
        if not self._connpool:
            self._connpool = oracledb.create_pool(
                user=config.online_store.username,
                password=config.online_store.password,
                dsn=config.online_store.dsn,
                config_dir=config.online_store.config_dir,
                min=1,
                max=config.online_store.connection_pool_max_size,
                increment=config.online_store.connection_pool_increment_size
            )
        return self._connpool.acquire()

    @staticmethod
    def _batch_feature_views(config, data):
        batch_size = config.online_store.write_batch_statement_size

        value_map = [None] * batch_size
        data_generator = iter(data)
        data_tuple = next(data_generator, None)
        while data_tuple is not None:
            value_map = {}
            for enum in range(batch_size):
                entity_key, values, timestamp, created_ts = data_tuple
                value_map.update(
                    dict(
                        [
                            (
                                f"{feature_name}_{enum}",
                                value.SerializeToString()
                            )
                            for feature_name, value in values.items()
                        ],
                        **{
                            f"entity_key_{enum}": serialize_entity_key(
                                entity_key,
                                entity_key_serialization_version=config.entity_key_serialization_version,
                            ),
                            f"event_ts_{enum}": to_naive_utc(timestamp),
                            f"created_ts_{enum}": None if created_ts is None else to_naive_utc(created_ts),
                        }
                    )
                )

                data_tuple = next(data_generator, None)
                if data_tuple is None:
                    batch_size = enum+1
                    break
            yield batch_size, value_map

    @staticmethod
    def _generate_merge_statement(table_id, feature_names, entity_count):
        select_line = " UNION ALL\n".join([
            f"SELECT :entity_key_{i} entity_key, '{feature_name}' feature_name, :{feature_name}_{i} value, :event_ts_{i} event_ts, :created_ts_{i} created_ts FROM DUAL"
            for i in range(entity_count) for feature_name in feature_names
        ])
        return f"""
        MERGE INTO {table_id} tt
        USING (
            {select_line}
        ) vt
        ON ( tt.entity_key = vt.entity_key and tt.feature_name = vt.feature_name )
        WHEN NOT MATCHED THEN
            insert ( tt.entity_key, tt.feature_name, tt.value, tt.event_ts, tt.created_ts )
            values ( vt.entity_key, vt.feature_name, vt.value, vt.event_ts, vt.created_ts )
        WHEN MATCHED THEN
            update set tt.value = vt.value, tt.event_ts = vt.event_ts, tt.created_ts = vt.created_ts
        """

    @log_exceptions_and_usage(online_store="oracledb")
    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:

        if len(data) == 0:
            return


        project = config.project
        with self._get_conn(config) as conn:
            with conn.cursor() as cursor:
                # each entry has a composite key of (entity_key, feature_name)
                # and the number of feature-names is the same for entity for this FeatureView.
                # So make each `merge into` statement select all the feature-names for each entity,
                # and `executemany` over all the entities

                # peak the first entity's features to ascertain the feature-names that will be repeated
                feature_names = data[0][1].keys()

                size_of_batches = 0
                merge_statement = None
                batches = None
                for batch_size, batch_feature_value_map in self._batch_feature_views(config, data):
                    if (
                        batches is not None
                        and batch_size == size_of_batches
                        and len(batches) < config.online_store.write_batch_size
                    ):
                        batches.append(batch_feature_value_map)
                    else:
                        if batches is not None:
                            # push batches
                            cursor.executemany(
                                merge_statement,
                                batches
                            )
                        if batch_size != size_of_batches:
                            size_of_batches = batch_size
                            merge_statement = self._generate_merge_statement(
                                _table_id(project, table),
                                feature_names,
                                batch_size
                            )
                        batches = [batch_feature_value_map]

                    if progress:
                        progress(batch_size)

                if batches is not None:
                    # push batches
                    cursor.executemany(
                        merge_statement,
                        batches
                    )
                    batches = None
                conn.commit()

    @log_exceptions_and_usage(online_store="oracledb")
    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        result: List[Tuple[
            Optional[datetime],
            Optional[Dict[str, ValueProto]]
            ]
        ] = []

        # abide by 50 term where clause limit,
        # with "or in" clauses for extensions
        where_clause_splits = [
            ', '.join(
                f":{i}"
                for i in range(splits*50, min(len(entity_keys), (splits+1)*50))
            )
            for splits in range((len(entity_keys)+49)//50)
        ]
        where_clause_terms = ') or entity_key in ('.join(where_clause_splits)
        
        entity_key_bins = [
            serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            for entity_key in entity_keys
        ]
        with tracing_span(name="remote_call"):
            # Fetch all entities in one go
            with self._get_conn(config) as conn:
                with conn.cursor() as cursor:
                    cursor.arraysize = len(entity_keys)
                    if requested_features is not None:
                        cursor.arraysize *= len(requested_features)
                    cursor.prefetchrows = cursor.arraysize + 1

                    cursor.execute(
                        f"SELECT entity_key, feature_name, value, event_ts "
                        f"FROM {_table_id(config.project, table)} "
                        f"WHERE entity_key IN ({where_clause_terms})",
                        entity_key_bins
                    )
                    rows = cursor.fetchall()

        rows = {
            k: list(group)
            for k, group in itertools.groupby(rows, key=lambda r: r[0])
        }
        for entity_key_bin in entity_key_bins:
            if entity_key_bin not in rows:
                result.append((None, None))
                continue
            
            res_ts = None
            res = {}
            for _, feature_name, val_bin, ts in rows[entity_key_bin]:
                val = ValueProto()
                val.ParseFromString(val_bin)
                res[feature_name] = val
                res_ts = ts

            result.append((res_ts, res))

        return result

    @log_exceptions_and_usage(online_store="oracledb")
    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        project = config.project

        table_creation_query_lines = [
            "BEGIN",
            "    BEGIN",
            "        EXECUTE IMMEDIATE 'CREATE TABLE :table_id (entity_key RAW(2000), feature_name VARCHAR2(255), value BLOB, event_ts timestamp, created_ts timestamp, primary key(entity_key, feature_name))';",
            "        EXECUTE IMMEDIATE 'CREATE INDEX :table_id_ek ON :table_id (entity_key)';",
            "    EXCEPTION",
            "        WHEN OTHERS THEN",
            "            IF SQLCODE <> -955 THEN",
            "                RAISE;",
            "            END IF;",
            "    END;",
            "END;",
        ]
        if config.online_store.alter_table_option is not None:
            table_creation_query_lines.insert(
                3,
                f"        EXECUTE IMMEDIATE 'ALTER TABLE {config.online_store.username}.:table_id {config.online_store.alter_table_option}';",
            )
        table_creation_query = "\n".join(table_creation_query_lines)
        table_deletion_query = """
        DECLARE
            table_not_exists EXCEPTION;
            PRAGMA EXCEPTION_INIT (table_not_exists, -942);
            index_not_exists EXCEPTION;
            PRAGMA EXCEPTION_INIT (index_not_exists, -1418);
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE :table_id cascade constraints';
            EXECUTE IMMEDIATE 'DROP INDEX :table_id_ek';
        EXCEPTION
            WHEN table_not_exists THEN
                NULL;
            WHEN index_not_exists THEN
                NULL;
            WHEN OTHERS THEN
                RAISE;
        END;
        """

        with self._get_conn(config) as conn:
            with conn.cursor() as cursor:
                for table in tables_to_keep:
                    cursor.execute(
                        table_creation_query.replace(
                            ":table_id",
                            _table_id(project, table),
                        )
                    )
                for table in tables_to_delete:
                    cursor.execute(
                        table_deletion_query.replace(
                            ":table_id",
                            _table_id(project, table),
                        )
                    )

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        self.update(
            config,
            tables_to_delete=tables,
            tables_to_keep=[],
            entities_to_delete=entities,
            entities_to_keep=[],
            partial=False
        )

def _table_id(project: str, table: FeatureView) -> str:
    return f"{project}_{table.name}"
