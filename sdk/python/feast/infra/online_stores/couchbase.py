from pydantic import StrictStr
from pydantic.schema import Literal

import base64
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

# needed for any cluster connection
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
# needed for options -- cluster, timeout, SQL++ (N1QL) query, etc.
from couchbase.options import ClusterOptions, QueryOptions
from couchbase.management.collections import CollectionSpec
from couchbase.management.logic.buckets_logic import CreateBucketSettings, BucketType, EvictionPolicyType, CompressionMode, EjectionMethod, StorageBackend, ConflictResolutionType
import couchbase.subdocument as SD
from couchbase.exceptions import CollectionAlreadyExistsException, CollectionNotFoundException, BucketNotFoundException, DocumentExistsException

from feast import Entity
from feast.feature_view import FeatureView
from feast.infra.infra_object import InfraObject
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.usage import log_exceptions_and_usage
from feast.utils import to_naive_utc


class CouchbaseOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for local (Couchbase-based) store"""

    type: Literal[
        "couchbase"
    ] = "couchbase"
    """ Online store type selector """

    path: StrictStr = "couchbase://localhost"
    """ Path to couchbase db """

    username: StrictStr = "username"
    """ Username for connection to couchbase db """

    password: StrictStr = "password"
    """ Password for connection to couchbase db """

    timeout_seconds: Optional[int] = 5
    """ (optional) connection timeout in seconds """


class CouchbaseOnlineStore(OnlineStore):
    """
    The interface that Feast uses to interact with the storage system that handles online features.
    """

    _cluster: Cluster = None

    @staticmethod
    def _get_db_path(config: RepoConfig) -> str:
        assert (
            config.online_store.type == "couchbase"
            or config.online_store.type.endswith("CouchbaseOnlineStore")
        )

        return config.online_store.path

    def _get_collection(
        self,
        config: RepoConfig,
        table: FeatureView,
        cluster: Cluster = None
    ):
        return self._get_scope(
            config,
            cluster=cluster
        ).collection(table.name)

    def _get_scope(
        self,
        config: RepoConfig,
        cluster: Cluster = None
    ):
        return self._get_bucket(
            config,
            cluster=cluster
        ).scope("_default")

    def _get_bucket(
        self,
        config: RepoConfig,
        make_if_missing: bool = False,
        cluster: Cluster = None
    ):
        if cluster is None:
            cluster = self._get_cluster(config)
        return cluster.bucket(config.project)

    def _get_cluster(
        self,
        config: RepoConfig,
    ):
        if self._cluster is None:
            self._cluster = Cluster(
                self._get_db_path(config),
                ClusterOptions(
                    authenticator=PasswordAuthenticator(
                        config.online_store.username,
                        config.online_store.password,
                    ),
                )
            )
        # Wait until the cluster is ready for use.
        self._cluster.wait_until_ready(timedelta(seconds=config.online_store.timeout_seconds))
        return self._cluster

    @log_exceptions_and_usage(online_store="couchbase")
    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        """
        Writes a batch of feature rows to the online store.

        If a tz-naive timestamp is passed to this method, it is assumed to be UTC.

        Args:
            config: The config for the current feature store.
            table: Feature view to which these feature rows correspond.
            data: A list of quadruplets containing feature data. Each quadruplet contains an entity
                key, a dict containing feature values, an event timestamp for the row, and the created
                timestamp for the row if it exists.
            progress: Function to be called once a batch of rows is written to the online store, used
                to show progress.
        """

        cb_collection = self._get_collection(config, table)

        for entity_key, values, timestamp, created_ts in data:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            entity_key_bin = base64.b64encode(
                entity_key_bin
            ).decode('ascii')

            timestamp = to_naive_utc(timestamp)
            if created_ts is None:
                created_ts = datetime.now()
            created_ts = to_naive_utc(created_ts)

            try:
                cb_collection.insert(
                    entity_key_bin,
                    {}
                )
            except DocumentExistsException:
                pass

            cb_collection.mutate_in(
                entity_key_bin,
                [
                    SD.upsert(
                        feature_name,
                        {
                            "value": base64.b64encode(
                                val.SerializeToString()
                            ).decode('ascii'),
                            "event_ts": timestamp.isoformat(),
                            "created_ts": created_ts.isoformat(),
                        }
                    )
                    for feature_name, val in values.items()
                ]
            )
            if progress:
                progress(1)

    @log_exceptions_and_usage(online_store="couchbase")
    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """
        Reads features values for the given entity keys.

        Args:
            config: The config for the current feature store.
            table: The feature view whose feature values should be read.
            entity_keys: The list of entity keys for which feature values should be read.
            requested_features: The list of features that should be read.

        Returns:
            A list of the same length as entity_keys. Each item in the list is a tuple where the first
            item is the event timestamp for the row, and the second item is a dict mapping feature names
            to values, which are returned in proto format.
        """
        entity_key_bins = list(map(
            lambda entity_key: base64.b64encode(
                serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                )
            ).decode('ascii'),
            entity_keys
        ))

        cb_cluster = self._get_cluster(config)
        cb_query_result = cb_cluster.query(
            f"SELECT meta({table.name}).id as _document_id_, * from `{config.project}`._default.{table.name}"
            f" WHERE meta({table.name}).id IN $1",
            QueryOptions(
                read_only=True,
                positional_parameters=entity_key_bins
            )
        )
        cb_query_result = {
            r['_document_id_']: r[table.name]
            for r in cb_query_result.rows()
        }

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for entity_key_bin in entity_key_bins:
            cb_res = cb_query_result.get(entity_key_bin, None)
            if cb_res is None:
                result.append((None, None))
                continue

            res = {}
            res_ts = None
            for feature_name, feature in cb_res.items():
                val = ValueProto()
                val.ParseFromString(
                    base64.b64decode(
                        feature["value"].encode('ascii')
                    ),
                )
                res[feature_name] = val
                res_ts = datetime.fromisoformat(feature["event_ts"])

                result.append((res_ts, res))
        return result

    @log_exceptions_and_usage(online_store="couchbase")
    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        """
        Reconciles cloud resources with the specified set of Feast objects.

        Args:
            config: The config for the current feature store.
            tables_to_delete: Feature views whose corresponding infrastructure should be deleted.
            tables_to_keep: Feature views whose corresponding infrastructure should not be deleted, and
                may need to be updated.
            entities_to_delete: Entities whose corresponding infrastructure should be deleted.
            entities_to_keep: Entities whose corresponding infrastructure should not be deleted, and
                may need to be updated.
            partial: If true, tables_to_delete and tables_to_keep are not exhaustive lists, so
                infrastructure corresponding to other feature views should be not be touched.
        """
        cluster = self._get_cluster(config)
        try:
            collection_manager = self._get_bucket(config).collections()
        except BucketNotFoundException:
            cluster.buckets().create_bucket(
                CreateBucketSettings(
                    name=config.project,  # str. name of the bucket
                    flush_enabled=False,  # bool. whether flush is enabled
                    ram_quota_mb=1024,  # int. raw quota in megabytes
                    num_replicas=0,  # int. number of replicas
                    replica_index=False,  # bool. whether this is a replica index
                    bucket_type=BucketType.COUCHBASE,  # BucketType. type of bucket
                    eviction_policy=EvictionPolicyType.VALUE_ONLY,  # EvictionPolicyType. policy for eviction
                    max_ttl=0,  # Union[timedelta,float,int]. **DEPRECATED** max time to live for bucket
                    max_expiry=timedelta(5),  # Union[timedelta,float,int]. max expiry time for bucket
                    compression_mode=CompressionMode.OFF,  # CompressionMode. compression mode
                    ejection_method=EjectionMethod.FULL_EVICTION,  # EjectionMethod. ejection method (deprecated, please use eviction_policy instead)
                    storage_backend=StorageBackend.COUCHSTORE,  # StorageBackend. **UNCOMMITTED** specifies the storage type to use for the bucket
                    bucket_password="",  # str
                    conflict_resolution_type=ConflictResolutionType.TIMESTAMP,  # ConflictResolutionType
                )
            )
            collection_manager = self._get_bucket(config).collections()

        for table in tables_to_keep:
            try:
                collection_manager.create_collection(
                    CollectionSpec(
                        table.name,
                        # "_default" # scope name
                    )
                )
                list(cluster.query(f"CREATE PRIMARY INDEX ON `{config.project}`"))
                list(cluster.query(f"CREATE PRIMARY INDEX ON default:`{config.project}`._default.{table.name}"))
            except CollectionAlreadyExistsException:
                pass

        for table in tables_to_delete:
            try:
                collection_manager.drop_collection(
                    CollectionSpec(
                        table.name,
                        # "_default" # scope name
                    )
                )
                list(cluster.query(f"DROP PRIMARY INDEX ON `{config.project}`"))
                list(cluster.query(f"DROP PRIMARY INDEX ON default:`{config.project}`._default.{table.name}"))
            except CollectionNotFoundException:
                pass

    def plan(
        self, config: RepoConfig, desired_registry_proto: RegistryProto
    ) -> List[InfraObject]:
        """
        Returns the set of InfraObjects required to support the desired registry.

        Args:
            config: The config for the current feature store.
            desired_registry_proto: The desired registry, in proto form.
        """
        return []

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        """
        Tears down all cloud resources for the specified set of Feast objects.

        Args:
            config: The config for the current feature store.
            tables: Feature views whose corresponding infrastructure should be deleted.
            entities: Entities whose corresponding infrastructure should be deleted.
        """
        bucket_manager = self._get_cluster(config).buckets()
        bucket_manager.flush_bucket(config.project)
        bucket_manager.drop_bucket(config.project)
