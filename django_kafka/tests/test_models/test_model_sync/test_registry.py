from unittest import TestCase, mock

from django_kafka.connect.connector import Connector
from django_kafka.models.model_sync.registry import ModelSyncRegistry
from django_kafka.models.model_sync.sink.dbz_jdbc import DbzJdbcSink
from django_kafka.models.model_sync.source.dbz_postgres import DbzPostgresSource

from .factories import make_sync


class _Connector(Connector):
    """Standalone Connector used as the registration target in tests."""

    config = {}


_CONNECTOR_PATH = f"{_Connector.__module__}.{_Connector.__name__}"


@mock.patch("django_kafka.conf.settings.CONNECT_HOST", "http://kafka-connect")
class GetForConnectorTestCase(TestCase):
    def test_yields_sync_for_matching_connector(self):
        registry = ModelSyncRegistry()
        make_sync(registry, source=DbzPostgresSource(connector=_CONNECTOR_PATH))

        syncs = list(registry.get_for_connector(_Connector()))
        self.assertEqual(len(syncs), 1)

    def test_yields_nothing_for_non_matching_connector(self):
        registry = ModelSyncRegistry()
        make_sync(registry, source=DbzPostgresSource(connector="other.Connector"))

        syncs = list(registry.get_for_connector(_Connector()))
        self.assertEqual(len(syncs), 0)

    def test_yields_nothing_without_source(self):
        registry = ModelSyncRegistry()
        make_sync(registry, source=None, sink=DbzJdbcSink())

        syncs = list(registry.get_for_connector(_Connector()))
        self.assertEqual(len(syncs), 0)
