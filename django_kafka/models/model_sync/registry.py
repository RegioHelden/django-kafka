from collections import defaultdict
from collections.abc import Iterator
from pydoc import locate
from typing import TYPE_CHECKING

from django_kafka import kafka
from django_kafka.conf import settings
from django_kafka.connect.connector import Connector
from django_kafka.models.model_sync.sink import ConnectorSink
from django_kafka.models.model_sync.sink.python import PythonSink
from django_kafka.models.model_sync.source import ConnectorSource
from django_kafka.registry import Registry

if TYPE_CHECKING:
    from django_kafka.consumer import Consumer
    from django_kafka.topic import TopicConsumer

    from .sync import ModelSync


class ModelSyncRegistry(Registry["ModelSync"]):
    def __init__(self):
        super().__init__()
        self._sources: dict[str, list[type[ModelSync]]] = defaultdict(list)
        self._topics: dict[str, list[TopicConsumer]] = defaultdict(list)

    def register(self, model_sync_cls: type["ModelSync"]):
        super().register(model_sync_cls)
        self._register_connector_source(model_sync_cls)
        self._register_connector_sink(model_sync_cls)
        self._register_python_sink(model_sync_cls)
        self._register_enricher(model_sync_cls)

    def _register_connector_source(self, model_sync_cls: type["ModelSync"]):
        if not isinstance(model_sync_cls.source, ConnectorSource):
            return

        connector_path = model_sync_cls.source.connector_path
        if not connector_path:
            raise ValueError(
                f"{model_sync_cls.__name__}.source is a ConnectorSource but no "
                f"connector is configured. Set `connector` on the source or "
                f"set MODEL_SYNC_SOURCE_CONNECTOR in DJANGO_KAFKA settings.",
            )
        self._sources[connector_path].append(model_sync_cls)

    def _register_connector_sink(self, model_sync_cls: type["ModelSync"]):
        if not isinstance(model_sync_cls.sink, ConnectorSink):
            return

        def config(self):
            return model_sync_cls().sink.config

        connector_cls = type(
            f"{model_sync_cls.__name__}SinkConnector",
            (Connector,),
            {"config": property(config)},
        )
        kafka.connectors.register(connector_cls)

    def _register_python_sink(self, model_sync_cls: type["ModelSync"]):
        if not isinstance(model_sync_cls.sink, PythonSink):
            return

        consumer_path = model_sync_cls.sink.consumer_path
        self._ensure_consumer_registered(consumer_path)
        self._topics[consumer_path].append(
            model_sync_cls().sink.make_topic(),
        )

    def _register_enricher(self, model_sync_cls: type["ModelSync"]):
        from django_kafka.models.model_sync.enricher import ModelSyncEnricher

        if not model_sync_cls.has_enrich():
            return

        consumer_path = (
            model_sync_cls.enricher_consumer
            or settings.MODEL_SYNC_ENRICHER_CONSUMER
        )
        self._ensure_consumer_registered(consumer_path)
        self._topics[consumer_path].append(
            ModelSyncEnricher.for_sync(model_sync_cls),
        )

    def get_for_connector(
        self, connector: "Connector",
    ) -> Iterator["ModelSync"]:
        connector_path = self.get_key(connector.__class__)
        for model_sync_cls in self._sources.get(
            connector_path, [],
        ):
            yield model_sync_cls()

    def get_topics_for_consumer(
        self, consumer: "Consumer",
    ) -> Iterator["TopicConsumer"]:
        yield from self.get_topics_for_consumer_class(consumer.__class__)

    def get_topics_for_consumer_class(
        self, consumer_cls: type["Consumer"],
    ) -> Iterator["TopicConsumer"]:
        consumer_path = self.get_key(consumer_cls)
        yield from self._topics.get(consumer_path, [])

    @staticmethod
    def _ensure_consumer_registered(consumer_path: str):
        consumer_cls = locate(consumer_path)
        if consumer_cls is None:
            raise ValueError(
                f"Consumer '{consumer_path}' not found.",
            )
        if consumer_cls not in kafka.consumers:
            kafka.consumers.register(consumer_cls)


model_sync_registry = ModelSyncRegistry()
