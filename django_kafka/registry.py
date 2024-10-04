from typing import Generic, TYPE_CHECKING, Type, TypeVar

from django_kafka.exceptions import DjangoKafkaError

if TYPE_CHECKING:
    from django_kafka.consumer import Consumer
    from django_kafka.connect.connector import Connector


T = TypeVar('T')


class Registry(Generic[T]):
    def __init__(self):
        self._classes: dict[str, Type[T]] = {}

    def __call__(self):
        def add_to_registry(cls: Type[T]) -> Type[T]:
            self.register(cls)
            return cls

        return add_to_registry

    def __getitem__(self, key: str):
        try:
            return self._classes[key]
        except KeyError as error:
            raise DjangoKafkaError(f"`{key}` is not registered.") from error

    def __iter__(self):
        yield from self._classes.keys()

    def get_key(self, cls: Type[T]) -> str:
        return f"{cls.__module__}.{cls.__name__}"

    def register(self, cls: Type[T]):
        key = self.get_key(cls)
        if key in self._classes:
            raise DjangoKafkaError(f"`{key}` is already registered.")
        self._classes[key] = cls


class ConnectorsRegistry(Registry["Connector"]):
    def get_key(self, cls) -> str:
        return cls.name


class ConsumersRegistry(Registry["Consumer"]):

    def register(self, cls):
        from django_kafka.retry.consumer import RetryConsumer
        
        super().register(cls)
        
        if retry_consumer_cls := RetryConsumer.build(cls):
            self._classes[f"{self.get_key(cls)}.retry"] = retry_consumer_cls
