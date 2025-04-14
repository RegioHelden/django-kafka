import requests
from requests.adapters import HTTPAdapter
from requests.auth import AuthBase
from urllib3.util import Retry

from django_kafka.exceptions import DjangoKafkaError


class KafkaConnectSession(requests.Session):
    def __init__(
        self,
        host: str,
        auth: tuple | AuthBase | None = None,
        retry: dict | None = None,
        timeout: int | None = None,
    ):
        super().__init__()
        self.auth = auth
        self.host = host
        self.timeout = timeout
        if retry:
            self.mount_retry_adapter(retry)

    def mount_retry_adapter(self, retry: dict):
        self.mount(self.host, HTTPAdapter(max_retries=Retry(**retry)))

    def request(self, method, url, *args, **kwargs) -> requests.Response:
        kwargs.setdefault("timeout", self.timeout)
        return super().request(method, f"{self.host}{url}", *args, **kwargs)


class KafkaConnectClient:
    """
    https://kafka.apache.org/documentation/#connect_rest
    https://docs.confluent.io/platform/current/connect/references/restapi.html
    """

    def __init__(
        self,
        host: str,
        auth: tuple | AuthBase | None = None,
        retry: dict | None = None,
        timeout: int | None = None,
    ):
        self._requests = KafkaConnectSession(host, auth, retry, timeout)

    def update_or_create(self, connector_name: str, config: dict):
        return self._requests.put(f"/connectors/{connector_name}/config", json=config)

    def delete(self, connector_name: str):
        return self._requests.delete(f"/connectors/{connector_name}")

    def validate(self, config: dict):
        if not config.get("connector.class"):
            raise DjangoKafkaError(
                "'connector.class' config is required for validation.",
            )

        connector_class_name = config.get("connector.class").rsplit(".", 1)[-1]
        return self._requests.put(
            f"/connector-plugins/{connector_class_name}/config/validate",
            json=config,
        )

    def connector_status(self, connector_name: str):
        """
        https://docs.confluent.io/platform/current/connect/references/restapi.html#get--connectors-(string-name)-status
        """
        return self._requests.get(f"/connectors/{connector_name}/status")
