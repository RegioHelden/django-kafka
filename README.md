# django-kafka
This library is using [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python) which is a wrapper around the [librdkafka](https://github.com/confluentinc/librdkafka) (Apache Kafka C/C++ client library).

It helps to integrate kafka with Django. 

## Quick start

```bash
pip install django-kafka
```

### Configure:
Considering you have locally setup kafka instance with no authentication. All you need is to define the bootstrap servers.
```python
# ./settings.py

INSTALLED_APPS = [
  # ...
  "django_kafka",
]

DJANGO_KAFKA = {
    "GLOBAL_CONFIG": {
      "bootstrap.servers": "kafka1:9092",
    },
}
```

### Define a Topic:

Topics define how to handle incoming messages and how to produce an outgoing message.
```python
from confluent_kafka.serialization import MessageField
from django_kafka.topic import Topic


class Topic1(Topic):
    name = "topic1"

    def consume(self, msg):
        key = self.deserialize(msg.key(), MessageField.KEY, msg.headers())
        value = self.deserialize(msg.value(), MessageField.VALUE, msg.headers())
        # ... process values
```

`Topic` inherits from the `TopicProducer` and `TopicConsumer` classes. If you only need to consume or produce messages, inherit from one of these classes instead to avoid defining unnecessary abstract methods. 

### Define a Consumer:

Consumers define which topics they take care of. Usually you want one consumer per project. If 2 consumers are defined, then they will be started in parallel.

Consumers are auto-discovered and are expected to be located under the `some_django_app/kafka/consumers.py` or `some_django_app/consumers.py`.

```python
# ./consumers.py

from django_kafka import kafka
from django_kafka.consumer import Consumer, Topics

from my_app.topics import Topic1


# register your consumer using `DjangoKafka` class API decorator
@kafka.consumers()
class MyAppConsumer(Consumer):
    # tell the consumers which topics to process using `django_kafka.consumer.Topics` interface.
    topics = Topics(
        Topic1(),
    )

    config = {
        "group.id": "my-app-consumer",
        "auto.offset.reset": "latest",
        "enable.auto.offset.store": False,
    }
```


### Start the Consumers:
You can use django management command to start defined consumers.
```bash
./manage.py kafka_consume
```
Or you can use `DjangoKafka` class API.
```python
from django_kafka import kafka

kafka.run_consumers()
```
Check [Confluent Python Consumer](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#consumer) for API documentation.



### Produce:
Message are produced using a Topic instance.
```python
from my_app.topics import Topic1

# this will send a message to kafka, serializing it using the defined serializer 
Topic1().produce("some message")
```
Check [Confluent Python Producer](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#producer) for API documentation.


### Define schema registry:

The library is using [Confluent's SchemaRegistryClient](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#schemaregistryclient). In order to use it define a `SCHEMA_REGISTRY` setting. 

Find available configs in the [SchemaRegistryClient docs](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#schemaregistryclient).
```python
DJANGO_KAFKA = {
    "SCHEMA_REGISTRY": {
      "url": "http://schema-registry",
    },
}
```

**Note:** take [django_kafka.topic.AvroTopic](./django_kafka/topic.py) as an example if you want to implement a custom Topic with your schema.

## Specialized Topics:

### ModelTopicConsumer:

`ModelTopicConsumer` can be used to sync django model instances from abstract kafka events. Simply inherit the class, set the model, the topic to consume from and define a few abstract methods.

```py

from django_kafka.topic.model import ModelTopicConsumer

from my_app.models import MyModel

class MyModelConsumer(ModelTopicConsumer):
    name = "topic"
    model = MyModel

    def is_deletion(self, model, key, value) -> bool:
        """returns if the message represents a deletion"""
        return value.pop('__deleted', False)
    
    def get_lookup_kwargs(self, model, key, value) -> dict:
        """returns the lookup kwargs used for filtering the model instance"""
        return {"id": key}
```

Model instances will have their attributes synced from the message value. If you need to alter a message key or value before it is assigned, define a `transform_{attr}` method:

```python

    class MyModelConsumer(ModelTopicConsumer):
        ...
        def transform_name(model, key, value):
            return 'first_name', value["name"].upper()
```

### DbzModelTopicConsumer:

`DbzModelTopicConsumer` helps sync model instances from [debezium source connector](https://debezium.io/documentation/reference/stable/architecture.html) topics. It inherits from `ModelTopicConsumer` and  defines default implementations for `is_deletion` and `get_lookup_kwargs` methods.

In Debezium it is possible to [reroute records](https://debezium.io/documentation/reference/stable/transformations/topic-routing.html) from multiple sources to the same topic. In doing so Debezium [inserts a table identifier](https://debezium.io/documentation/reference/stable/transformations/topic-routing.html#_ensure_unique_key) to the key to ensure uniqueness. When this key is inserted, you **must instead** define a `reroute_model_map` to map the table identifier to the model class to be created.

```py

from django_kafka.topic.debezium import DbzModelTopicConsumer

from my_app.models import MyModel, MyOtherModel

class MyModelConsumer(DbzModelTopicConsumer):
    name = "debezium_topic"
    reroute_model_map = {
        'public.my_model': MyModel,
        'public.my_other_model': MyOtherModel,
    }
```

A few notes:

1. The connector must be using the [event flattening SMT](https://debezium.io/documentation/reference/stable/transformations/event-flattening.html) to simplify the message structure.
2. [Deletions](https://debezium.io/documentation/reference/stable/transformations/event-flattening.html#extract-new-record-state-delete-tombstone-handling-mode) are detected automatically based on a null message value or the presence of a `__deleted` field.
3. The message key is assumed to contain the model PK as a field, [which is the default behaviour](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-property-message-key-columns) for Debezium source connectors. If you need more complicated lookup behaviour, override `get_lookup_kwargs`.

## Non-Blocking Retries:

Add non-blocking retry behaviour to a topic by using the `retry` decorator:

```python
from django_kafka import kafka
from django_kafka.topic import Topic


@kafka.retry(max_retries=3, delay=120, include=[ValueError])
class RetryableTopic(Topic):
    name = "topic"
    ...
```

When the consumption of a message in a retryable topic fails, the message is re-sent to a topic with a name combined of the consumer group id, the original topic name, a `.retry` suffix, and the retry number. Subsequent failed retries will then be sent to retry topics of incrementing retry number until the maximum attempts are reached, after which it will be sent to a dead letter topic suffixed by `.dlt`. So for a failed message in topic `topic` received by consumer group `group`, the expected topic sequence would be: 

1. `topic`
2. `group.topic.retry.1`
3. `group.topic.retry.2`
4. `group.topic.retry.3`
5. `group.topic.dlt`

When consumers are started using [start commands](#start-the-Consumers), an additional retry consumer will be started in parallel for any consumer containing a retryable topic. This retry consumer will be assigned to a consumer group whose id is a combination of the original group id and a `.retry` suffix. This consumer is subscribed to the retry topics, and manages the message retry and delay behaviour. Please note that messages are retried directly by the retry consumer and are not sent back to the original topic.

## Connectors:

Connectors are auto-discovered and are expected to be located under the `some_django_app/kafka/connectors.py` or `some_django_app/connectors.py`.

Connectors are defined as python classes decorated with `@kafka.connectors()` which adds the class to the global registry. 

`django_kafka.connect.connector.Connector` implements submission, validation and deletion of the connector configuration.

### Define connector:
```python
# Connectors are discovered automatically when placed under the connectors module
# e.g. ./connectors.py

from django_kafka import kafka
from django_kafka.connect.connector import Connector


@kafka.connectors()
class MyConnector(Connector):
    config = {
        # configuration for the connector
    }
```

### Mark a connector for removal:

```python
from django_kafka import kafka
from django_kafka.connect.connector import Connector


@kafka.connectors()
class MyConnector(Connector):
    mark_for_removal = True
    config = {
        # configuration for the connector
    }
```

### Manage connectors:

django-kafka provides `./manage.py kafka_connect` management command to manage your connectors.


#### Manage a single connector
```bash
./manage.py kafka_connect path.to.my.SpecialConnector --validate --publish --check-status --ignore-failures
````

#### Manage all connectors
```bash
./manage.py kafka_connect --validate --publish --check-status --ignore-failures
````

`--validate` - validates the config over the connect REST API

`--publish` - create or update the connector or delete when `mark_for_removal = True`

`--check-status` - check the status of the connector is `RUNNING`.

`--ignore-failures` - command wont fail if any of the connectors fail to validate or publish.

See `--help`.

## Settings:

**Defaults:**
```python
DJANGO_KAFKA = {
    "CLIENT_ID": f"{socket.gethostname()}-python",
    "ERROR_HANDLER": "django_kafka.error_handlers.ClientErrorHandler",
    "GLOBAL_CONFIG": {},
    "PRODUCER_CONFIG": {},
    "CONSUMER_CONFIG": {},
    "RETRY_CONSUMER_CONFIG": {
        "auto.offset.reset": "earliest",
        "enable.auto.offset.store": False,
        "topic.metadata.refresh.interval.ms": 10000,
    },
    "RETRY_TOPIC_SUFFIX": "retry",
    "DEAD_LETTER_TOPIC_SUFFIX": "dlt",
    "POLLING_FREQUENCY": 1,  # seconds
    "SCHEMA_REGISTRY": {},
    # Rest API of the kafka-connect instance
    "CONNECT_HOST": None,
    # `requests.auth.AuthBase` instance or tuple of (username, password) for Basic Auth
    "CONNECT_AUTH": None,
    # kwargs for `urllib3.util.retry.Retry` initialization
    "CONNECT_RETRY": dict(
        connect=5,
        read=5,
        status=5,
        backoff_factor=0.5,
        status_forcelist=[502, 503, 504],
    ),
    # `django_kafka.connect.client.KafkaConnectSession` would pass this value to every request method call
    "CONNECT_REQUESTS_TIMEOUT": 30,
    "CONNECTOR_NAME_PREFIX": "",
}
```

#### `CLIENT_ID`
Default: `f"{socket.gethostname()}-python"`

An id string to pass to the server when making requests. The purpose of this is to be able to track the source of requests beyond just ip/port by allowing a logical application name to be included in server-side request logging.

**Note:** This parameter is included in the config of both the consumer and producer unless `client.id` is overwritten within `PRODUCER_CONFIG` or `CONSUMER_CONFIG`.

#### `GLOBAL_CONFIG`
Default: `{}`

Defines configurations applied to both consumer and producer. See [configs marked with `*`](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).

#### `PRODUCER_CONFIG`
Default: `{}`

Defines configurations of the producer. See [configs marked with `P`](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).

#### `CONSUMER_CONFIG`
Default: `{}`

Defines configurations of the consumer. See [configs marked with `C`](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).

#### `POLLING_FREQUENCY`
Default: 1  # second

How often client polls for events.

#### `SCHEMA_REGISTRY`
Default: `{}`

Configuration for [confluent_kafka.schema_registry.SchemaRegistryClient](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#schemaregistryclient).

#### `ERROR_HANDLER`
Default: `django_kafka.error_handlers.ClientErrorHandler`

This is an `error_cb` hook (see [Kafka Client Configuration](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#kafka-client-configuration) for reference).
It is triggered for client global errors and in case of fatal error it raises `DjangoKafkaError`.

#### `CONNECT_HOST`
Default: `None`

Rest API of the kafka-connect instance.

#### `CONNECT_AUTH`
Default: `None`

`requests.auth.AuthBase` instance or `("username", "password")` for Basic Auth.

#### `CONNECT_AUTH`
Default: `dict(
    connect=5,
    read=5,
    status=5,
    backoff_factor=0.5,
    status_forcelist=[502, 503, 504],
)`

kwargs for `urllib3.util.retry.Retry` initialization.

#### `CONNECT_REQUESTS_TIMEOUT`
Default: `30`

`django_kafka.connect.client.KafkaConnectSession` would pass this value to every request method call.

#### `CONNECTOR_NAME_PREFIX`
Default: `""`

Prefix which will be added to the connector name when publishing the connector. 

`CONNECT_` settings are required for `./manage.py kafka_connect` command which talks to the Rest API of the kafka-connect instance.

Used by `django_kafka.connect.connector.Connector` to initialize `django_kafka.connect.client.KafkaConnectClient`.


## Suppressing producers:

`django-kafka` provides two ways to suppress producers:

### `producer.suppress`

Use the `producer.suppress` decorator and context manager to suppress the producing of messages generated by the `Producer` class during a particular context.

```python
from django_kafka import producer


@producer.suppress(["topic1"])  # suppress producers to topic1
def my_function():
    ...


def my_function_two():
    with producer.suppress(["topic1"]):  # suppress producers to topic1
        ...
```

`producer.suppress` can take a list of topic names, or no arguments to suppress producers of all topics. 

Use `producer.unsuppress` to deactivate any set suppression during a specific context.


### `KafkaConnectSkipModel.kafka_skip`

Pythonic suppression methods will not suffice when using Kafka Connect to directly produce events from database changes. In this scenario, it's more appropriate to add a flag to the model database table which indicates if the connector should generate an event. Two classes are provided subclassing Django's Model and QuerySet to manage this flag:

#### KafkaConnectSkipModel
Adds the `kafka_skip` boolean field, defaulting to `False`. This also automatically resets `kafka_skip` to `False` when saving instances (if not explicitly set).

Usage:

```python
from django.contrib.auth.base_user import AbstractBaseUser
from django.contrib.auth.models import PermissionsMixin
from django_kafka.connect.models import KafkaConnectSkipModel


class User(KafkaConnectSkipModel, PermissionsMixin, AbstractBaseUser):
    # ...
```


#### KafkaConnectSkipQueryset
If you have defined a custom manager on your model then you should inherit it from `KafkaConnectSkipQueryset`. It adds `kafka_skip=False` when using the `update` method.

**Note:** `kafka_skip=False` is only set when it's not provided to the `update` kwargs. E.g. `User.objects.update(first_name="John", kafka_skip=True)` will not be changed to `kafka_skip=False`.

Usage:

```python
from django.contrib.auth.base_user import AbstractBaseUser
from django.contrib.auth.base_user import BaseUserManager
from django.contrib.auth.models import PermissionsMixin
from django_kafka.connect.models import KafkaConnectSkipModel, KafkaConnectSkipQueryset


class UserManager(BaseUserManager.from_queryset(KafkaConnectSkipQueryset)):


# ...


class User(KafkaConnectSkipModel, PermissionsMixin, AbstractBaseUser):
    # ...
    objects = UserManager()
```

## Bidirectional data sync with no infinite event loop:

**For example, you want to keep a User table in sync in multiple systems.**

### Infinite loop

You are likely to encounter infinite message generation when syncing data between multiple systems. Message suppression helps overcome this issue.

For purely pythonic producers and consumers, the `produce.suppress` decorator can be used to suppress messages produced during consumption. If you wish to do this globally for all consuming, use the decorator in your `Consumer` class:

```python
from django_kafka import producer
from django_kafka.consumer import Consumer

class MyConsumer(Consumer):
    
    @producer.suppress
    def consume(self, *args, **kwargs):
        super().consume(*args, **kwargs)
```

When producing with Kafka Connect, the `KafkaConnectSkipModel` provides the `kafka_skip` flag; the record should be manually marked with `kafka_skip=True` at consumption time and the connector should be configured not to send events when this flag is set.

### Global message ordering

To maintain global message ordering between systems, all events for the same database table should be sent to the same topic. The disadvantage is that each system will still consume its own message. 


## Making a new release
- [bump-my-version](https://github.com/callowayproject/bump-my-version) is used to manage releases.
- [Ruff](https://github.com/astral-sh/ruff) linter is used to validate the code style. Make sure your code complies withg the defined rules. You may use `ruff check --fix` for that. Ruff is executed by GitHub actions and the workflow will fail if Ruff validation fails. 

- Add your changes to the [CHANGELOG](CHANGELOG.md), then run
```bash
docker compose run --rm app bump-my-version bump <major|minor|patch>
```
This will update version major/minor/patch version respectively and add a tag for release.

- Once the changes are approved and merged, push the tag to publish the release to pypi.
```bash
git push origin tag <tag_name>
```
