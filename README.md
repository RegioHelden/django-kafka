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

### Define a Consumer:

Consumers define which topics they take care of. Usually you want one consumer per project. If 2 consumers are defined, then they will be started in parallel.

Consumers are auto-discovered and are expected to be located under the `consumers.py`.

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

kafka.start_consumers()
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

## Settings:

**Defaults:**
```python
DJANGO_KAFKA = {
    "CLIENT_ID": f"{socket.gethostname()}-python",
    "GLOBAL_CONFIG": {},
    "PRODUCER_CONFIG": {},
    "CONSUMER_CONFIG": {},
    "POLLING_FREQUENCY": 1,  # seconds
    "SCHEMA_REGISTRY": {},
    "ERROR_HANDLER": "django_kafka.error_handlers.ClientErrorHandler",
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
It is triggered for client global errors and in case of fatal error it raises `DjangoKafkaException`.


## Bidirectional data sync with no infinite event loop.

**For example, you want to keep a User table in sync in multiple systems.**

The idea is to send events from all systems to the same topic, and also consume events from the same topic, marking the record with `kafka_skip=True` at the consumption time.
- Producer should respect `kafka_skip=True` and do not produce new events when `True`.
- Any updates to the User table, which are happening outside the consumer, should set `kafka_skip=False` which will allow the producer to create an event again.

This way the chronology is strictly kept and the infinite events loop is avoided.

The disadvantage is that each system will still consume its own message.

#### There are 2 mixins for django Model and for QuerySet:

#### KafkaSkipMixin
It adds new `kafka_skip` boolean field, which defaults to `False`. And overrides `Model.save` method and sets `kafka_skip=False`.

Usage:
```python
from django.contrib.auth.base_user import AbstractBaseUser
from django.contrib.auth.models import PermissionsMixin
from django_kafka.models import KafkaSkipMixin

class User(KafkaSkipMixin, PermissionsMixin, AbstractBaseUser):
    # ...
```


#### KafkaSkipQueryset
If you have defined a custom manager on your model then you should inherit it from `KafkaSkipQueryset`. It adds `kafka_skip=False` when using `update` method.

**Note:** `kafka_skip=False` is only set when it's not provided to the `update` kwargs. E.g. `User.objects.update(first_name="John", kafka_skip=True)` will not be changed to `kafka_skip=False`.

Usage:
```python
from django.contrib.auth.base_user import AbstractBaseUser
from django.contrib.auth.base_user import BaseUserManager
from django.contrib.auth.models import PermissionsMixin
from django_kafka.models import KafkaSkipMixin, KafkaSkipQueryset


class UserManager(BaseUserManager.from_queryset(KafkaSkipQueryset)):
    # ...


class User(KafkaSkipMixin, PermissionsMixin, AbstractBaseUser):
    # ...
    objects = UserManager()
```


## Making a new release
- [bump-my-version](https://github.com/callowayproject/bump-my-version) is used to manage releases.
- [Ruff](https://github.com/astral-sh/ruff) linter is used to validate the code style. Make sure your code complies withg the defined rules. You may use `ruff check --fix` for that. Ruff is executed by GitHub actions and the workflow will fail if Ruff validation fails. 

- Add your changes to the [CHANGELOG](CHANGELOG.md), then run
```bash
docker compose run --rm app bump-my-version bump <major|minor|patch>
```
This will update version major/minor/patch version respectively and add a tag for release.

- Push including new tag to publish the release to pypi.
```bash
git push origin tag <tag_name>
```
