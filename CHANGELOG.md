# Changelog

## 0.5.18 (2025-03-31)
* Add "Topic key offset tracker" to resolve race condition or missing "relation" messages which are blocking consumers.

## 0.5.17 (2025-02-07)
* Fix `Suppression.active` returning `True` even if suppress was not used.
* Update readme `ruff` section.

## 0.5.16 (2025-01-27)
* Integrate ruff and renovate.

## 0.5.15 (2025-02-06)
* Suppress messages serialization if producer is suppressed.

## 0.5.14 (2025-01-27)
* Improve logging. Add consumer and topic names to the error message.

## 0.5.13 (2025-01-07)
* Upgrade `confluent-kafka` from `v2.6.2` to `v2.7.0` as recommended by authors.

## 0.5.12 (2024-12-20)
* Fix dependency name rename `schema-registry` to `schemaregistry`.

## 0.5.11 (2024-12-20)
* Upgrade `confluent-kafka` which adds support of the list of urls to the `SchemaRegistryClient`.

## 0.5.10 (2024-11-29)
* `settings.RETRY_SETTINGS` takes a `dict` of kwargs to pass to `RetrySettings` class.
* Add `exclude_fields` option to `ModelTopicConsumer` to ignore message value fields.

## 0.5.9 (2024-11-28)
* Add `RETRY_SETTINGS` configuration setting.
* Add blocking retry behaviour and make it the default.

## 0.5.8 (2024-11-19)
* `ModelTopicConsumer.get_defaults` will skip fields not defined on the model.

## 0.5.7 (2024-11-18)
* `@substitute_error` now shows error message of the original error.

## 0.5.6 (2024-11-13)
* Fix `DjangoKafka.run_consumers` failing when there are no consumers in the registry.

## 0.5.5 (2024-11-06)
* `./manage.py kafka_connect` command now will exit with `CommandError` in case of any exception.
* New `@substitute_error(errors: Iterable[Type[Exception]], substitution: Type[Exception])` decorator to substitute exceptions.

## 0.5.2 (2024-10-17)
* Added `producer.suppress` decorator.
* Renamed `KafkaSkipModel` to `KafkaConnectSkipModel`.
* Renamed `KafkaConnectSkipQueryset` to `KafkaConnectSkipQueryset`

## 0.5.1 (2024-10-16)
* `ModelTopicConsumer.sync` returns now the results of the `update_or_create` method.
* Add `days_from_epoch_to_date` function to convert `io.debezium.time.Date` to python `datetime.date`.

## 0.4.1 (2024-09-17)
* Support string-based delete keys in DbzModelTopicConsumer

## 0.4.0 (2024-09-17)
* Add ModelTopicConsumer and DbzModelTopicConsumer 

## 0.3.0 (2024-09-05)
* Add decorator for topic retry and dead letter topic, see `README.md`
* Separate `Topic` class in to `TopicProducer` and `TopicConsumer` classes.

## 0.2.0 (2024-09-04)
* Release removed
