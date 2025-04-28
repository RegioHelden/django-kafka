# Changelog

## [v0.7.0](https://github.com/RegioHelden/django-kafka/tree/v0.7.0) (2025-04-28)

[Full Changelog](https://github.com/RegioHelden/django-kafka/compare/v0.6.0...v0.7.0)

**Implemented enhancements:**

- Updates from modulesync [\#78](https://github.com/RegioHelden/django-kafka/pull/78) (@regiohelden-dev)
- Removed unused wsgi file from example app [\#67](https://github.com/RegioHelden/django-kafka/pull/67) (@lociii)

**Fixed bugs:**

- fix: fix start command of the "app" container. [\#68](https://github.com/RegioHelden/django-kafka/pull/68) (@bodja)

**Merged pull requests:**

- Updates from modulesync [\#81](https://github.com/RegioHelden/django-kafka/pull/81) (@regiohelden-dev)
- Updates from modulesync [\#80](https://github.com/RegioHelden/django-kafka/pull/80) (@regiohelden-dev)
- Updates from modulesync [\#77](https://github.com/RegioHelden/django-kafka/pull/77) (@regiohelden-dev)
- Updates from modulesync [\#76](https://github.com/RegioHelden/django-kafka/pull/76) (@regiohelden-dev)
- Updates from modulesync [\#75](https://github.com/RegioHelden/django-kafka/pull/75) (@regiohelden-dev)
- chore\(deps\): bump regiohelden/github-reusable-workflows from 2.0.0 to 2.1.0 [\#74](https://github.com/RegioHelden/django-kafka/pull/74) (@dependabot[bot])
- Updates from modulesync  [\#73](https://github.com/RegioHelden/django-kafka/pull/73) (@regiohelden-dev)
- Preparations for modulesync rollout [\#72](https://github.com/RegioHelden/django-kafka/pull/72) (@lociii)

## 0.5.18 (2025-03-31)

**Fixed bugs:**

* Add "Topic key offset tracker" to resolve race condition or missing "relation" messages which are blocking consumers.

## 0.5.17 (2025-02-07)

**Fixed bugs:**

* Fix `Suppression.active` returning `True` even if suppress was not used.

**Implemented enhancements:**

* Update readme `ruff` section.

## 0.5.16 (2025-01-27)

**Implemented enhancements:**

* Integrate ruff and renovate.

## 0.5.15 (2025-02-06)

**Fixed bugs:**

* Suppress messages serialization if producer is suppressed.

## 0.5.14 (2025-01-27)

**Implemented enhancements:**

* Improve logging. Add consumer and topic names to the error message.

## 0.5.13 (2025-01-07)

**Implemented enhancements:**

* Upgrade `confluent-kafka` from `v2.6.2` to `v2.7.0` as recommended by authors.

## 0.5.12 (2024-12-20)

**Fixed bugs:**

* Fix dependency name rename `schema-registry` to `schemaregistry`.

## 0.5.11 (2024-12-20)

**Implemented enhancements:**

* Upgrade `confluent-kafka` which adds support of the list of urls to the `SchemaRegistryClient`.

## 0.5.10 (2024-11-29)

**Implemented enhancements:**

* `settings.RETRY_SETTINGS` takes a `dict` of kwargs to pass to `RetrySettings` class.
* Add `exclude_fields` option to `ModelTopicConsumer` to ignore message value fields.

## 0.5.9 (2024-11-28)

**Implemented enhancements:**

* Add `RETRY_SETTINGS` configuration setting.
* Add blocking retry behaviour and make it the default.

## 0.5.8 (2024-11-19)

**Implemented enhancements:**

* `ModelTopicConsumer.get_defaults` will skip fields not defined on the model.

## 0.5.7 (2024-11-18)

**Implemented enhancements:**

* `@substitute_error` now shows error message of the original error.

## 0.5.6 (2024-11-13)

**Fixed bugs:**

* Fix `DjangoKafka.run_consumers` failing when there are no consumers in the registry.

## 0.5.5 (2024-11-06)

**Implemented enhancements:**

* `./manage.py kafka_connect` command now will exit with `CommandError` in case of any exception.
* New `@substitute_error(errors: Iterable[Type[Exception]], substitution: Type[Exception])` decorator to substitute exceptions.

## 0.5.2 (2024-10-17)

**Implemented enhancements:**

* Added `producer.suppress` decorator.

**Breaking changes:**

* Renamed `KafkaSkipModel` to `KafkaConnectSkipModel`.
* Renamed `KafkaConnectSkipQueryset` to `KafkaConnectSkipQueryset`

## 0.5.1 (2024-10-16)

**Implemented enhancements:**

* `ModelTopicConsumer.sync` returns now the results of the `update_or_create` method.
* Add `days_from_epoch_to_date` function to convert `io.debezium.time.Date` to python `datetime.date`.

## 0.4.1 (2024-09-17)

**Implemented enhancements:**

* Support string-based delete keys in DbzModelTopicConsumer

## 0.4.0 (2024-09-17)

**Implemented enhancements:**

* Add ModelTopicConsumer and DbzModelTopicConsumer 

## 0.3.0 (2024-09-05)

**Implemented enhancements:**

* Add decorator for topic retry and dead letter topic, see `README.md`
* Separate `Topic` class in to `TopicProducer` and `TopicConsumer` classes.

## 0.2.0 (2024-09-04)

* Yanked


