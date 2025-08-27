# Changelog

## [v0.13.0](https://github.com/RegioHelden/django-kafka/tree/v0.13.0) (2025-08-27)

[Full Changelog](https://github.com/RegioHelden/django-kafka/compare/v0.12.0...v0.13.0)

**Implemented enhancements:**

- feat: add TopicReproducer for debezium source connector message augmentation, refs \#136 [\#137](https://github.com/RegioHelden/django-kafka/pull/137) (@stefan-cardnell-rh)
- Update uv to 0.8.13 [\#135](https://github.com/RegioHelden/django-kafka/pull/135) (@regiohelden-dev)
- Update uv to 0.8.12 and reusable workflows to 2.3.0 [\#134](https://github.com/RegioHelden/django-kafka/pull/134) (@regiohelden-dev)
- Update uv to 0.8.2 [\#132](https://github.com/RegioHelden/django-kafka/pull/132) (@regiohelden-dev)

**Merged pull requests:**

- Update uv to 0.8.3 [\#133](https://github.com/RegioHelden/django-kafka/pull/133) (@regiohelden-dev)
- Update uv to 0.8.0 [\#131](https://github.com/RegioHelden/django-kafka/pull/131) (@regiohelden-dev)

## [v0.12.0](https://github.com/RegioHelden/django-kafka/tree/v0.12.0) (2025-07-17)

[Full Changelog](https://github.com/RegioHelden/django-kafka/compare/v0.11.1...v0.12.0)

**Implemented enhancements:**

- feat: improve debug logging, refs \#128 [\#129](https://github.com/RegioHelden/django-kafka/pull/129) (@stefan-cardnell-rh)
- Set dependencies in sync config for modulesync to pick them up [\#126](https://github.com/RegioHelden/django-kafka/pull/126) (@lociii)
- Require postgres container from modulesync [\#124](https://github.com/RegioHelden/django-kafka/pull/124) (@lociii)
- Update test dependencies [\#123](https://github.com/RegioHelden/django-kafka/pull/123) (@lociii)

**Merged pull requests:**

- Update uv to 0.7.21 [\#127](https://github.com/RegioHelden/django-kafka/pull/127) (@regiohelden-dev)

## [v0.11.1](https://github.com/RegioHelden/django-kafka/tree/v0.11.1) (2025-07-08)

[Full Changelog](https://github.com/RegioHelden/django-kafka/compare/v0.11.0...v0.11.1)

**Fixed bugs:**

- fix: `KeyOffsetTrackerTopic.consume` returns proper value now. [\#120](https://github.com/RegioHelden/django-kafka/pull/120) (@bodja)

## [v0.11.0](https://github.com/RegioHelden/django-kafka/tree/v0.11.0) (2025-07-07)

[Full Changelog](https://github.com/RegioHelden/django-kafka/compare/v0.10.0...v0.11.0)

**Implemented enhancements:**

- update django-temporalio dependency version [\#117](https://github.com/RegioHelden/django-kafka/pull/117) (@krayevidi)

## [v0.10.0](https://github.com/RegioHelden/django-kafka/tree/v0.10.0) (2025-07-04)

[Full Changelog](https://github.com/RegioHelden/django-kafka/compare/v0.9.0...v0.10.0)

**Implemented enhancements:**

- feat: implement relations resolver. [\#109](https://github.com/RegioHelden/django-kafka/pull/109) (@bodja)
- Update reusable workflows [\#104](https://github.com/RegioHelden/django-kafka/pull/104) (@lociii)
- chore\(deps\): bump setuptools from 78.1.0 to 78.1.1 [\#97](https://github.com/RegioHelden/django-kafka/pull/97) (@dependabot[bot])

**Fixed bugs:**

- fix: `ModelMessageProcessor._aget_missing_relation` wrong return value. [\#115](https://github.com/RegioHelden/django-kafka/pull/115) (@bodja)

**Merged pull requests:**

- Update uv to 0.7.19 [\#114](https://github.com/RegioHelden/django-kafka/pull/114) (@regiohelden-dev)
- Update uv to 0.7.18 [\#113](https://github.com/RegioHelden/django-kafka/pull/113) (@regiohelden-dev)
- Update uv to 0.7.17 [\#108](https://github.com/RegioHelden/django-kafka/pull/108) (@regiohelden-dev)
- Update uv to 0.7.15 [\#107](https://github.com/RegioHelden/django-kafka/pull/107) (@regiohelden-dev)
- Updates GitHub reusable workflows to 2.2.4 [\#106](https://github.com/RegioHelden/django-kafka/pull/106) (@regiohelden-dev)
- Update uv to 0.7.14 and vscode ruff to 2025.24.0 [\#105](https://github.com/RegioHelden/django-kafka/pull/105) (@regiohelden-dev)
- Update uv to 0.7.13 [\#102](https://github.com/RegioHelden/django-kafka/pull/102) (@regiohelden-dev)
- Update uv to 0.7.12 [\#101](https://github.com/RegioHelden/django-kafka/pull/101) (@regiohelden-dev)
- Update uv to 0.7.11 [\#100](https://github.com/RegioHelden/django-kafka/pull/100) (@regiohelden-dev)
- Update uv to 0.7.8 [\#98](https://github.com/RegioHelden/django-kafka/pull/98) (@regiohelden-dev)
- Update uv to 0.7.7 [\#96](https://github.com/RegioHelden/django-kafka/pull/96) (@regiohelden-dev)
- Update uv to 0.7.6 [\#95](https://github.com/RegioHelden/django-kafka/pull/95) (@regiohelden-dev)
- Update uv to 0.7.5 [\#94](https://github.com/RegioHelden/django-kafka/pull/94) (@regiohelden-dev)

## [v0.9.0](https://github.com/RegioHelden/django-kafka/tree/v0.9.0) (2025-05-16)

[Full Changelog](https://github.com/RegioHelden/django-kafka/compare/v0.8.0...v0.9.0)

**Implemented enhancements:**

- fix: add retry setting to log every Nth attempt and add more info headers to dead letter topic, refs \#91 [\#92](https://github.com/RegioHelden/django-kafka/pull/92) (@stefan-cardnell-rh)

## [v0.8.0](https://github.com/RegioHelden/django-kafka/tree/v0.8.0) (2025-05-16)

[Full Changelog](https://github.com/RegioHelden/django-kafka/compare/v0.7.0...v0.8.0)

**Implemented enhancements:**

- Remove unused version from init file [\#85](https://github.com/RegioHelden/django-kafka/pull/85) (@lociii)

**Fixed bugs:**

- fix: resume partitions should resume at the paused offsets, refs \#88 [\#89](https://github.com/RegioHelden/django-kafka/pull/89) (@stefan-cardnell-rh)

**Merged pull requests:**

- Update uv to 0.7.4 [\#87](https://github.com/RegioHelden/django-kafka/pull/87) (@regiohelden-dev)
- Update uv to 0.7.3 [\#86](https://github.com/RegioHelden/django-kafka/pull/86) (@regiohelden-dev)
- Updates from modulesync [\#83](https://github.com/RegioHelden/django-kafka/pull/83) (@regiohelden-dev)

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

## [v0.6.0](https://github.com/RegioHelden/django-kafka/tree/v0.6.0) (2025-04-14)

[Full Changelog](https://github.com/RegioHelden/django-kafka/compare/v0.5.18...v0.6.0)

**Implemented enhancements:**

- Test against Django 5.2 [\#64](https://github.com/RegioHelden/django-kafka/pull/64) (@lociii)
- Introduce reusable workflows [\#62](https://github.com/RegioHelden/django-kafka/pull/62) (@lociii)

**Merged pull requests:**

- feat: introduce "Topic key offset tracker" to resolve race condition … [\#59](https://github.com/RegioHelden/django-kafka/pull/59) (@bodja)

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


