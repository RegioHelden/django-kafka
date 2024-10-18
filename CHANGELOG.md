# Changelog

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
