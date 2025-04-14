from unittest.mock import patch

from django_kafka.connect.models import KafkaConnectSkipModel
from django_kafka.tests.models import AbstractModelTestCase


class KafkaConnectSkipModelTestCase(AbstractModelTestCase):
    abstract_model = KafkaConnectSkipModel
    model: type[KafkaConnectSkipModel]

    def test_save__direct_instance_respects_set_kafka_skip(self):
        """test `save` on directly created instances will not ignore set kafka_skip"""
        instance = self.model(kafka_skip=True)
        instance_with_pk = self.model(pk=2, kafka_skip=True)

        instance.save()
        instance_with_pk.save()

        self.assertTrue(instance.kafka_skip)
        self.assertTrue(instance_with_pk.kafka_skip)

    def test_save__second_save_resets_kafka_skip(self):
        """test saving a second time will reset kafka_skip to False"""
        instance = self.model(kafka_skip=True)

        instance.save()
        instance.save()

        self.assertFalse(instance.kafka_skip)

    def test_save__adds_kafka_skip_to_update_fields_when_reset(self):
        """test update_fields has kafka_skip added when kafka_skip is reset"""
        instance = self.model(kafka_skip=True)
        instance.save()

        with patch("django.db.models.Model.save") as mock_save:
            instance.save(update_fields=["field"])

        mock_save.assert_called_once_with(
            force_insert=False,
            force_update=False,
            using=None,
            update_fields=["kafka_skip", "field"],
        )

    def test__saving_retrieved_db_instance__resets_unset_kafka_skip(self):
        """test saving a db-retrieved instance will reset kafka_skip if not set"""
        instance = self.model(kafka_skip=True)
        instance.save()

        retrieved = self.model.objects.get(pk=instance.pk)
        retrieved.save()

        self.assertFalse(retrieved.kafka_skip)

    def test__saving_retrieved_db_instance__respects_set_kafka_skip(self):
        """test saving a db-retrieved instance will not ignore newly set kafka skip"""
        instance = self.model(kafka_skip=False)
        instance.save()

        retrieved = self.model.objects.get(pk=instance.pk)
        retrieved.kafka_skip = True
        retrieved.save()

        self.assertTrue(retrieved.kafka_skip)

    def test__refresh_from_db__makes_kafka_skip_be_reset(self):
        """test refreshing from db will ignore newly set kafka_skip"""
        instance = self.model(kafka_skip=False)
        instance.save()

        instance.kafka_skip = True
        instance.refresh_from_db()
        instance.save()

        self.assertFalse(instance.kafka_skip)

    def test_queryset_create__respects_kafka_skip(self):
        """test kafka_skip=True is maintained for QuerySet.create"""
        instance = self.model.objects.create(kafka_skip=True)
        instance_with_pk = self.model.objects.create(pk=2, kafka_skip=True)

        self.assertTrue(instance.kafka_skip)
        self.assertTrue(instance_with_pk.kafka_skip)

    def test_queryset_get_or_create__respects_kafka_skip(self):
        """test kafka_skip=True is maintained for QuerySet.get_or_create"""
        instance, _ = self.model.objects.get_or_create(defaults={"kafka_skip": True})
        instance_with_pk, _ = self.model.objects.get_or_create(
            pk=2,
            defaults={"kafka_skip": True},
        )

        self.assertTrue(instance.kafka_skip)
        self.assertTrue(instance_with_pk.kafka_skip)

    def test_queryset_update_or_create_with_kafka_skip(self):
        """test kafka_skip=True is maintained for QuerySet.update_or_create"""
        instance = self.model.objects.create(kafka_skip=False)

        existing_instance, _ = self.model.objects.update_or_create(
            pk=instance.id,
            defaults={"kafka_skip": True},
        )
        new_instance, _ = self.model.objects.update_or_create(
            pk=2,
            defaults={"kafka_skip": True},
        )

        self.assertTrue(existing_instance.kafka_skip)
        self.assertTrue(new_instance.kafka_skip)

    @patch("django.db.models.QuerySet.update")
    def test_queryset_update_sets_kafka_skip(self, mock_update):
        """
        `update` method should automatically set `kafka_skip=False`
        if `kafka_skip` is not provided in kwargs.
        """
        # update_kwargs does not contain kafka_skip
        update_kwargs = {"some_field": "some_value"}

        self.model.objects.update(**update_kwargs)

        # kafka_skip=False was added to the update_kwargs
        mock_update.assert_called_once_with(
            **{"kafka_skip": False, **update_kwargs},
        )

    @patch("django.db.models.QuerySet.update")
    def test_queryset_update_does_not_override_kafka_skip(self, mock_update):
        """
        `update` method should not change `kafka_skip`
        if `kafka_skip` is provided in kwargs
        """
        # update_kwargs contains kafka_skip
        update_kwargs = {"kafka_skip": True, "some_field": "some_value"}

        self.model.objects.update(**update_kwargs)

        # kafka_skip is not changed, update_kwargs are not changed
        mock_update.assert_called_once_with(**update_kwargs)
