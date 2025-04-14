import datetime

from django.test import SimpleTestCase

from django_kafka.topic.transforms import days_from_epoch_to_date


class TransformsTestCase(SimpleTestCase):
    def test_days_from_epoch_to_date(self):
        # days are calculated from epoch
        result = days_from_epoch_to_date(10)
        self.assertEqual(result, datetime.date(1970, 1, 11))

        # None values are silently skipped
        empty_values = [None, ""]
        default_value = "some-default"
        for value in empty_values:
            result = days_from_epoch_to_date(value, default=default_value)
            self.assertEqual(result, default_value)
