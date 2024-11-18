from unittest.mock import Mock

from django.core.management import CommandError
from django.test import SimpleTestCase

from django_kafka.management.commands.errors import substitute_error


class SubstituteErrorTestCase(SimpleTestCase):
    def test_substitute(self):
        class CustomError(Exception):
            pass

        errors = {
            ValueError: "value error",
            KeyError: "key error",
            CustomError: "custom error",
        }
        decorator = substitute_error(errors.keys(), CommandError)

        for error, msg in errors.items():
            func = Mock(side_effect=error(msg))

            with self.assertRaisesMessage(CommandError, msg):
                decorator(func)()
