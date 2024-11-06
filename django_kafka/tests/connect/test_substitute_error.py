from unittest.mock import Mock

from django.core.management import CommandError
from django.test import SimpleTestCase

from django_kafka.management.commands.errors import substitute_error


class SubstituteErrorTestCase(SimpleTestCase):
    def test_substitute(self):
        class CustomException(Exception):
            pass

        errors = [ValueError, KeyError, CustomException]
        decorator = substitute_error(errors, CommandError)

        for error in errors:
            func = Mock(side_effect=error)

            with self.assertRaises(CommandError):
                decorator(func)()
