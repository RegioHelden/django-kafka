from django.db import connection
from django.db.models import Model
from django.test import TestCase


class AbstractModelTestCase(TestCase):
    abstract_model: type[Model]
    model: type[Model]

    @classmethod
    def setUpClass(cls):
        class TestModel(cls.abstract_model):
            class Meta:
                app_label = "django_kafka"

        cls.model = TestModel

        with connection.schema_editor() as editor:
            editor.create_model(cls.model)

        # super has to be at the end, otherwise model won't initialize
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()

        with connection.schema_editor() as editor:
            editor.delete_model(cls.model)

        connection.close()
