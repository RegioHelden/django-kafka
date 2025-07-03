from django.db import connection
from django.db.models import Model
from django.db.models.base import ModelBase
from django.test import TestCase


class AbstractModelTestCase(TestCase):
    abstract_model: type[Model]
    model: type[Model]

    @classmethod
    def setUpClass(cls):
        class Meta:
            app_label = cls.__module__

        cls.model = ModelBase(
            f"__Test{cls.abstract_model.__name__}__",
            (cls.abstract_model,),
            {"__module__": cls.abstract_model.__module__, "Meta": Meta},
        )

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
