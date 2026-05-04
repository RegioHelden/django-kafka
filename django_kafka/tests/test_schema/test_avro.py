from typing import TypedDict
from unittest import TestCase

from django.db import models

from django_kafka.schema.avro import AvroSchema


class RelatedModel(models.Model):
    class Meta:
        app_label = "test_schema"


class UUIDRelatedModel(models.Model):
    id = models.UUIDField(primary_key=True)

    class Meta:
        app_label = "test_schema"


class FullModel(models.Model):
    char = models.CharField(max_length=100)
    text = models.TextField()
    slug = models.SlugField()
    email = models.EmailField()
    url = models.URLField()
    boolean = models.BooleanField()
    small_int = models.SmallIntegerField()
    integer = models.IntegerField()
    big_int = models.BigIntegerField()
    positive_int = models.PositiveIntegerField()
    float_field = models.FloatField()
    decimal_field = models.DecimalField(max_digits=10, decimal_places=2)
    date = models.DateField()
    datetime = models.DateTimeField()
    time = models.TimeField()
    duration = models.DurationField()
    uuid = models.UUIDField()
    binary = models.BinaryField()
    json_field = models.JSONField()
    ip = models.GenericIPAddressField()
    file_path = models.FilePathField()

    class Meta:
        app_label = "test_schema"


class NullableModel(models.Model):
    name = models.CharField(max_length=100)
    description = models.TextField(null=True)
    count = models.IntegerField(null=True)

    class Meta:
        app_label = "test_schema"


class FKModel(models.Model):
    related = models.ForeignKey(RelatedModel, on_delete=models.CASCADE)
    uuid_related = models.ForeignKey(UUIDRelatedModel, on_delete=models.CASCADE)

    class Meta:
        app_label = "test_schema"


class AvroSchemaFromModelTestCase(TestCase):
    def _schema(self, model, **kwargs):
        return AvroSchema.from_model(model, **kwargs).to_dict()

    def test_basic_string_fields(self):
        schema = self._schema(
            FullModel,
            fields=("char", "text", "slug", "email", "url"),
        )
        for field in schema["fields"]:
            self.assertEqual(field["type"], "string", f"Field {field['name']}")

    def test_integer_fields(self):
        schema = self._schema(FullModel, fields=("small_int", "integer"))
        for field in schema["fields"]:
            self.assertEqual(field["type"], "int", f"Field {field['name']}")

    def test_big_integer_field(self):
        schema = self._schema(FullModel, fields=("big_int",))
        self.assertEqual(schema["fields"][0]["type"], "long")

    def test_float_field(self):
        schema = self._schema(FullModel, fields=("float_field",))
        self.assertEqual(schema["fields"][0]["type"], "double")

    def test_boolean_field(self):
        schema = self._schema(FullModel, fields=("boolean",))
        self.assertEqual(schema["fields"][0]["type"], "boolean")

    def test_decimal_field(self):
        schema = self._schema(FullModel, fields=("decimal_field",))
        expected = {
            "type": "bytes",
            "logicalType": "decimal",
            "precision": 10,
            "scale": 2,
        }
        self.assertEqual(schema["fields"][0]["type"], expected)

    def test_date_field(self):
        schema = self._schema(FullModel, fields=("date",))
        self.assertEqual(
            schema["fields"][0]["type"],
            {"type": "int", "logicalType": "date"},
        )

    def test_datetime_field(self):
        schema = self._schema(FullModel, fields=("datetime",))
        self.assertEqual(
            schema["fields"][0]["type"],
            {"type": "long", "logicalType": "timestamp-millis"},
        )

    def test_time_field(self):
        schema = self._schema(FullModel, fields=("time",))
        self.assertEqual(
            schema["fields"][0]["type"],
            {"type": "int", "logicalType": "time-millis"},
        )

    def test_uuid_field(self):
        schema = self._schema(FullModel, fields=("uuid",))
        self.assertEqual(
            schema["fields"][0]["type"],
            {"type": "string", "logicalType": "uuid"},
        )

    def test_binary_field(self):
        schema = self._schema(FullModel, fields=("binary",))
        self.assertEqual(schema["fields"][0]["type"], "bytes")

    def test_json_field(self):
        schema = self._schema(FullModel, fields=("json_field",))
        self.assertEqual(schema["fields"][0]["type"], "string")

    def test_duration_field(self):
        schema = self._schema(FullModel, fields=("duration",))
        self.assertEqual(schema["fields"][0]["type"], "long")

    def test_nullable_fields(self):
        schema = self._schema(NullableModel)
        fields = {f["name"]: f for f in schema["fields"]}

        self.assertEqual(fields["name"]["type"], "string")
        self.assertEqual(fields["description"]["type"], ["null", "string"])
        self.assertEqual(fields["description"]["default"], None)
        self.assertEqual(fields["count"]["type"], ["null", "int"])

    def test_fk_uses_related_pk_type(self):
        schema = self._schema(FKModel, fields=("related_id",))
        # RelatedModel has default BigAutoField pk → long
        self.assertEqual(schema["fields"][0]["type"], "long")
        self.assertEqual(schema["fields"][0]["name"], "related_id")

    def test_fk_uuid_pk(self):
        schema = self._schema(FKModel, fields=("uuid_related_id",))
        self.assertEqual(
            schema["fields"][0]["type"],
            {"type": "string", "logicalType": "uuid"},
        )

    def test_record_name_defaults_to_model_name(self):
        schema = self._schema(NullableModel, fields=("name",))
        self.assertEqual(schema["name"], "NullableModel")

    def test_custom_record_name(self):
        schema = self._schema(NullableModel, fields=("name",), name="Custom")
        self.assertEqual(schema["name"], "Custom")

    def test_namespace(self):
        schema = self._schema(
            NullableModel,
            fields=("name",),
            namespace="com.example",
        )
        self.assertEqual(schema["namespace"], "com.example")

    def test_no_namespace_by_default(self):
        schema = self._schema(NullableModel, fields=("name",))
        self.assertNotIn("namespace", schema)

    def test_fields_filter(self):
        schema = self._schema(FullModel, fields=("char", "integer"))
        field_names = [f["name"] for f in schema["fields"]]
        self.assertEqual(field_names, ["char", "integer"])

    def test_exclude_fields(self):
        schema = self._schema(NullableModel, fields=("description",), exclude=True)
        field_names = [f["name"] for f in schema["fields"]]
        self.assertNotIn("description", field_names)
        self.assertIn("name", field_names)

    def test_all_fields(self):
        schema = self._schema(NullableModel)
        field_names = [f["name"] for f in schema["fields"]]
        self.assertIn("name", field_names)
        self.assertIn("description", field_names)
        self.assertIn("count", field_names)

    def test_invalid_field_raises(self):
        with self.assertRaises(ValueError) as ctx:
            AvroSchema.from_model(NullableModel, fields=("nonexistent",))
        self.assertIn("nonexistent", str(ctx.exception))

    def test_to_json_returns_string(self):
        schema = AvroSchema.from_model(NullableModel, fields=("name",))
        result = schema.to_json()
        self.assertIsInstance(result, str)

    def test_to_dict_returns_dict(self):
        schema = AvroSchema.from_model(NullableModel, fields=("name",))
        result = schema.to_dict()
        self.assertIsInstance(result, dict)
        self.assertEqual(result["type"], "record")


class AvroSchemaFromTypeTestCase(TestCase):
    def test_typed_dict(self):
        class Extra(TypedDict):
            name_de: str
            score: float

        schema = AvroSchema.from_type(Extra)
        fields = schema.to_dict()["fields"]
        self.assertEqual(len(fields), 2)
        self.assertEqual(fields[0], {"name": "name_de", "type": "string"})
        self.assertEqual(fields[1], {"name": "score", "type": "double"})

    def test_optional_field(self):
        class Extra(TypedDict):
            value: str | None

        schema = AvroSchema.from_type(Extra)
        fields = schema.to_dict()["fields"]
        self.assertEqual(fields[0]["type"], ["null", "string"])

    def test_all_python_types(self):
        class Extra(TypedDict):
            s: str
            i: int
            f: float
            b: bool
            by: bytes

        schema = AvroSchema.from_type(Extra)
        types = {f["name"]: f["type"] for f in schema.to_dict()["fields"]}
        self.assertEqual(types["s"], "string")
        self.assertEqual(types["i"], "int")
        self.assertEqual(types["f"], "double")
        self.assertEqual(types["b"], "boolean")
        self.assertEqual(types["by"], "bytes")

    def test_unsupported_type_raises(self):
        class Extra(TypedDict):
            data: list

        with self.assertRaises(ValueError):
            AvroSchema.from_type(Extra)

    def test_class_with_annotations(self):
        class Extra:
            name: str
            count: int

        schema = AvroSchema.from_type(Extra)
        fields = schema.to_dict()["fields"]
        self.assertEqual(len(fields), 2)
        self.assertEqual(fields[0], {"name": "name", "type": "string"})
        self.assertEqual(fields[1], {"name": "count", "type": "int"})


class AvroKeySchemaTestCase(TestCase):
    def _schema(self, model, lookup_fields, **kwargs):
        return AvroSchema.key_from_model(model, lookup_fields, **kwargs).to_dict()

    def test_single_lookup_field(self):
        schema = self._schema(NullableModel, "id")
        self.assertEqual(schema["name"], "Key")
        self.assertEqual(len(schema["fields"]), 1)
        self.assertEqual(schema["fields"][0]["name"], "id")

    def test_pk_alias(self):
        schema = self._schema(NullableModel, "pk")
        self.assertEqual(schema["fields"][0]["name"], "id")

    def test_composite_key(self):
        schema = self._schema(NullableModel, ("id", "name"))
        self.assertEqual(len(schema["fields"]), 2)
        field_names = [f["name"] for f in schema["fields"]]
        self.assertEqual(field_names, ["id", "name"])

    def test_namespace(self):
        schema = self._schema(NullableModel, "id", namespace="com.example")
        self.assertEqual(schema["namespace"], "com.example")

    def test_invalid_field_raises(self):
        with self.assertRaises(ValueError):
            AvroSchema.key_from_model(NullableModel, "nonexistent")

    def test_uuid_lookup_field(self):
        schema = self._schema(FullModel, "uuid")
        self.assertEqual(
            schema["fields"][0]["type"],
            {"type": "string", "logicalType": "uuid"},
        )

    def test_fk_lookup_field(self):
        schema = self._schema(FKModel, "related_id")
        self.assertEqual(schema["fields"][0]["type"], "long")


class AvroSchemaExtendTestCase(TestCase):
    def test_extend_adds_fields(self):
        class Extra(TypedDict):
            name_de: str

        schema = AvroSchema.from_model(NullableModel, fields=("name",))
        extended = schema.extend(Extra)
        field_names = [f["name"] for f in extended.to_dict()["fields"]]
        self.assertIn("name", field_names)
        self.assertIn("name_de", field_names)

    def test_extend_preserves_original(self):
        class Extra(TypedDict):
            extra: str

        original = AvroSchema.from_model(NullableModel, fields=("name",))
        original.extend(Extra)
        # Original should not be modified
        field_names = [f["name"] for f in original.to_dict()["fields"]]
        self.assertNotIn("extra", field_names)

    def test_extend_preserves_namespace(self):
        class Extra(TypedDict):
            extra: str

        schema = AvroSchema.from_model(
            NullableModel,
            fields=("name",),
            namespace="com.example",
        )
        extended = schema.extend(Extra)
        self.assertEqual(extended.to_dict()["namespace"], "com.example")
