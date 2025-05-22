from pydoc import locate

from django_temporalio.client import init_client
from temporalio.client import WorkflowExecutionStatus

from django_kafka.relations_resolver.relation import ModelRelation


class TemporalModelRelation(ModelRelation):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.workflow_id = f"{self.model_path}-{self.object_id}"

    async def has_waiting_messages(self) -> bool:
        return bool(
            await anext(
                (await init_client())
                .list_workflows(
                    query=f"WorkflowId='{self.workflow_id}' AND ExecutionStatus={WorkflowExecutionStatus.RUNNING}"
                ),
                None,
            )
        )

    def serialize(self) -> dict:
        return {
            "model": self.model_path,
            "object_id": self.object_id,
            "id_field": self.id_field,
        }

    @classmethod
    def deserialize(cls, model: str, object_id: int | str, id_field: str) -> "TemporalModelRelation":
        return cls(locate(model), object_id, id_field)
