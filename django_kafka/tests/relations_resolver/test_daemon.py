from unittest.mock import AsyncMock, MagicMock, patch

from django.test import SimpleTestCase
from temporalio.common import WorkflowIDConflictPolicy

from django_kafka.relations_resolver.daemon.temporal import TemporalDaemon
from django_kafka.relations_resolver.relation import Relation
from django_kafka.relations_resolver.temporalio.workflows import ResolveRelation


@patch(
    "django_kafka.relations_resolver.daemon.temporal.init_client",
    new_callable=AsyncMock,
)
class TemporalDaemonTestCase(SimpleTestCase):
    async def test_aresolve_relation_starts_the_workflow(self, mock_init_client):
        relation = MagicMock(spec=Relation)
        relation.aidentifier.return_value = "example.order.id-1"

        await TemporalDaemon().aresolve_relation(relation)

        start_workflow = mock_init_client.return_value.start_workflow
        start_workflow.assert_awaited_once()
        self.assertEqual(
            start_workflow.await_args.args,
            (ResolveRelation.run, relation.serialize()),
        )
        self.assertEqual(start_workflow.await_args.kwargs["id"], "example.order.id-1")
        self.assertEqual(
            start_workflow.await_args.kwargs["task_queue"],
            TemporalDaemon.task_queue,
        )

    async def test_aresolve_relation_joins_a_running_workflow(self, mock_init_client):
        """A relation queued again while its previous run is still going must
        not fail the daemon's pass."""
        relation = MagicMock(spec=Relation)

        await TemporalDaemon().aresolve_relation(relation)

        start_workflow = mock_init_client.return_value.start_workflow
        self.assertEqual(
            start_workflow.await_args.kwargs["id_conflict_policy"],
            WorkflowIDConflictPolicy.USE_EXISTING,
        )
