from unittest.mock import AsyncMock, MagicMock, patch

from django.test import SimpleTestCase
from temporalio.common import WorkflowIDConflictPolicy

from django_kafka.relations_resolver.daemon import RelationResolverDaemon
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


class ResolveRelationsBatchTestCase(SimpleTestCase):
    class Daemon(RelationResolverDaemon):
        def __init__(self):
            self.dispatched = []

        async def aresolve_relation(self, relation):
            self.dispatched.append(relation)

    def _relations(self, count):
        async def generator():
            for _index in range(count):
                relation = MagicMock(spec=Relation)
                relation.amark_resolving = AsyncMock()
                yield relation

        return generator()

    async def _run(self, available, batch_size):
        daemon = self.Daemon()
        with (
            patch(
                "django_kafka.conf.settings.RELATION_RESOLVER_DAEMON_BATCH_SIZE",
                batch_size,
            ),
            patch("django_kafka.relations_resolver.daemon.kafka") as mock_kafka,
        ):
            processor = mock_kafka.relations_resolver.processor
            processor.ato_resolve.return_value = self._relations(available)
            await daemon.aresolve_relations()
        return daemon.dispatched

    async def test_stops_at_the_batch_size(self):
        dispatched = await self._run(available=10, batch_size=4)

        self.assertEqual(len(dispatched), 4)

    async def test_dispatches_everything_below_the_batch_size(self):
        dispatched = await self._run(available=3, batch_size=4)

        self.assertEqual(len(dispatched), 3)

    async def test_dispatches_everything_when_uncapped(self):
        dispatched = await self._run(available=10, batch_size=None)

        self.assertEqual(len(dispatched), 10)
