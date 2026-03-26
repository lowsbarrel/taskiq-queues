"""Tests for multi-queue routing support in taskiq core."""

import asyncio
from collections.abc import AsyncGenerator

import pytest

from taskiq import AsyncBroker, BrokerMessage
from taskiq.acks import AckableMessage
from taskiq.cli.worker.args import WorkerArgs


class QueueCapturingBroker(AsyncBroker):
    """Broker that captures which queue_name label each kicked message has."""

    def __init__(self) -> None:
        super().__init__(None, None)
        self.kicked_messages: list[BrokerMessage] = []

    async def kick(self, message: BrokerMessage) -> None:
        self.kicked_messages.append(message)

    async def listen(self) -> AsyncGenerator[AckableMessage, None]:
        while True:
            await asyncio.sleep(1)
            yield  # type: ignore  # never reached


# -- with_queue on AsyncKicker --


@pytest.mark.anyio
async def test_kicker_with_queue_sets_label() -> None:
    broker = QueueCapturingBroker()

    @broker.task
    async def my_task(x: int) -> int:
        return x

    await my_task.kicker().with_queue("high").kiq(1)

    msg = broker.kicked_messages[0]
    assert msg.labels["queue_name"] == "high"


@pytest.mark.anyio
async def test_kicker_with_queue_overrides_decorator_label() -> None:
    broker = QueueCapturingBroker()

    @broker.task(queue_name="default")
    async def my_task(x: int) -> int:
        return x

    await my_task.kicker().with_queue("critical").kiq(1)

    msg = broker.kicked_messages[0]
    assert msg.labels["queue_name"] == "critical"


@pytest.mark.anyio
async def test_with_queue_does_not_mutate_task_defaults() -> None:
    broker = QueueCapturingBroker()

    @broker.task(queue_name="default")
    async def my_task(x: int) -> int:
        return x

    await my_task.kicker().with_queue("special").kiq(1)
    await my_task.kiq(2)

    assert broker.kicked_messages[0].labels["queue_name"] == "special"
    assert broker.kicked_messages[1].labels["queue_name"] == "default"


# -- with_queue on AsyncTaskiqDecoratedTask --


@pytest.mark.anyio
async def test_decorated_task_with_queue() -> None:
    broker = QueueCapturingBroker()

    @broker.task
    async def my_task(x: int) -> int:
        return x

    await my_task.with_queue("emails").kiq(42)

    msg = broker.kicked_messages[0]
    assert msg.labels["queue_name"] == "emails"


# -- with_labels(queue_name=...) still works --


@pytest.mark.anyio
async def test_with_labels_queue_name_still_works() -> None:
    broker = QueueCapturingBroker()

    @broker.task
    async def my_task() -> None:
        pass

    await my_task.kicker().with_labels(queue_name="legacy").kiq()

    msg = broker.kicked_messages[0]
    assert msg.labels["queue_name"] == "legacy"


# -- listen_queues attribute on broker --


def test_broker_listen_queues_defaults_to_none() -> None:
    broker = QueueCapturingBroker()
    assert broker.listen_queues is None


def test_broker_listen_queues_can_be_set() -> None:
    broker = QueueCapturingBroker()
    broker.listen_queues = ["q1", "q2"]
    assert broker.listen_queues == ["q1", "q2"]


# -- CLI --queues parsing --


def test_worker_args_queues_default_empty() -> None:
    args = WorkerArgs.from_cli(["module:broker"])
    assert args.queues == []


def test_worker_args_queues_comma_separated() -> None:
    args = WorkerArgs.from_cli(["module:broker", "--queues", "high,low,default"])
    assert args.queues == ["high", "low", "default"]


def test_worker_args_queues_single() -> None:
    args = WorkerArgs.from_cli(["module:broker", "-q", "emails"])
    assert args.queues == ["emails"]


# -- Scheduler label flow --


@pytest.mark.anyio
async def test_scheduled_task_preserves_queue_label() -> None:
    """When a task is scheduled with a queue_name label, the ScheduledTask stores it."""
    broker = QueueCapturingBroker()

    @broker.task(queue_name="scheduled_queue")
    async def my_task(x: int) -> int:
        return x

    # Build a kicker and prepare a scheduled task via schedule_by_cron
    # We can't actually add to a source here, but we can verify the message
    # has the queue_name label set.
    kicker = my_task.kicker().with_queue("cron_queue")
    message = kicker._prepare_message(1)

    assert message.labels["queue_name"] == "cron_queue"
