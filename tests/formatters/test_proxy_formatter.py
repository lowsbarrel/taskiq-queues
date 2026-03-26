import json

from taskiq.brokers.inmemory_broker import InMemoryBroker
from taskiq.message import DEFAULT_QUEUE, BrokerMessage, TaskiqMessage


async def test_proxy_dumps() -> None:
    # uses json serializer by default
    broker = InMemoryBroker()
    msg = TaskiqMessage(
        task_id="task-id",
        task_name="task.name",
        labels={"label1": 1, "label2": "text"},
        args=[1, "a"],
        kwargs={"p1": "v1"},
    )
    dumped = broker.formatter.dumps(msg)
    assert dumped.task_id == "task-id"
    assert dumped.task_name == "task.name"
    assert dumped.queue == DEFAULT_QUEUE
    assert dumped.labels == {"label1": 1, "label2": "text"}
    payload = json.loads(dumped.message)
    assert payload["task_id"] == "task-id"
    assert payload["task_name"] == "task.name"
    assert payload["queue"] == DEFAULT_QUEUE
    assert payload["args"] == [1, "a"]
    assert payload["kwargs"] == {"p1": "v1"}


async def test_proxy_loads() -> None:
    # uses json serializer by default
    broker = InMemoryBroker()
    msg = (
        b'{"task_id":"task-id","task_name":"task.name",'
        b'"labels":{"label1":1,"label2":"text"},'
        b'"args":[1,"a"],"kwargs":{"p1":"v1"}}'
    )
    expected = TaskiqMessage(
        task_id="task-id",
        task_name="task.name",
        labels={"label1": 1, "label2": "text"},
        args=[1, "a"],
        kwargs={"p1": "v1"},
    )
    assert broker.formatter.loads(msg) == expected
