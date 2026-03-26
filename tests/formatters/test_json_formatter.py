import json

from taskiq.formatters.json_formatter import JSONFormatter
from taskiq.message import DEFAULT_QUEUE, BrokerMessage, TaskiqMessage


async def test_json_dumps() -> None:
    fmt = JSONFormatter()
    msg = TaskiqMessage(
        task_id="task-id",
        task_name="task.name",
        labels={"label1": 1, "label2": "text"},
        args=[1, "a"],
        kwargs={"p1": "v1"},
    )
    dumped = fmt.dumps(msg)
    assert dumped.task_id == "task-id"
    assert dumped.task_name == "task.name"
    assert dumped.queue == DEFAULT_QUEUE
    assert dumped.labels == {"label1": 1, "label2": "text"}
    payload = json.loads(dumped.message)
    assert payload["task_id"] == "task-id"
    assert payload["task_name"] == "task.name"
    assert payload["queue"] == DEFAULT_QUEUE
    assert payload["labels"] == {"label1": 1, "label2": "text"}
    assert payload["args"] == [1, "a"]
    assert payload["kwargs"] == {"p1": "v1"}


async def test_json_loads() -> None:
    fmt = JSONFormatter()
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
    assert fmt.loads(msg) == expected
