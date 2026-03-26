from typing import Any

from pydantic import BaseModel, field_validator

from taskiq.labels import parse_label

DEFAULT_QUEUE = "default"


class TaskiqMessage(BaseModel):
    """
    Message abstractions.

    This an internal class used
    by brokers. Every remote call
    receive such messages.
    """

    task_id: str
    task_name: str
    queue: str = DEFAULT_QUEUE
    labels: dict[str, Any]
    labels_types: dict[str, int] | None = None
    args: list[Any]
    kwargs: dict[str, Any]

    @field_validator("queue")
    @classmethod
    def queue_must_not_be_empty(cls, v: str) -> str:
        """Validate that queue name is not empty or whitespace."""
        if not v or not v.strip():
            raise ValueError("Queue name must not be empty or whitespace")
        return v

    def parse_labels(self) -> None:
        """
        Parse labels.

        :return: None
        """
        if self.labels_types is None:
            return

        for label, label_type in self.labels_types.items():
            if label in self.labels:
                self.labels[label] = parse_label(self.labels[label], label_type)


class BrokerMessage(BaseModel):
    """Format of messages for brokers."""

    task_id: str
    task_name: str
    queue: str = DEFAULT_QUEUE
    message: bytes
    labels: dict[str, Any]

    @field_validator("queue")
    @classmethod
    def queue_must_not_be_empty(cls, v: str) -> str:
        """Validate that queue name is not empty or whitespace."""
        if not v or not v.strip():
            raise ValueError("Queue name must not be empty or whitespace")
        return v
