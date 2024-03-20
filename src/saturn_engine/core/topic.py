import typing as t

import dataclasses
import datetime
import uuid

from .types import MessageId


@dataclasses.dataclass
class TopicMessage:
    #: Message arguments used to call the pipeline.
    args: dict[str, t.Optional[t.Any]]

    #: Unique message id.
    id: MessageId = dataclasses.field(
        default_factory=lambda: MessageId(str(uuid.uuid4()))
    )

    #: Tags to attach to observability (logging, events, metrics and tracing).
    tags: dict[str, str] = dataclasses.field(default_factory=dict)

    #: Service-specific configuration overriding the job or global config.
    config: dict[str, dict[str, t.Optional[t.Any]]] = dataclasses.field(
        default_factory=dict
    )

    #: Service-specific data used in the message lifecycle.
    metadata: dict[str, dict[str, t.Optional[t.Any]]] = dataclasses.field(
        default_factory=dict
    )

    expire_after: t.Optional[float] = None

    # Hack to allow building object with `str` instead of new types `MessageId`
    # and `Cursor`.
    if t.TYPE_CHECKING:

        def __init__(
            self,
            args: dict[str, t.Any],
            *,
            id: str = None,  # type: ignore[assignment]
            tags: dict[str, str] = None,  # type: ignore[assignment]
            config: dict[str, dict[str, t.Optional[t.Any]]] = None,  # type: ignore[assignment]
            metadata: dict[str, dict[str, t.Optional[t.Any]]] = None,  # type: ignore[assignment]
            expire_after: t.Optional[datetime.timedelta | float] = None,
        ) -> None: ...

    def __post_init__(self) -> None:
        if isinstance(self.expire_after, datetime.timedelta):
            self.expire_after = self.expire_after.total_seconds()

    def extend(self, args: dict[str, object]) -> "TopicMessage":
        return dataclasses.replace(self, args=args | self.args)

    async def __aenter__(self) -> "TopicMessage":
        return self

    async def __aexit__(self, *args: t.Any, **kwargs: t.Any) -> None:
        return None
