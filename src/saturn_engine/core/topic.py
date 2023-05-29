from typing import Any
from typing import Optional

import dataclasses
import uuid

from .types import MessageId


@dataclasses.dataclass
class TopicMessage:
    #: Message arguments used to call the pipeline.
    args: dict[str, Optional[Any]]

    #: Unique message id.
    id: MessageId = dataclasses.field(
        default_factory=lambda: MessageId(str(uuid.uuid4()))
    )

    #: Tags to attach to observability (logging, events, metrics and tracing).
    tags: dict[str, str] = dataclasses.field(default_factory=dict)

    #: Service-specific configuration overriding the job or global config.
    config: dict[str, dict[str, Optional[Any]]] = dataclasses.field(
        default_factory=dict
    )

    #: Service-specific data used in the message lifecycle.
    metadata: dict[str, dict[str, Optional[Any]]] = dataclasses.field(
        default_factory=dict
    )

    def extend(self, args: dict[str, object]) -> "TopicMessage":
        return dataclasses.replace(self, args=args | self.args)
