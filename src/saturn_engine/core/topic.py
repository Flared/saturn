from typing import Any
from typing import Optional

import dataclasses
import uuid


@dataclasses.dataclass
class TopicMessage:
    #: Message arguments used to call the pipeline.
    args: dict[str, Optional[Any]]

    #: Unique message id.
    id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))

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
