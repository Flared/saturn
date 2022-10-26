from typing import Any
from typing import Optional

import dataclasses
import uuid


@dataclasses.dataclass
class TopicMessage:
    args: dict[str, Optional[Any]]
    id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))
    tags: dict[str, str] = dataclasses.field(default_factory=dict)
    metadata: dict[str, Optional[Any]] = dataclasses.field(default_factory=dict)

    def extend(self, args: dict[str, object]) -> "TopicMessage":
        return self.__class__(
            id=self.id, args=args | self.args, tags=self.tags, metadata=self.metadata
        )
