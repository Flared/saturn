import dataclasses
from typing import Any


@dataclasses.dataclass(eq=False)
class Resource:
    data: Any
