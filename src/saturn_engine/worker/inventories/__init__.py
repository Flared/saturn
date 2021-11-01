import dataclasses
from collections.abc import Iterable
from typing import Optional

from saturn_engine.utils.options import OptionsSchema


@dataclasses.dataclass
class Item:
    id: int
    data: dict[str, object]


class Inventory(OptionsSchema):
    async def next_batch(self, after: Optional[int] = None) -> Iterable[Item]:
        return []
