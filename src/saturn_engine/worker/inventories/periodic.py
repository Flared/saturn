from typing import Optional

import asyncio
import dataclasses
import datetime
from collections.abc import AsyncIterator

import croniter

from saturn_engine.utils import utcnow

from . import Item
from . import IteratorInventory


class PeriodicInventory(IteratorInventory):
    """
    PeriodicInventory yields all datetimes starting from start_date that match
    the given period.

    Dates in the future will not be returned. Instead, the inventory will wait
    until the date is reached.
    """

    @dataclasses.dataclass
    class Options:
        interval: str
        start_date: Optional[datetime.datetime] = None
        end_date: Optional[datetime.datetime] = None
        batch_size: int = 1

    def __init__(self, options: Options, **kwargs: object) -> None:
        super().__init__(
            options=options,
            batch_size=options.batch_size or 1,
            **kwargs,
        )
        self.start_date = options.start_date or utcnow()
        self.end_date = options.end_date
        self.interval = options.interval

    async def iterate(self, after: Optional[str] = None) -> AsyncIterator[Item]:
        after_date = (
            datetime.datetime.fromisoformat(after) if after is not None else None
        )
        resume_from = after_date or self.start_date
        for tick in croniter.croniter(
            self.interval,
            resume_from,
            ret_type=datetime.datetime,
        ):
            if tick == after_date:
                continue
            if self.end_date is not None and tick > self.end_date:
                break
            now = utcnow()
            if tick > now:
                await asyncio.sleep((tick - now).total_seconds())
            yield Item(
                id=tick.isoformat(),
                args={"timestamp": tick.isoformat()},
            )
