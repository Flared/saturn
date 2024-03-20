import typing as t

import abc
import asyncio
import contextlib
import dataclasses
import json
import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack
from contextlib import suppress
from datetime import timedelta
from functools import cached_property
from itertools import islice

import asyncstdlib as alib

from saturn_engine.core import Cursor
from saturn_engine.core import MessageId
from saturn_engine.core.topic import TopicMessage
from saturn_engine.utils.log import getLogger
from saturn_engine.utils.options import OptionsSchema

MISSING = object()


@dataclasses.dataclass
class Item:
    args: dict[str, t.Any]
    id: MessageId = dataclasses.field(
        default_factory=lambda: MessageId(str(uuid.uuid4()))
    )
    cursor: t.Optional[Cursor] = MISSING  # type: ignore[assignment]
    tags: dict[str, str] = dataclasses.field(default_factory=dict)
    metadata: dict[str, t.Any] = dataclasses.field(default_factory=dict)
    config: dict[str, t.Any] = dataclasses.field(default_factory=dict)
    context: AsyncExitStack = dataclasses.field(
        default_factory=AsyncExitStack, compare=False
    )

    def as_topic_message(self) -> TopicMessage:
        return TopicMessage(
            id=self.id,
            args=self.args,
            tags=self.tags,
            metadata=self.metadata | {"job": {"cursor": self.cursor}},
            config=self.config,
        )

    # Hack to allow building object with `str` instead of new types `MessageId`
    # and `Cursor`.
    if t.TYPE_CHECKING:

        def __init__(
            self,
            *,
            args: dict[str, t.Any],
            id: str = None,  # type: ignore[assignment]
            cursor: t.Optional[str] = None,
            tags: dict[str, str] = None,  # type: ignore[assignment]
            metadata: dict[str, t.Any] = None,  # type: ignore[assignment]
            config: dict[str, t.Any] = None,  # type: ignore[assignment]
            context: AsyncExitStack = None,  # type: ignore[assignment]
        ) -> None: ...

    def __post_init__(self) -> None:
        if self.cursor is MISSING:
            self.cursor = Cursor(self.id)

    async def __aenter__(self) -> "Item":
        await self.context.__aenter__()
        return self

    async def __aexit__(self, *exc: t.Any) -> t.Optional[bool]:
        return await self.context.__aexit__(*exc)


class MaxRetriesError(Exception):
    pass


class RetryBatch(Exception):
    def __init__(self, *, delay: t.Optional[timedelta], max_retries: int = 1) -> None:
        super().__init__(delay)
        self.delay = delay
        self.max_retries = max_retries

    async def wait_delay(self) -> None:
        if self.delay:
            await asyncio.sleep(self.delay.total_seconds())

    def check_max_retries(self, retries_count: int) -> None:
        if retries_count >= self.max_retries:
            raise MaxRetriesError() from self.__cause__


class PendingItem(t.NamedTuple):
    is_pending: bool
    item: Item


@dataclasses.dataclass
class CursorsState:
    after: t.Optional[Cursor] = None
    partials: set[Cursor] = dataclasses.field(default_factory=set)
    max_partials: t.Optional[int] = None

    _pendings: dict[int, PendingItem] = dataclasses.field(
        default_factory=dict, init=False, repr=False
    )

    @classmethod
    def from_cursor(
        cls, raw: t.Optional[Cursor], /, *, max_partials: t.Optional[int] = None
    ) -> "CursorsState":
        state = None
        if raw:
            # Try parsing cursor format that might be in the old unstructured
            # format. We valid it is both parseable in json and has the
            # expected fields.
            with suppress(json.JSONDecodeError):
                data = json.loads(raw)
                if (
                    isinstance(data, dict)
                    and data.get("v") == 1
                    and not (data.keys() - ["v", "a", "p"])
                ):
                    state = data
            if state is None:
                state = {"a": raw}
        return cls.from_dict(state or {}, max_partials=max_partials)

    @classmethod
    def from_dict(
        cls, state: dict, /, *, max_partials: t.Optional[int] = None
    ) -> "CursorsState":
        return cls(
            after=state.get("a"),
            partials=set(state.get("p") or set()),
            max_partials=max_partials,
        )

    def as_dict(self) -> dict:
        data: dict = {"v": 1}
        if self.after is not None:
            data["a"] = self.after

        if self.max_partials != 0:
            partials_cursors = (
                p.item.cursor
                for p in self._pendings.values()
                if not p.is_pending and p.item.cursor is not None
            )
            if self.max_partials:
                partials = set(islice(partials_cursors, self.max_partials))
                partials = (
                    set(islice(self.partials, self.max_partials - len(partials)))
                    | partials
                )
            else:
                partials = self.partials | set(partials_cursors)

            if partials:
                data["p"] = list(sorted(partials))

        return data

    def as_cursor(self) -> t.Optional[Cursor]:
        d = self.as_dict()
        return Cursor(json.dumps(d)) if d != {"v": 1} else None

    @contextlib.asynccontextmanager
    async def process_item(self, item: Item) -> t.AsyncIterator[bool]:
        self._pendings[id(item)] = PendingItem(True, item)
        try:
            if item.cursor and item.cursor in self.partials:
                self.partials.remove(item.cursor)
                yield False
            else:
                yield True
        finally:
            self._pendings[id(item)] = PendingItem(False, item)

            # Collect the serie of done item from the beginning.
            items_done = []
            for pending, item in self._pendings.values():
                if pending:
                    break
                items_done.append(item)

            # Remove all done item from the pendings
            for item in items_done:
                del self._pendings[id(item)]

            # Commit last cursor.
            for item in reversed(items_done):
                if item.cursor:
                    self.after = item.cursor
                    break

    def has_pendings(self) -> bool:
        return bool(self._pendings)


class Inventory(abc.ABC, OptionsSchema):
    name: str
    _cursors: t.Optional[CursorsState] = None
    _is_done_event: t.Optional[asyncio.Event] = None

    max_partials: t.Optional[int] = 100

    @abc.abstractmethod
    async def next_batch(self, after: t.Optional[Cursor] = None) -> list[Item]:
        """Returns a batch of item with id greater than `after`."""
        raise NotImplementedError()

    async def iterate(self, after: t.Optional[Cursor] = None) -> AsyncIterator[Item]:
        """Returns an iterable that goes over the whole inventory."""
        retries_count = 0
        while True:
            try:
                batch = await self.next_batch(after)
            except RetryBatch as e:
                e.check_max_retries(retries_count)
                if e.__cause__:
                    self.logger.warning("Retrying to get batch", exc_info=e)
                await e.wait_delay()
                retries_count += 1
                continue
            else:
                retries_count = 0

            if not batch:
                return
            for item in batch:
                yield item
            after = item.cursor

    async def run(self, after: t.Optional[Cursor] = None) -> t.AsyncIterator[Item]:
        self._is_done_event = asyncio.Event()
        self._cursors = CursorsState.from_cursor(after, max_partials=self.max_partials)
        async for item in self.iterate(after=self._cursors.after):
            item.context.callback(self._check_has_pendings)
            self._is_done_event.clear()
            if not await item.context.enter_async_context(
                self._cursors.process_item(item)
            ):
                # Items has already been processed, skipping.
                async with item:
                    continue
            yield item
        else:
            self._check_has_pendings()

    @property
    def cursor(self) -> t.Optional[Cursor]:
        if self._cursors:
            return self._cursors.as_cursor()
        return None

    @cached_property
    def logger(self) -> logging.Logger:
        return getLogger(__name__, self)

    async def join(self) -> None:
        if self._is_done_event:
            await self._is_done_event.wait()

    def _check_has_pendings(self) -> None:
        if not self.has_pendings() and self._is_done_event:
            self._is_done_event.set()

    def has_pendings(self) -> bool:
        if self._cursors:
            return self._cursors.has_pendings()
        return False

    async def open(self) -> None:
        pass


class IteratorInventory(Inventory):
    def __init__(self, *, batch_size: t.Optional[int] = None, **kwargs: object) -> None:
        self.batch_size = batch_size or 10

    async def next_batch(self, after: t.Optional[Cursor] = None) -> list[Item]:
        batch: list[Item] = await alib.list(
            alib.islice(self.iterate(after=after), self.batch_size)
        )
        return batch

    @abc.abstractmethod
    async def iterate(self, after: t.Optional[Cursor] = None) -> AsyncIterator[Item]:
        raise NotImplementedError()
        yield


class BlockingInventory(Inventory, abc.ABC):
    async def next_batch(self, after: t.Optional[Cursor] = None) -> list[Item]:
        return await asyncio.get_event_loop().run_in_executor(
            None,
            self.next_batch_blocking,
            after,
        )

    @abc.abstractmethod
    def next_batch_blocking(self, after: t.Optional[Cursor] = None) -> list[Item]:
        raise NotImplementedError()


class SubInventory(abc.ABC, OptionsSchema):
    @abc.abstractmethod
    async def next_batch(
        self, source_item: Item, after: t.Optional[Cursor] = None
    ) -> list[Item]:
        """Returns a batch of item with id greater than `after`."""
        raise NotImplementedError()

    async def iterate(
        self, source_item: Item, after: t.Optional[Cursor] = None
    ) -> AsyncIterator[Item]:
        """Returns an iterable that goes over the whole inventory."""
        retries_count = 0
        while True:
            try:
                batch = await self.next_batch(source_item, after)
            except RetryBatch as e:
                e.check_max_retries(retries_count)
                if e.__cause__:
                    self.logger.warning("Retrying to get batch", exc_info=e)
                await e.wait_delay()
                retries_count += 1
                continue
            else:
                retries_count = 0

            if not batch:
                return

            for item in batch:
                yield item

            after = item.cursor

    @cached_property
    def logger(self) -> logging.Logger:
        return logging.getLogger(__name__ + ".SubInventory")


class BlockingSubInventory(SubInventory, abc.ABC):
    async def next_batch(
        self, source_item: Item, after: t.Optional[Cursor] = None
    ) -> list[Item]:
        return await asyncio.get_event_loop().run_in_executor(
            None,
            self.next_batch_blocking,
            source_item,
            after,
        )

    @abc.abstractmethod
    def next_batch_blocking(
        self, source_item: Item, after: t.Optional[Cursor] = None
    ) -> list[Item]:
        raise NotImplementedError()
