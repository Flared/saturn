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
    context: AsyncExitStack = dataclasses.field(
        default_factory=AsyncExitStack, compare=False
    )

    def as_topic_message(self) -> TopicMessage:
        return TopicMessage(
            id=self.id,
            args=self.args,
            tags=self.tags,
            metadata=self.metadata | {"job": {"cursor": self.cursor}},
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
            context: AsyncExitStack = None,  # type: ignore[assignment]
        ) -> None:
            ...

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
    _pendings: dict[int, PendingItem] = dataclasses.field(
        default_factory=dict, init=False, repr=False
    )

    @classmethod
    def from_cursor(cls, raw: t.Optional[Cursor], /) -> "CursorsState":
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
        return cls.from_dict(state or {})

    @classmethod
    def from_dict(cls, state: dict) -> "CursorsState":
        return cls(
            after=state.get("a"),
            partials=set(state.get("p") or set()),
        )

    def as_dict(self) -> dict:
        data: dict = {"v": 1}
        if self.after is not None:
            data["a"] = self.after

        partials = self.partials | {
            p.item.cursor
            for p in self._pendings.values()
            if not p.is_pending and p.item.cursor is not None
        }
        if partials:
            data["p"] = list(sorted(partials))

        return data

    def as_cursor(self) -> Cursor:
        return Cursor(json.dumps(self.as_dict()))

    @contextlib.contextmanager
    def process_item(self, item: Item) -> t.Iterator[None]:
        self._pendings[id(item)] = PendingItem(True, item)
        try:
            yield
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


class Inventory(abc.ABC, OptionsSchema):
    name: str

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
        self._cursors = CursorsState.from_cursor(after)
        async for item in self.iterate(after=self._cursors.after):
            item.context.enter_context(self._cursors.process_item(item))
            yield item

    @property
    def cursor(self) -> Cursor:
        return self._cursors.as_cursor()

    @cached_property
    def logger(self) -> logging.Logger:
        return getLogger(__name__, self)


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
