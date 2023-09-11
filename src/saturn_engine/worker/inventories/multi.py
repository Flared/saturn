import typing as t

import contextlib
import dataclasses
import json
import operator
from contextlib import AsyncExitStack
from functools import reduce

from saturn_engine.core import Cursor
from saturn_engine.worker.inventory import Inventory
from saturn_engine.worker.inventory import Item


class MultiItems(t.NamedTuple):
    ids: dict[str, str]
    cursors: dict[str, t.Optional[str]]
    args: dict[str, dict[str, t.Any]]
    tags: dict[str, str]
    metadata: dict[str, t.Any]

    @classmethod
    def from_one(cls, item: Item, *, name: str) -> "MultiItems":
        return MultiItems(
            ids={name: item.id},
            cursors={name: item.cursor},
            args={name: item.args},
            tags=item.tags,
            metadata=item.metadata,
        )

    def with_item(self, item: Item, *, name: str) -> "MultiItems":
        return self._replace(
            ids=self.ids | {name: item.id},
            cursors=self.cursors | {name: item.cursor},
            args=self.args | {name: item.args},
            tags=self.tags | item.tags,
            metadata=self.metadata | item.metadata,
        )

    def as_item(
        self,
        *,
        alias: t.Optional[str] = None,
        flatten: bool = False,
        context: t.Optional[AsyncExitStack] = None,
    ) -> Item:
        args = self.args
        if flatten:
            args = reduce(operator.or_, args.values(), {})
        if alias:
            args = {alias: args}

        return Item(
            id=json.dumps(self.ids),
            cursor=json.dumps(self.cursors),
            args=args,
            metadata=self.metadata,
            tags=self.tags,
            context=context or AsyncExitStack(),
        )


@dataclasses.dataclass
class MultiCursorsState:
    after: t.Optional[Cursor] = None
    partials: dict[Cursor, Cursor] = dataclasses.field(default_factory=dict)

    @classmethod
    def from_cursor(cls, raw: t.Optional[Cursor], /) -> "MultiCursorsState":
        state = None
        if raw:
            # Try parsing cursor format that might be in the old unstructured
            # format. We valid it is both parseable in json and has the
            # expected fields.
            with contextlib.suppress(json.JSONDecodeError):
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
    def from_dict(cls, state: dict) -> "MultiCursorsState":
        return cls(
            after=state.get("a"),
            partials=state.get("p") or {},
        )

    def as_dict(self) -> dict:
        data: dict = {"v": 1}
        if self.after is not None:
            data["a"] = self.after
        if self.partials:
            data["p"] = self.partials
        return data

    def as_cursor(self) -> Cursor:
        return Cursor(json.dumps(self.as_dict()))

    def process_root(self, inventory: Inventory) -> "RootCursors":
        return RootCursors(cursors=self, inventory=inventory)

    def process_sub(self, subinv: Inventory, *, parent: Item) -> "SubCursors":
        return SubCursors(cursors=self, parent=parent, inventory=subinv)


@dataclasses.dataclass
class RootCursors:
    cursors: MultiCursorsState
    inventory: Inventory

    @contextlib.contextmanager
    def process(self, item: Item) -> t.Iterator[None]:
        try:
            yield
        finally:
            self.cursors.after = self.inventory.cursor
            if item.cursor is not None:
                self.cursors.partials.pop(item.cursor, None)


@dataclasses.dataclass
class SubCursors:
    parent: Item
    cursors: MultiCursorsState
    inventory: Inventory

    @contextlib.contextmanager
    def process(self) -> t.Iterator[None]:
        try:
            yield
        finally:
            parent_cursor = self.parent.cursor
            sub_cursor = self.inventory.cursor
            if parent_cursor is not None and sub_cursor is not None:
                self.cursors.partials[parent_cursor] = sub_cursor

    @property
    def cursor(self) -> t.Optional[Cursor]:
        if (c := self.parent.cursor) is not None:
            return self.cursors.partials.get(c)
        return None
