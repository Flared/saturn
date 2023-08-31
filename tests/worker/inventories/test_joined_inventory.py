import typing as t

import dataclasses
import itertools
import json

import asyncstdlib as alib
import pytest

from saturn_engine.core import Cursor
from saturn_engine.utils.inspect import get_import_name
from saturn_engine.worker.inventories.joined import JoinInventory
from saturn_engine.worker.inventory import Inventory
from saturn_engine.worker.inventory import Item
from saturn_engine.worker.inventory import IteratorInventory


class FakeSubInventory(IteratorInventory):
    @dataclasses.dataclass
    class Options:
        parent_item: Item

    def __init__(self, *args: t.Any, options: Options, **kwargs: t.Any) -> None:
        self.options = options

    async def iterate(self, after: t.Optional[Cursor] = None) -> t.AsyncIterator[Item]:
        iterable = iter(enumerate("12"))
        if after:
            itertools.islice(iterable, int(after))
        for i, x in iterable:
            yield Item(
                id=x, cursor=str(i), args={self.options.parent_item.args["key"]: x}
            )


async def iterate_with_cursor(
    inventory: Inventory, after: t.Optional[Cursor] = None
) -> t.AsyncIterator[tuple[Item, t.Optional[Cursor]]]:
    async for item in inventory.run(after=after):
        async with item:
            pass
        yield item, inventory.cursor


@pytest.mark.asyncio
async def test_join_inventory() -> None:
    def make_inventory() -> JoinInventory:
        return JoinInventory.from_options(
            {
                "root": {
                    "name": "A",
                    "type": "StaticInventory",
                    "options": {"items": [{"n": 1}, {"n": 2}, {"n": 3}]},
                },
                "join": {
                    "name": "B",
                    "type": "StaticInventory",
                    "options": {"items": [{"c": "A"}, {"c": "B"}, {"c": "C"}]},
                },
                "batch_size": 10,
                "max_concurrency": 1,
            },
            services=None,
        )

    inventory = make_inventory()
    batch = await alib.list(iterate_with_cursor(inventory))
    assert [(json.loads(i.id), i.args) for i, _ in batch] == [
        ({"A": "0", "B": "0"}, {"A": {"n": 1}, "B": {"c": "A"}}),
        ({"A": "0", "B": "1"}, {"A": {"n": 1}, "B": {"c": "B"}}),
        ({"A": "0", "B": "2"}, {"A": {"n": 1}, "B": {"c": "C"}}),
        ({"A": "1", "B": "0"}, {"A": {"n": 2}, "B": {"c": "A"}}),
        ({"A": "1", "B": "1"}, {"A": {"n": 2}, "B": {"c": "B"}}),
        ({"A": "1", "B": "2"}, {"A": {"n": 2}, "B": {"c": "C"}}),
        ({"A": "2", "B": "0"}, {"A": {"n": 3}, "B": {"c": "A"}}),
        ({"A": "2", "B": "1"}, {"A": {"n": 3}, "B": {"c": "B"}}),
        ({"A": "2", "B": "2"}, {"A": {"n": 3}, "B": {"c": "C"}}),
    ]

    assert [json.loads(c) for _, c in batch if c] == [
        {"v": 1, "p": {"0": '{"v": 1, "a": "0"}'}},
        {"v": 1, "p": {"0": '{"v": 1, "a": "1"}'}},
        {"v": 1, "p": {"0": '{"v": 1, "a": "2"}'}},
        {"v": 1, "a": '{"v": 1, "a": "0"}', "p": {"1": '{"v": 1, "a": "0"}'}},
        {"v": 1, "a": '{"v": 1, "a": "0"}', "p": {"1": '{"v": 1, "a": "1"}'}},
        {"v": 1, "a": '{"v": 1, "a": "0"}', "p": {"1": '{"v": 1, "a": "2"}'}},
        {"v": 1, "a": '{"v": 1, "a": "1"}', "p": {"2": '{"v": 1, "a": "0"}'}},
        {"v": 1, "a": '{"v": 1, "a": "1"}', "p": {"2": '{"v": 1, "a": "1"}'}},
        {"v": 1, "a": '{"v": 1, "a": "1"}', "p": {"2": '{"v": 1, "a": "2"}'}},
    ]
    assert json.loads(inventory.cursor) == {"v": 1, "a": '{"v": 1, "a": "2"}'}

    inventory = make_inventory()
    batch = await alib.list(
        iterate_with_cursor(
            inventory,
            after=Cursor(
                json.dumps(
                    {
                        "v": 1,
                        "a": '{"v": 1, "a": "0"}',
                        "p": {"1": '{"v": 1, "a": "1"}'},
                    }
                )
            ),
        )
    )
    assert [(json.loads(i.id), i.args) for i, _ in batch] == [
        ({"A": "1", "B": "2"}, {"A": {"n": 2}, "B": {"c": "C"}}),
        ({"A": "2", "B": "0"}, {"A": {"n": 3}, "B": {"c": "A"}}),
        ({"A": "2", "B": "1"}, {"A": {"n": 3}, "B": {"c": "B"}}),
        ({"A": "2", "B": "2"}, {"A": {"n": 3}, "B": {"c": "C"}}),
    ]
    assert [json.loads(c) for _, c in batch if c] == [
        {"v": 1, "a": '{"v": 1, "a": "0"}', "p": {"1": '{"v": 1, "a": "2"}'}},
        {"v": 1, "a": '{"v": 1, "a": "1"}', "p": {"2": '{"v": 1, "a": "0"}'}},
        {"v": 1, "a": '{"v": 1, "a": "1"}', "p": {"2": '{"v": 1, "a": "1"}'}},
        {"v": 1, "a": '{"v": 1, "a": "1"}', "p": {"2": '{"v": 1, "a": "2"}'}},
    ]

    inventory = make_inventory()
    assert not await alib.list(
        inventory.run(after=Cursor(r'{"v": 1, "a": "{\"v\": 1, \"a\": \"2\"}"}'))
    )


@pytest.mark.asyncio
async def test_join_inventory_flatten() -> None:
    inventory = JoinInventory.from_options(
        {
            "flatten": True,
            "root": {
                "name": "a",
                "type": "StaticInventory",
                "options": {"items": [{"n": 1}]},
            },
            "join": {
                "name": "b",
                "type": "StaticInventory",
                "options": {"items": [{"c": "A"}]},
            },
            "batch_size": 10,
            "max_concurrency": 1,
        },
        services=None,
    )
    batch = await alib.list(inventory.run())
    assert [(json.loads(i.id), i.args) for i in batch] == [
        ({"a": "0", "b": "0"}, {"n": 1, "c": "A"})
    ]


@pytest.mark.asyncio
async def test_join_inventory_alias() -> None:
    # Just alias
    inventory = JoinInventory.from_options(
        {
            "alias": "veggie_fruit",
            "root": {
                "name": "fruits",
                "type": "StaticInventory",
                "options": {"items": [{"fruit_name": "apple"}]},
            },
            "join": {
                "name": "veggies",
                "type": "StaticInventory",
                "options": {"items": [{"veggie_name": "carrot"}]},
            },
            "batch_size": 10,
        },
        services=None,
    )
    batch = await alib.list(inventory.run())
    assert [(json.loads(i.id), i.args) for i in batch] == [
        (
            {"fruits": "0", "veggies": "0"},
            {
                "veggie_fruit": {
                    "veggies": {
                        "veggie_name": "carrot",
                    },
                    "fruits": {
                        "fruit_name": "apple",
                    },
                }
            },
        ),
    ]

    # Alias and flatten
    inventory = JoinInventory.from_options(
        {
            "alias": "veggie_fruit",
            "flatten": True,
            "root": {
                "name": "fruits",
                "type": "StaticInventory",
                "options": {"items": [{"fruit_name": "apple"}]},
            },
            "join": {
                "name": "veggies",
                "type": "StaticInventory",
                "options": {"items": [{"veggie_name": "carrot"}]},
            },
            "batch_size": 10,
        },
        services=None,
    )
    batch = await alib.list(inventory.run())
    assert [(json.loads(i.id), i.args) for i in batch] == [
        (
            {"fruits": "0", "veggies": "0"},
            {
                "veggie_fruit": {
                    "veggie_name": "carrot",
                    "fruit_name": "apple",
                }
            },
        ),
    ]


async def test_join_sub_inventories() -> None:
    inventory = JoinInventory.from_options(
        {
            "root": {
                "name": "A",
                "type": "StaticInventory",
                "options": {"items": [{"key": "a"}, {"key": "b"}]},
            },
            "join": {
                "name": "B",
                "type": get_import_name(FakeSubInventory),
            },
            "batch_size": 10,
            "max_concurrency": 1,
            "flatten": True,
        },
        services=None,
    )

    batch = await alib.list(iterate_with_cursor(inventory))
    assert [(json.loads(i.id), i.args) for i, _ in batch] == [
        ({"A": "0", "B": "1"}, {"key": "a", "a": "1"}),
        ({"A": "0", "B": "2"}, {"key": "a", "a": "2"}),
        ({"A": "1", "B": "1"}, {"key": "b", "b": "1"}),
        ({"A": "1", "B": "2"}, {"key": "b", "b": "2"}),
    ]

    assert [json.loads(c) for _, c in batch if c] == [
        {"v": 1, "p": {"0": '{"v": 1, "a": "0"}'}},
        {"v": 1, "p": {"0": '{"v": 1, "a": "1"}'}},
        {"v": 1, "a": '{"v": 1, "a": "0"}', "p": {"1": '{"v": 1, "a": "0"}'}},
        {"v": 1, "a": '{"v": 1, "a": "0"}', "p": {"1": '{"v": 1, "a": "1"}'}},
    ]
