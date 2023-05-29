import typing as t

import json

import asyncstdlib as alib
import pytest

from saturn_engine.core import Cursor
from saturn_engine.core import MessageId
from saturn_engine.worker.inventories import JoinedSubInventory
from saturn_engine.worker.inventory import Item
from saturn_engine.worker.inventory import SubInventory


class TestSubInventory(SubInventory):
    __test__ = False

    async def next_batch(
        self, source_item: Item, after: t.Optional[Cursor] = None
    ) -> list[Item]:
        items: list[Item] = []

        if source_item.args["a"] == 0:
            items = [
                Item(args={"b": {"c": 7}}, id=MessageId("7")),
                Item(args={"b": {"c": 8}}, id=MessageId("8")),
                Item(args={"b": {"c": 9}}, id=MessageId("9")),
            ]
        elif source_item.args["a"] == 1:
            items = [
                Item(args={"b": {"c": 4}}, id=MessageId("4")),
                Item(args={"b": {"c": 5}}, id=MessageId("5")),
                Item(args={"b": {"c": 6}}, id=MessageId("6")),
            ]
        elif source_item.args["a"] == 2:
            items = [
                Item(args={"b": {"c": 1}}, id=MessageId("1")),
                Item(args={"b": {"c": 2}}, id=MessageId("2")),
                Item(args={"b": {"c": 3}}, id=MessageId("3")),
            ]

        return [i for i in items if after is None or int(i.id) > int(after)]


TEST_SUB_INVENTORY_TYPE: t.Final[
    str
] = f"{TestSubInventory.__module__}.{TestSubInventory.__qualname__}"


@pytest.mark.asyncio
async def test_joined_sub_inventory() -> None:
    inventory = JoinedSubInventory.from_options(
        {
            "inventory": {
                "name": "a",
                "type": "StaticInventory",
                "options": {"items": [{"a": 0}, {"a": 1}, {"a": 2}]},
            },
            "sub_inventory": {
                "name": "b",
                "type": TEST_SUB_INVENTORY_TYPE,
            },
            "batch_size": 10,
        },
        services=None,
    )
    batch = await alib.list(inventory.iterate())
    assert [(json.loads(i.id), json.loads(i.cursor or ""), i.args) for i in batch] == [
        ({"a": "0", "b": "7"}, {"b": "7"}, {"a": {"a": 0}, "b": {"b": {"c": 7}}}),
        ({"a": "0", "b": "8"}, {"b": "8"}, {"a": {"a": 0}, "b": {"b": {"c": 8}}}),
        ({"a": "0", "b": "9"}, {"b": "9"}, {"a": {"a": 0}, "b": {"b": {"c": 9}}}),
        (
            {"a": "1", "b": "4"},
            {"a": "0", "b": "4"},
            {"a": {"a": 1}, "b": {"b": {"c": 4}}},
        ),
        (
            {"a": "1", "b": "5"},
            {"a": "0", "b": "5"},
            {"a": {"a": 1}, "b": {"b": {"c": 5}}},
        ),
        (
            {"a": "1", "b": "6"},
            {"a": "0", "b": "6"},
            {"a": {"a": 1}, "b": {"b": {"c": 6}}},
        ),
        (
            {"a": "2", "b": "1"},
            {"a": "1", "b": "1"},
            {"a": {"a": 2}, "b": {"b": {"c": 1}}},
        ),
        (
            {"a": "2", "b": "2"},
            {"a": "1", "b": "2"},
            {"a": {"a": 2}, "b": {"b": {"c": 2}}},
        ),
        (
            {"a": "2", "b": "3"},
            {"a": "1", "b": "3"},
            {"a": {"a": 2}, "b": {"b": {"c": 3}}},
        ),
    ]

    batch = await alib.list(inventory.iterate(after=Cursor('{"a": "0", "b": "6"}')))
    assert [(json.loads(i.id), json.loads(i.cursor or ""), i.args) for i in batch] == [
        (
            {"a": "2", "b": "1"},
            {"a": "1", "b": "1"},
            {"a": {"a": 2}, "b": {"b": {"c": 1}}},
        ),
        (
            {"a": "2", "b": "2"},
            {"a": "1", "b": "2"},
            {"a": {"a": 2}, "b": {"b": {"c": 2}}},
        ),
        (
            {"a": "2", "b": "3"},
            {"a": "1", "b": "3"},
            {"a": {"a": 2}, "b": {"b": {"c": 3}}},
        ),
    ]
    assert not await alib.list(inventory.iterate(after=Cursor('{"a": "1", "b": "3"}')))


@pytest.mark.asyncio
async def test_joined_sub_inventory_flatten() -> None:
    inventory = JoinedSubInventory.from_options(
        {
            "inventory": {
                "name": "a",
                "type": "StaticInventory",
                "options": {"items": [{"a": 0}]},
            },
            "sub_inventory": {
                "name": "b",
                "type": TEST_SUB_INVENTORY_TYPE,
            },
            "batch_size": 10,
            "flatten": True,
        },
        services=None,
    )
    batch = await alib.list(inventory.iterate())
    assert [(json.loads(i.id), json.loads(i.cursor or ""), i.args) for i in batch] == [
        ({"a": "0", "b": "7"}, {"b": "7"}, {"a": 0, "b": {"c": 7}}),
        ({"a": "0", "b": "8"}, {"b": "8"}, {"a": 0, "b": {"c": 8}}),
        ({"a": "0", "b": "9"}, {"b": "9"}, {"a": 0, "b": {"c": 9}}),
    ]


@pytest.mark.asyncio
async def test_joined_sub_inventory_alias() -> None:
    # Just alias
    inventory = JoinedSubInventory.from_options(
        {
            "inventory": {
                "name": "a",
                "type": "StaticInventory",
                "options": {"items": [{"a": 0}]},
            },
            "sub_inventory": {
                "name": "b",
                "type": TEST_SUB_INVENTORY_TYPE,
            },
            "batch_size": 10,
            "alias": "hello",
        },
        services=None,
    )
    batch = await alib.list(inventory.iterate())
    assert [(json.loads(i.id), json.loads(i.cursor or ""), i.args) for i in batch] == [
        (
            {"a": "0", "b": "7"},
            {"b": "7"},
            {"hello": {"a": {"a": 0}, "b": {"b": {"c": 7}}}},
        ),
        (
            {"a": "0", "b": "8"},
            {"b": "8"},
            {"hello": {"a": {"a": 0}, "b": {"b": {"c": 8}}}},
        ),
        (
            {"a": "0", "b": "9"},
            {"b": "9"},
            {"hello": {"a": {"a": 0}, "b": {"b": {"c": 9}}}},
        ),
    ]

    # Alias and flatten
    inventory = JoinedSubInventory.from_options(
        {
            "inventory": {
                "name": "a",
                "type": "StaticInventory",
                "options": {"items": [{"a": 0}]},
            },
            "sub_inventory": {
                "name": "b",
                "type": TEST_SUB_INVENTORY_TYPE,
            },
            "batch_size": 10,
            "alias": "hello",
            "flatten": True,
        },
        services=None,
    )
    batch = await alib.list(inventory.iterate())
    assert [(json.loads(i.id), json.loads(i.cursor or ""), i.args) for i in batch] == [
        ({"a": "0", "b": "7"}, {"b": "7"}, {"hello": {"a": 0, "b": {"c": 7}}}),
        ({"a": "0", "b": "8"}, {"b": "8"}, {"hello": {"a": 0, "b": {"c": 8}}}),
        ({"a": "0", "b": "9"}, {"b": "9"}, {"hello": {"a": 0, "b": {"c": 9}}}),
    ]
