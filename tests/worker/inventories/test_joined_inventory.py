import json

import asyncstdlib as alib
import pytest

from saturn_engine.worker.inventories import JoinedInventory


@pytest.mark.asyncio
async def test_joined_inventory() -> None:
    inventory = JoinedInventory.from_options(
        {
            "inventories": [
                {
                    "name": "a",
                    "type": "StaticInventory",
                    "options": {"items": [{"n": 1}, {"n": 2}, {"n": 3}]},
                },
                {
                    "name": "b",
                    "type": "StaticInventory",
                    "options": {"items": [{"c": "A"}, {"c": "B"}, {"c": "C"}]},
                },
            ],
            "batch_size": 10,
        },
        services=None,
    )
    batch = await alib.list(inventory.iterate())
    assert [(json.loads(i.id), i.args) for i in batch] == [
        ({"b": "0"}, {"a": {"n": 1}, "b": {"c": "A"}}),
        ({"b": "1"}, {"a": {"n": 1}, "b": {"c": "B"}}),
        ({"b": "2"}, {"a": {"n": 1}, "b": {"c": "C"}}),
        ({"a": "0", "b": "0"}, {"a": {"n": 2}, "b": {"c": "A"}}),
        ({"a": "0", "b": "1"}, {"a": {"n": 2}, "b": {"c": "B"}}),
        ({"a": "0", "b": "2"}, {"a": {"n": 2}, "b": {"c": "C"}}),
        ({"a": "1", "b": "0"}, {"a": {"n": 3}, "b": {"c": "A"}}),
        ({"a": "1", "b": "1"}, {"a": {"n": 3}, "b": {"c": "B"}}),
        ({"a": "1", "b": "2"}, {"a": {"n": 3}, "b": {"c": "C"}}),
    ]

    batch = await alib.list(inventory.iterate(after='{"a": "0", "b": "1"}'))
    assert [(json.loads(i.id), i.args) for i in batch] == [
        ({"a": "0", "b": "2"}, {"a": {"n": 2}, "b": {"c": "C"}}),
        ({"a": "1", "b": "0"}, {"a": {"n": 3}, "b": {"c": "A"}}),
        ({"a": "1", "b": "1"}, {"a": {"n": 3}, "b": {"c": "B"}}),
        ({"a": "1", "b": "2"}, {"a": {"n": 3}, "b": {"c": "C"}}),
    ]
    assert not await alib.list(inventory.iterate(after='{"a": "1", "b": "2"}'))


@pytest.mark.asyncio
async def test_joined_inventory_flatten() -> None:
    inventory = JoinedInventory.from_options(
        {
            "flatten": True,
            "inventories": [
                {
                    "name": "a",
                    "type": "StaticInventory",
                    "options": {"items": [{"n": 1}]},
                },
                {
                    "name": "b",
                    "type": "StaticInventory",
                    "options": {"items": [{"c": "A"}]},
                },
            ],
            "batch_size": 10,
        },
        services=None,
    )
    batch = await alib.list(inventory.iterate())
    assert [(json.loads(i.id), i.args) for i in batch] == [
        ({"b": "0"}, {"n": 1, "c": "A"})
    ]
