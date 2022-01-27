import json

import asyncstdlib as alib
import pytest

from saturn_engine.worker.inventories.chained import ChainedInventory


@pytest.mark.asyncio
async def test_chained_inventory() -> None:
    inventory = ChainedInventory.from_options(
        {
            "inventories": [
                {
                    "name": "a",
                    "type": "StaticInventory",
                    "options": {"items": [{"a": 1}, {"a": 2}, {"a": 3}]},
                },
                {
                    "name": "b",
                    "type": "StaticInventory",
                    "options": {"items": [{"b": "1"}, {"b": "2"}, {"b": "3"}]},
                },
                {
                    "name": "c",
                    "type": "StaticInventory",
                    "options": {"items": [{"c": "1"}, {"c": "2"}, {"c": "3"}]},
                },
            ],
            "batch_size": 10,
        },
        services=None,
    )
    batch = await alib.list(inventory.iterate())
    assert [(json.loads(i.id), i.args) for i in batch] == [
        ({"a": "0"}, {"a": {"a": 1}}),
        ({"a": "1"}, {"a": {"a": 2}}),
        ({"a": "2"}, {"a": {"a": 3}}),
        ({"b": "0"}, {"b": {"b": "1"}}),
        ({"b": "1"}, {"b": {"b": "2"}}),
        ({"b": "2"}, {"b": {"b": "3"}}),
        ({"c": "0"}, {"c": {"c": "1"}}),
        ({"c": "1"}, {"c": {"c": "2"}}),
        ({"c": "2"}, {"c": {"c": "3"}}),
    ]

    batch = await alib.list(inventory.iterate(after='{"b": "1"}'))
    assert [(json.loads(i.id), i.args) for i in batch] == [
        ({"b": "2"}, {"b": {"b": "3"}}),
        ({"c": "0"}, {"c": {"c": "1"}}),
        ({"c": "1"}, {"c": {"c": "2"}}),
        ({"c": "2"}, {"c": {"c": "3"}}),
    ]
    assert not await alib.list(inventory.iterate(after='{"c": "2"}'))
