import json
from collections import Counter

import asyncstdlib as alib
import pytest

from saturn_engine.core.types import Cursor
from saturn_engine.core.types import MessageId
from saturn_engine.worker.inventories.fanin import FanIn
from saturn_engine.worker.inventories.fanin import PriorityFanIn
from saturn_engine.worker.inventory import Item


@pytest.mark.asyncio
async def test_fanin_inventory() -> None:
    inventory = FanIn.from_options(
        {
            "inputs": [
                {
                    "name": "a",
                    "type": "StaticInventory",
                    "options": {"items": [{"n": 0}, {"n": 1}, {"n": 2}, {"n": 3}]},
                },
                {
                    "name": "b",
                    "type": "StaticInventory",
                    "options": {"items": [{"n": 4}, {"n": 5}]},
                },
            ],
            "batch_size": 10,
        },
        services=None,
    )
    messages = await alib.list(inventory.iterate())
    assert {m.args["n"] for m in messages} == set(range(6))
    m = messages[-1]
    assert m.cursor
    assert json.loads(m.cursor) == {"a": "3", "b": "1"}

    messages = await alib.list(inventory.iterate(after=Cursor('{"a": "3", "b": "0"}')))
    assert messages == [
        Item(id=MessageId("1"), cursor='{"a": "3", "b": "1"}', args={"n": 5})
    ]


@pytest.mark.asyncio
async def test_priority_fanin_inventory() -> None:
    inventory = PriorityFanIn.from_options(
        {
            "inputs": [
                {
                    "priority": 1,
                    "inventory": {
                        "name": "a",
                        "type": "StaticInventory",
                        "options": {"items": [{"n": "a"}] * 100},
                    },
                },
                {
                    "priority": 2,
                    "inventory": {
                        "name": "b",
                        "type": "StaticInventory",
                        "options": {"items": [{"n": "b"}] * 100},
                    },
                },
            ],
        },
        services=None,
    )
    messages = (await alib.list(inventory.iterate()))[:75]
    counter = Counter([m.args["n"] for m in messages])
    assert counter["a"] in range(49, 51)
    assert counter["b"] in range(24, 26)

    messages = (
        await alib.list(inventory.iterate(after=Cursor('{"a": "70", "b": "30"}')))
    )[:50]
    counter = Counter([m.args["n"] for m in messages])
    assert counter["a"] == 29
    assert counter["b"] == 21
