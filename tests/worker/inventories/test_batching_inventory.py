import json

import asyncstdlib as alib
import pytest

from saturn_engine.core import Cursor
from saturn_engine.worker.inventories.batching import BatchingInventory


@pytest.mark.asyncio
async def test_batching_inventory() -> None:
    inventory = BatchingInventory.from_options(
        {
            "inventory": {
                "name": "a",
                "type": "StaticInventory",
                "options": {"items": [{"a": str(i)} for i in range(10)]},
            },
            "batch_size": 3,
        },
        services=None,
    )
    items = await alib.list(inventory.run())

    assert [(i.id, i.args) for i in items] == [
        ("2", {"batch": [{"a": "0"}, {"a": "1"}, {"a": "2"}]}),
        ("5", {"batch": [{"a": "3"}, {"a": "4"}, {"a": "5"}]}),
        ("8", {"batch": [{"a": "6"}, {"a": "7"}, {"a": "8"}]}),
        (
            "9",
            {
                "batch": [
                    {"a": "9"},
                ]
            },
        ),
    ]

    async with items[1]:
        pass

    assert (c := inventory.cursor)
    assert json.loads(c) == {"v": 1, "p": ["3", "4", "5"]}

    async with items[0]:
        pass

    assert (c := inventory.cursor)
    assert json.loads(c) == {"v": 1, "a": "5"}

    items = await alib.list(inventory.run(after=Cursor("4")))

    assert [(i.id, i.args) for i in items] == [
        ("7", {"batch": [{"a": "5"}, {"a": "6"}, {"a": "7"}]}),
        ("9", {"batch": [{"a": "8"}, {"a": "9"}]}),
    ]
