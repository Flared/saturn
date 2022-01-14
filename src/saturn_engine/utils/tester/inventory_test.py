import asyncio

from saturn_engine.config import default_config_with_env
from saturn_engine.utils.options import asdict
from saturn_engine.worker import work_factory
from saturn_engine.worker.services.manager import ServicesManager
from saturn_engine.worker_manager.config.static_definitions import StaticDefinitions

from .config.inventory_test import InventoryTest
from .diff import print_diff


def run_saturn_inventory_test(
    *,
    static_definitions: StaticDefinitions,
    inventory_test: InventoryTest,
) -> None:

    inventory_item = static_definitions.inventories[
        inventory_test.spec.selector.inventory
    ]

    inventory = work_factory.build_inventory(
        inventory_item=inventory_item,
        services=ServicesManager(
            config=default_config_with_env(),
        ).services,
    )

    items: list[dict] = []

    async def run_inventory() -> None:
        limit = inventory_test.spec.limit
        count = 0
        async for item in inventory.iterate(
            after=inventory_test.spec.after,
        ):
            items.append(asdict(item))
            count = count + 1
            if limit and count >= limit:
                break

    asyncio.run(run_inventory())

    expected_items: list[dict] = [asdict(item) for item in inventory_test.spec.items]

    if items != expected_items:
        print_diff(
            expected=expected_items,
            got=items,
        )
        raise AssertionError("Inventory items do not match the expected items")
    else:
        print("Success.")
