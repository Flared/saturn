from saturn_engine.utils.declarative_config import ObjectMetadata
from saturn_engine.worker_manager.config.declarative_inventory import Inventory
from saturn_engine.worker_manager.config.declarative_inventory import InventorySpec
from saturn_engine.worker_manager.config.static_definitions import StaticDefinitions


def build(definitions: StaticDefinitions) -> None:
    definitions.inventories["test-inventory"] = Inventory(
        metadata=ObjectMetadata(name="test-inventory"),
        apiVersion="saturn.flared.io/v1alpha1",
        kind="SaturnInventory",
        spec=InventorySpec(type="testtype"),
    ).to_core_object()
