from flask.testing import FlaskClient

from saturn_engine.worker_manager.config.declarative import Inventory
from saturn_engine.worker_manager.config.declarative import InventorySpec
from saturn_engine.worker_manager.config.declarative import ObjectMetadata
from saturn_engine.worker_manager.config.declarative import StaticDefinitions


def test_api_inventories_empty(client: FlaskClient) -> None:
    resp = client.get("/api/inventories")
    assert resp.status_code == 200
    assert resp.json == {"inventories": []}


def test_api_inventories_loaded_from_file(
    client: FlaskClient,
    static_definitions: StaticDefinitions,
) -> None:

    static_definitions.inventories = [
        Inventory(
            apiVersion="aa",
            kind="SaturnInventory",
            metadata=ObjectMetadata(
                name="testinv",
            ),
            spec=InventorySpec(
                type="testtype",
                options=dict(),
            ),
        )
    ]

    resp = client.get("/api/inventories")
    assert resp.status_code == 200
    assert resp.json == {
        "inventories": [
            {
                "name": "testinv",
                "type": "testtype",
                "options": {},
            },
        ]
    }
