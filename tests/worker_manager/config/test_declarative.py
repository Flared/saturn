import os

from saturn_engine.worker_manager.config.declarative import (
    load_definitions_from_directory,
)


def test_load_definitions_from_directory_simple() -> None:
    test_dir = os.path.join(
        os.path.dirname(__file__),
        "testdata",
        "test_declarative_simple",
    )
    static_definitions = load_definitions_from_directory(test_dir)
    assert static_definitions.inventories[0].metadata.name == "github-identifiers"
