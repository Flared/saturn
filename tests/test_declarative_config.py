import os

from saturn_engine.utils.declarative_config import (
    load_uncompiled_objects_from_directory,
)


def test_load_uncompiled_objects_from_directory_subdir() -> None:
    objects = load_uncompiled_objects_from_directory(
        os.path.join(
            os.path.dirname(__file__),
            "testdata",
            "test_declarative_config_subdir",
        )
    )
    assert len(objects) == 2
