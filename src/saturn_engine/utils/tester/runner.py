from typing import DefaultDict

import dataclasses
import sys
from collections import defaultdict

import click

from saturn_engine.utils.declarative_config import UncompiledObject
from saturn_engine.utils.declarative_config import load_uncompiled_objects_from_path
from saturn_engine.utils.options import fromdict
from saturn_engine.worker_manager.config.declarative import compile_static_definitions
from saturn_engine.worker_manager.config.static_definitions import StaticDefinitions

from .config.inventory_test import InventoryTest
from .config.pipeline_test import PipelineTest
from .inventory_test import run_saturn_inventory_test
from .pipeline_test import run_saturn_pipeline_test


@dataclasses.dataclass
class SaturnTests:
    pipeline_tests: dict[str, PipelineTest] = dataclasses.field(default_factory=dict)
    inventory_tests: dict[str, InventoryTest] = dataclasses.field(default_factory=dict)


def compile_tests(uncompiled_objects: list) -> SaturnTests:

    objects_by_kind: DefaultDict[str, list[UncompiledObject]] = defaultdict(list)
    for uncompiled_object in uncompiled_objects:
        objects_by_kind[uncompiled_object.kind].append(uncompiled_object)

    tests: SaturnTests = SaturnTests()

    for uncompiled_pipeline_test in objects_by_kind.pop("SaturnPipelineTest", list()):
        pipeline_test: PipelineTest = fromdict(
            uncompiled_pipeline_test.data, PipelineTest
        )
        tests.pipeline_tests[pipeline_test.metadata.name] = pipeline_test

    for uncompiled_inventory_test in objects_by_kind.pop("SaturnInventoryTest", list()):
        inventory_test: InventoryTest = fromdict(
            uncompiled_inventory_test.data, InventoryTest
        )
        tests.inventory_tests[inventory_test.metadata.name] = inventory_test

    for object_kind in objects_by_kind.keys():
        raise Exception(f"Unsupported kind {object_kind}")

    return tests


@click.command()
@click.option(
    "--topology",
    type=click.Path(exists=True, dir_okay=True),
    required=True,
)
@click.option(
    "--tests",
    type=click.Path(exists=True, dir_okay=True),
    required=True,
)
def main(
    *,
    topology: str,
    tests: str,
) -> None:
    try:
        run_tests_from_files(
            static_definitions=topology,
            tests=tests,
        )
    except AssertionError as e:
        print(e)
        sys.exit(1)


def run_tests_from_files(
    *,
    static_definitions: str,
    tests: str,
) -> None:
    compiled_static_definitions = compile_static_definitions(
        load_uncompiled_objects_from_path(static_definitions),
    )
    compiled_tests = compile_tests(
        load_uncompiled_objects_from_path(tests),
    )
    run_tests(
        static_definitions=compiled_static_definitions,
        tests=compiled_tests,
    )


def run_tests(*, static_definitions: StaticDefinitions, tests: SaturnTests) -> None:
    for pipeline_test in tests.pipeline_tests.values():
        print(f"Running {pipeline_test.metadata.name}...")
        run_saturn_pipeline_test(
            static_definitions=static_definitions,
            pipeline_test=pipeline_test,
        )
    for inventory_test in tests.inventory_tests.values():
        print(f"Running {inventory_test.metadata.name}...")
        run_saturn_inventory_test(
            static_definitions=static_definitions,
            inventory_test=inventory_test,
        )


if __name__ == "__main__":
    main()
