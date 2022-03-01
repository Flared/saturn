from typing import DefaultDict
from typing import Optional

import dataclasses
import pprint
import sys
from collections import defaultdict

import click

from saturn_engine.utils.declarative_config import UncompiledObject
from saturn_engine.utils.declarative_config import load_uncompiled_objects_from_path
from saturn_engine.utils.options import fromdict
from saturn_engine.utils.tester.config.topic_test import TopicTest
from saturn_engine.utils.tester.topic_test import run_saturn_topic
from saturn_engine.utils.tester.topic_test import run_saturn_topic_test
from saturn_engine.worker_manager.config.declarative import compile_static_definitions
from saturn_engine.worker_manager.config.static_definitions import StaticDefinitions

from .config.inventory_test import InventoryTest
from .config.pipeline_test import PipelineTest
from .inventory_test import run_saturn_inventory
from .inventory_test import run_saturn_inventory_test
from .pipeline_test import run_saturn_pipeline_test


@dataclasses.dataclass
class SaturnTests:
    pipeline_tests: dict[str, PipelineTest] = dataclasses.field(default_factory=dict)
    inventory_tests: dict[str, InventoryTest] = dataclasses.field(default_factory=dict)
    topic_tests: dict[str, TopicTest] = dataclasses.field(default_factory=dict)


def compile_tests(uncompiled_objects: list) -> SaturnTests:
    objects_by_kind: DefaultDict[str, dict[str, UncompiledObject]] = defaultdict(dict)
    for uncompiled_object in uncompiled_objects:
        if uncompiled_object.name in objects_by_kind[uncompiled_object.kind]:
            raise Exception(
                f"{uncompiled_object.kind}/{uncompiled_object.name} already exists"
            )
        objects_by_kind[uncompiled_object.kind][
            uncompiled_object.name
        ] = uncompiled_object

    tests: SaturnTests = SaturnTests()

    for uncompiled_pipeline_test in objects_by_kind.pop(
        "SaturnPipelineTest",
        dict(),
    ).values():
        pipeline_test: PipelineTest = fromdict(
            uncompiled_pipeline_test.data, PipelineTest
        )
        tests.pipeline_tests[pipeline_test.metadata.name] = pipeline_test

    for uncompiled_inventory_test in objects_by_kind.pop(
        "SaturnInventoryTest",
        dict(),
    ).values():
        inventory_test: InventoryTest = fromdict(
            uncompiled_inventory_test.data, InventoryTest
        )
        tests.inventory_tests[inventory_test.metadata.name] = inventory_test

    for uncompiled_topic_test in objects_by_kind.pop(
        "SaturnTopicTest",
        dict(),
    ).values():
        topic_test: TopicTest = fromdict(uncompiled_topic_test.data, TopicTest)
        tests.topic_tests[topic_test.metadata.name] = topic_test

    for object_kind in objects_by_kind.keys():
        raise Exception(f"Unsupported kind {object_kind}")

    return tests


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

    for topic_test in tests.topic_tests.values():
        print(f"Running {topic_test.metadata.name}...")
        run_saturn_topic_test(
            static_definitions=static_definitions,
            topic_test=topic_test,
        )


@click.group()
def cli() -> None:
    pass


@cli.command()
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
def run(
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


@cli.command()
@click.option(
    "--topology",
    type=click.Path(exists=True, dir_okay=True),
    required=True,
)
@click.option("--name", type=str, required=True)
@click.option("--limit", type=int, required=True, default=1)
@click.option("--after", type=str, required=False)
def show_inventory(topology: str, name: str, limit: int, after: Optional[str]) -> None:
    static_definitions = compile_static_definitions(
        load_uncompiled_objects_from_path(topology),
    )
    for item in run_saturn_inventory(
        static_definitions=static_definitions,
        inventory_name=name,
        limit=limit,
        after=after,
    ):
        pprint.pprint(item)


@cli.command()
@click.option(
    "--topology",
    type=click.Path(exists=True, dir_okay=True),
    required=True,
)
@click.option("--name", type=str, required=True)
@click.option("--limit", type=int, required=True, default=1)
@click.option("--skip", type=int, required=True, default=0)
def show_topic(topology: str, name: str, limit: int, skip: int) -> None:
    static_definitions = compile_static_definitions(
        load_uncompiled_objects_from_path(topology),
    )
    for message in run_saturn_topic(
        static_definitions=static_definitions,
        topic_name=name,
        limit=limit,
        skip=skip,
    ):
        pprint.pprint(message)


if __name__ == "__main__":
    cli()
