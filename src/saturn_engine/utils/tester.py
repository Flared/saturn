import dataclasses
import os
import shutil
import sys
from typing import Any

import click
import yaml

from saturn_engine.core import TopicMessage
from saturn_engine.utils.options import asdict
from saturn_engine.utils.options import fromdict
from saturn_engine.worker.executors.bootstrap import bootstrap_pipeline
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker_manager.config.declarative import (
    load_definitions_from_directory,
)
from saturn_engine.worker_manager.config.declarative import load_definitions_from_str
from saturn_engine.worker_manager.config.declarative_base import BaseObject
from saturn_engine.worker_manager.config.static_definitions import StaticDefinitions


@dataclasses.dataclass
class ExpectedPipelineOutput:
    channel: str
    args: dict[str, Any]


@dataclasses.dataclass
class ExpectedPipelineResource:
    type: str


@dataclasses.dataclass
class PipelineResult:
    outputs: list[ExpectedPipelineOutput]
    resources: list[ExpectedPipelineResource]


@dataclasses.dataclass
class PipelineSelector:
    """
    Only support job_definition for now, but we may support other ways to
    target pipelines in the future.
    """

    job_definition: str


@dataclasses.dataclass
class PipelineTestSpec:
    selector: PipelineSelector
    inventory: list[dict[str, Any]]
    resources: dict[str, dict[str, str]]
    pipeline_results: list[PipelineResult]


@dataclasses.dataclass
class PipelineTest(BaseObject):
    spec: PipelineTestSpec


@click.command()
@click.option(
    "--topology",
    type=click.Path(exists=True, dir_okay=True),
    required=True,
)
@click.option(
    "--tests",
    type=click.Path(exists=True, dir_okay=False),
    required=True,
)
def main(
    *,
    topology: str,
    tests: str,
) -> None:
    # Load the topology
    if os.path.isdir(topology):
        static_definitions = load_definitions_from_directory(topology)
    else:
        with open(topology, "r", encoding="utf-8") as f:
            static_definitions = load_definitions_from_str(f.read())

    # Load and execute the tests
    with open(tests, "r", encoding="utf-8") as f:
        for loaded_yaml_object in yaml.load_all(f.read(), yaml.SafeLoader):
            pipeline_test: PipelineTest = fromdict(loaded_yaml_object, PipelineTest)
            if pipeline_test.kind != "SaturnPipelineTest":
                raise Exception(f"Unknown object kind {pipeline_test.kind}")

            print(f"Running {pipeline_test.metadata.name}...")
            run_saturn_pipeline_test(
                static_definitions=static_definitions,
                pipeline_test=pipeline_test,
            )


def run_saturn_pipeline_test(
    *,
    static_definitions: StaticDefinitions,
    pipeline_test: PipelineTest,
) -> None:
    # Find the pipeline
    job_definition = static_definitions.job_definitions[
        pipeline_test.spec.selector.job_definition
    ]
    pipeline_info = job_definition.template.pipeline.info

    # Execute it.
    pipeline_results: list[dict] = []

    for inventory_item in pipeline_test.spec.inventory:
        pipeline_message = PipelineMessage(
            info=pipeline_info,
            message=TopicMessage(args=inventory_item),
        )
        pipeline_message.update_with_resources(pipeline_test.spec.resources)
        pipeline_result = bootstrap_pipeline(pipeline_message)
        pipeline_results.append(
            asdict(
                PipelineResult(
                    outputs=[
                        ExpectedPipelineOutput(
                            channel=output.channel,
                            args=output.message.args,
                        )
                        for output in pipeline_result.outputs
                    ],
                    resources=[
                        ExpectedPipelineResource(
                            type=resource.type,
                        )
                        for resource in pipeline_result.resources
                    ],
                )
            )
        )

    # Assert the result
    expected_pipeline_results: list[dict] = [
        asdict(r) for r in pipeline_test.spec.pipeline_results
    ]
    if pipeline_results != expected_pipeline_results:
        print_diff(
            expected=expected_pipeline_results,
            got=pipeline_results,
        )
        sys.exit(1)
    else:
        print("Success.")


def print_diff(*, expected: Any, got: Any) -> None:
    """
    Inspired from pyest-icdiff.
    Licensed under the public domain.
    """
    try:
        import icdiff  # type: ignore
        from pprintpp import pformat  # type: ignore
    except ImportError:
        import pprint

        print("expected:", pprint.pformat(expected))
        print("got:", pprint.pformat(got))
        return

    COLS: int = shutil.get_terminal_size().columns
    MARGIN_L: int = 10
    GUTTER: int = 2
    MARGINS: int = MARGIN_L + GUTTER + 1

    half_cols: float = COLS / 2 - MARGINS

    pretty_left = pformat(expected, indent=2, width=half_cols).splitlines()
    pretty_right = pformat(got, indent=2, width=half_cols).splitlines()
    diff_cols = COLS - MARGINS

    if len(pretty_left) < 3 or len(pretty_right) < 3:
        # avoid small diffs far apart by smooshing them up to the left
        smallest_left = pformat(expected, indent=2, width=1).splitlines()
        smallest_right = pformat(got, indent=2, width=1).splitlines()
        max_side = max(len(line) + 1 for line in smallest_left + smallest_right)
        if (max_side * 2 + MARGINS) < COLS:
            diff_cols = max_side * 2 + GUTTER
            pretty_left = pformat(expected, indent=2, width=max_side).splitlines()
            pretty_right = pformat(got, indent=2, width=max_side).splitlines()

    differ = icdiff.ConsoleDiff(cols=diff_cols, tabsize=2)
    color_off = icdiff.color_codes["none"]

    icdiff_lines = list(differ.make_table(pretty_left, pretty_right, context=True))

    print("Expected" + (" " * int(half_cols)) + "Got")

    for line in [color_off + line for line in icdiff_lines]:
        print(line)


if __name__ == "__main__":
    main()
