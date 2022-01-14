import dataclasses
import shutil
import sys
from collections import defaultdict
from typing import Any
from typing import DefaultDict

import click

from saturn_engine.core import TopicMessage
from saturn_engine.utils.declarative_config import UncompiledObject
from saturn_engine.utils.declarative_config import load_uncompiled_objects_from_path
from saturn_engine.utils.options import asdict
from saturn_engine.utils.options import fromdict
from saturn_engine.worker.executors.bootstrap import bootstrap_pipeline
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker_manager.config.declarative import compile_static_definitions
from saturn_engine.worker_manager.config.static_definitions import StaticDefinitions

from .config.pipeline_test import ExpectedPipelineOutput
from .config.pipeline_test import ExpectedPipelineResource
from .config.pipeline_test import PipelineResult
from .config.pipeline_test import PipelineTest


@dataclasses.dataclass
class SaturnTests:
    pipeline_tests: dict[str, PipelineTest] = dataclasses.field(default_factory=dict)


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
    compiled_static_definitions = compile_static_definitions(
        load_uncompiled_objects_from_path(topology),
    )
    compiled_tests = compile_tests(
        load_uncompiled_objects_from_path(tests),
    )
    try:
        run_tests(
            static_definitions=compiled_static_definitions,
            tests=compiled_tests,
        )
    except AssertionError:
        sys.exit(1)


def run_tests(*, static_definitions: StaticDefinitions, tests: SaturnTests) -> None:
    for pipeline_test in tests.pipeline_tests.values():
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
        raise AssertionError("Pipeline results do not match the expected output")
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
