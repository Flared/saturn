import sys

from saturn_engine.core import TopicMessage
from saturn_engine.utils.hooks import EventHook
from saturn_engine.utils.options import asdict
from saturn_engine.utils.tester.json_utils import find_nodes
from saturn_engine.utils.tester.json_utils import get_node_value
from saturn_engine.utils.tester.json_utils import normalize_json
from saturn_engine.utils.tester.json_utils import replace_node
from saturn_engine.worker.error_handling import HandledError
from saturn_engine.worker.error_handling import process_pipeline_exception
from saturn_engine.worker.executors.bootstrap import PipelineBootstrap
from saturn_engine.worker.pipeline_message import PipelineMessage
from saturn_engine.worker_manager.config.static_definitions import StaticDefinitions

from .config.pipeline_test import ExpectedPipelineOutput
from .config.pipeline_test import ExpectedPipelineResource
from .config.pipeline_test import PipelineResult
from .config.pipeline_test import PipelineTest
from .diff import get_diff


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
    bootstraper = PipelineBootstrap(EventHook())

    for inventory_item in pipeline_test.spec.inventory:
        pipeline_message = PipelineMessage(
            info=pipeline_info,
            message=TopicMessage(args=inventory_item),
        )
        pipeline_message.update_with_resources(pipeline_test.spec.resources)

        # Run pipeline with exception handling
        pipeline_result = None
        try:
            pipeline_result = bootstraper.bootstrap_pipeline(pipeline_message)
        except Exception:
            exc_type, exc_value, exc_traceback = sys.exc_info()

            # Shouldn't be None but we want to make mypy happy
            assert exc_type and exc_value and exc_traceback  # noqa: S101

            try:
                process_pipeline_exception(
                    queue=job_definition.template,
                    message=pipeline_message.message,
                    exc_type=exc_type,
                    exc_value=exc_value,
                    exc_traceback=exc_traceback,
                )
            except HandledError as e:
                pipeline_result = e.results
            else:
                raise

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

    pipeline_results = normalize_json(pipeline_results)

    # Handle custom wildcard(__any__) fields in order to pass the assertion
    for i, expected_pipeline_result in enumerate(expected_pipeline_results):
        paths = find_nodes(expected_pipeline_result, "__any__", [])
        for path in paths:
            replace_node(
                expected_pipeline_result,
                path,
                get_node_value(pipeline_results[i], path),
            )

    if pipeline_results != expected_pipeline_results:
        diff: str = get_diff(
            expected=expected_pipeline_results,
            got=pipeline_results,
        )
        raise AssertionError(
            f"Pipeline results do not match the expected output:\n{diff}"
        )
