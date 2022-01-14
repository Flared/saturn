from saturn_engine.core import TopicMessage
from saturn_engine.utils.options import asdict
from saturn_engine.worker.executors.bootstrap import bootstrap_pipeline
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
        diff: str = get_diff(
            expected=expected_pipeline_results,
            got=pipeline_results,
        )
        raise AssertionError(
            f"Pipeline results do not match the expected output:\n{diff}"
        )
