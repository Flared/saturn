from typing import Any

from pydantic import dataclasses

from saturn_engine.utils.declarative_config import BaseObject


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
