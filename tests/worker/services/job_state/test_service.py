import typing as t

import asyncio
import dataclasses
from unittest.mock import call

import asyncstdlib as alib
import pytest
from pytest_mock import MockerFixture

from saturn_engine.core import Cursor
from saturn_engine.core import JobId
from saturn_engine.core.api import QueueItemWithState
from saturn_engine.core.job_state import CursorStateUpdated
from saturn_engine.core.pipeline import PipelineInfo
from saturn_engine.core.topic import TopicMessage
from saturn_engine.utils import utcnow
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.inventory import Inventory
from saturn_engine.worker.inventory import Item
from saturn_engine.worker.job import Job
from saturn_engine.worker.services.hooks import PipelineEventsEmitted
from saturn_engine.worker.services.job_state.service import CursorState
from saturn_engine.worker.services.job_state.service import JobStateService
from saturn_engine.worker.services.manager import ServicesManager
from tests.conftest import FreezeTime
from tests.utils import EqualAnyOrder
from tests.utils import HttpClientMock


@pytest.fixture
async def job_state_service(
    services_manager: ServicesManager,
    fake_http_client_service: t.Any,
) -> JobStateService:
    return await services_manager._reload_service(JobStateService)


@pytest.mark.asyncio
async def test_job_state_update(
    http_client_mock: HttpClientMock,
    frozen_time: FreezeTime,
    job_state_service: JobStateService,
) -> None:
    http_client_mock.put("http://127.0.0.1:5000/api/jobs/_states").return_value = {}
    job_state_service.set_job_cursor(job_name=JobId("job-1"), cursor=Cursor("1"))
    job_state_service.set_job_cursor(job_name=JobId("job-2"), cursor=Cursor("2"))
    job_state_service.set_job_failed(job_name=JobId("job-2"), error=ValueError("test"))
    job_state_service.set_job_cursor(job_name=JobId("job-1"), cursor=Cursor("3"))
    job_state_service.set_job_completed(job_name=JobId("job-3"))

    job_state_service.set_job_cursor_state(
        job_name=JobId("job-3"), cursor_state={"x": "1"}, cursor=Cursor("a")
    )
    job_state_service.set_job_cursor_state(
        job_name=JobId("job-3"), cursor_state={"x": "2"}, cursor=Cursor("b")
    )
    job_state_service.set_job_cursor_state(
        job_name=JobId("job-3"), cursor_state={"x": "3"}, cursor=Cursor("c")
    )
    await job_state_service.flush()
    http_client_mock.put(
        "http://127.0.0.1:5000/api/jobs/_states"
    ).assert_called_once_with(
        json={
            "state": {
                "jobs": {
                    "job-1": {"completion": None, "cursors_states": {}, "cursor": "3"},
                    "job-2": {
                        "completion": {
                            "error": "ValueError: test",
                            "completed_at": utcnow(),
                        },
                        "cursors_states": {},
                        "cursor": "2",
                    },
                    "job-3": {
                        "completion": {
                            "error": None,
                            "completed_at": utcnow(),
                        },
                        "cursors_states": {
                            "a": {"x": "1"},
                            "b": {"x": "2"},
                            "c": {"x": "3"},
                        },
                        "cursor": None,
                    },
                }
            }
        }
    )

    http_client_mock.reset_mock()
    job_state_service.set_job_cursor(job_name=JobId("job-1"), cursor=Cursor("10"))
    await job_state_service.flush()
    http_client_mock.put(
        "http://127.0.0.1:5000/api/jobs/_states"
    ).assert_called_once_with(
        json={
            "state": {
                "jobs": {
                    "job-1": {
                        "completion": None,
                        "cursor": "10",
                        "cursors_states": {},
                    },
                }
            }
        }
    )

    http_client_mock.reset_mock()
    await job_state_service.flush()
    http_client_mock.put("http://127.0.0.1:5000/api/jobs/_states").assert_not_called()


async def test_job_state_fetch_cursors(
    http_client_mock: HttpClientMock,
    job_state_service: JobStateService,
) -> None:
    http_client_mock.post(
        "http://127.0.0.1:5000/api/jobs/_states/fetch"
    ).return_value = {
        "cursors": {
            "job-1": {
                "a": {"x": 1},
                "b": {"x": 2},
            },
            "job-2": {
                "c": None,
            },
        }
    }

    fetch_1 = job_state_service.fetch_cursors_states(
        JobId("job-1"), cursors=[Cursor("a"), Cursor("b")]
    )
    fetch_2 = job_state_service.fetch_cursors_states(
        JobId("job-2"), cursors=[Cursor("c")]
    )
    states_1, states_2 = await asyncio.gather(fetch_1, fetch_2)

    assert states_1 == {"a": {"x": 1}, "b": {"x": 2}}
    assert states_2 == {"c": None}

    http_client_mock.post(
        "http://127.0.0.1:5000/api/jobs/_states/fetch"
    ).assert_called_once_with(
        json={
            "cursors": {
                "job-1": EqualAnyOrder(["a", "b"]),
                "job-2": ["c"],
            }
        }
    )


@dataclasses.dataclass
class SimpleCursorState(CursorState):
    x: str


def pipeline(state: t.Optional[SimpleCursorState] = None) -> t.Optional[str]:
    if state:
        return state.x
    return None


class FakeInventory(Inventory):
    name = "fake_inventory"

    @dataclasses.dataclass
    class Options:
        data: list[t.Union[int, tuple[int, dict]]]

    def __init__(self, *args: t.Any, options: Options, **kwargs: t.Any) -> None:
        self.options = options

    async def next_batch(self, after: t.Optional[Cursor] = None) -> list[Item]:
        raise NotImplementedError()

    async def iterate(self, after: t.Optional[Cursor] = None) -> t.AsyncIterator[Item]:
        for i, data in enumerate(self.options.data):
            if isinstance(data, tuple):
                x, metadata = data
            else:
                x = data
                metadata = {}

            await asyncio.sleep(x)
            yield Item(id=str(i), args={"x": x}, metadata=metadata)


async def test_job_state_set_message_cursor_state(
    services_manager: ServicesManager,
    fake_queue_item: QueueItemWithState,
    executable_maker: t.Callable[..., ExecutableMessage],
    inmemory_cursors_fetcher: None,
    mocker: MockerFixture,
) -> None:
    job_state_service = services_manager.services.cast_service(JobStateService)
    fetch_cursors = mocker.spy(job_state_service, "fetch_cursors_states")
    job_state_service.set_job_cursor_state(
        fake_queue_item.name, cursor=Cursor("1"), cursor_state={"x": 1}
    )
    job_state_service.set_job_cursor_state(
        fake_queue_item.name, cursor=Cursor("2"), cursor_state={"x": 2}
    )
    fake_queue_item.config["job"] = {
        "buffer_flush_after": 7,
        "buffer_size": 2,
    }
    fake_queue_item.config["job_state"] = {"cursors_states_enabled": True}

    @services_manager.services.s.hooks.work_queue_built.emit
    async def fake_work_builder(queue: QueueItemWithState) -> t.Any:
        pass

    await fake_work_builder(fake_queue_item)
    assert fake_queue_item.config["job"]["batching_enabled"]

    inventory = FakeInventory(
        options=FakeInventory.Options(
            data=[5, 4, 3, 2, (1, {"job_state": {"state_cursor": "a"}})]
        )
    )
    job = Job(
        inventory=inventory,
        queue_item=fake_queue_item,
        services=services_manager.services,
    )

    # Run the job manually
    msgs: list[TopicMessage] = await alib.list(job.run())  # type: ignore[arg-type]

    # Assert everything loaded and ran in order.
    assert [i.id for i in msgs] == ["0", "1", "2", "3", "4"]
    assert [i.metadata.get("job_state", {}).get("cursor_state") for i in msgs] == [
        None,
        {"x": 1},
        {"x": 2},
        None,
        None,
    ]

    assert fetch_cursors.call_args_list == [
        call(fake_queue_item.name, cursors=[Cursor("0")]),
        call(fake_queue_item.name, cursors=[Cursor("1"), Cursor("2")]),
        call(fake_queue_item.name, cursors=[Cursor("3"), Cursor("a")]),
    ]

    with job_state_service._store.flush() as state:
        assert len(state.jobs) == 1
        job_state = state.jobs[fake_queue_item.name]
        assert job_state.cursor == "4"
        assert job_state.completion

    results = []
    for i, msg in enumerate(msgs):
        xmsg = executable_maker(
            message=msg,
            pipeline_info=PipelineInfo.from_pipeline(pipeline),
        )
        await services_manager.services.s.hooks.message_polled.emit(xmsg)
        results.append(xmsg.message.execute())

        await services_manager.services.s.hooks.pipeline_events_emitted.emit(
            PipelineEventsEmitted(
                xmsg=xmsg, events=[CursorStateUpdated(state={"x": i * 10})]
            )
        )
    assert results == [None, "1", "2", None, None]

    # Rerun the inventory, expect new states to be loaded
    inventory = FakeInventory(
        options=FakeInventory.Options(
            data=[5, 4, 3, 2, (1, {"job_state": {"state_cursor": "a"}})]
        )
    )
    job = Job(
        inventory=inventory,
        queue_item=fake_queue_item,
        services=services_manager.services,
    )
    msgs = await alib.list(job.run())  # type: ignore[arg-type]

    # Assert everything loaded and ran in order.
    assert [i.metadata.get("job_state", {}).get("cursor_state") for i in msgs] == [
        {"x": 0},
        {"x": 10},
        {"x": 20},
        {"x": 30},
        {"x": 40},
    ]
