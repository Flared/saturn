import typing as t

import asyncio
import dataclasses
from unittest.mock import call

import pytest
from pytest_mock import MockerFixture

from saturn_engine.config import Config
from saturn_engine.core import Cursor
from saturn_engine.core import JobId
from saturn_engine.core.api import QueueItemWithState
from saturn_engine.core.job_state import CursorStateUpdated
from saturn_engine.core.pipeline import PipelineInfo
from saturn_engine.utils import utcnow
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.executors.executable import ExecutableQueue
from saturn_engine.worker.inventory import Inventory
from saturn_engine.worker.inventory import Item
from saturn_engine.worker.job import Job
from saturn_engine.worker.services.hooks import PipelineEventsEmitted
from saturn_engine.worker.services.job_state.service import CursorFormat
from saturn_engine.worker.services.job_state.service import CursorState
from saturn_engine.worker.services.job_state.service import JobStateService
from saturn_engine.worker.services.manager import ServicesManager
from tests.conftest import FreezeTime
from tests.utils import EqualAnyOrder
from tests.utils import HttpClientMock
from tests.worker.conftest import InMemoryCursorsFetcher

H = {
    "a": "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb",
    "b": "3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d",
    "c": "2e7d2c03a9507ae265ecf5b5356885a53393a2029d241394997265a1a25aefc6",
}


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
                H["a"]: {"x": 1},
                H["b"]: {"x": 2},
            },
            "job-2": {
                H["c"]: None,
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
                "job-1": EqualAnyOrder([H["a"], H["b"]]),
                "job-2": [H["c"]],
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
    executable_queue_maker: t.Callable[..., ExecutableQueue],
    inmemory_cursors_fetcher: InMemoryCursorsFetcher,
    mocker: MockerFixture,
) -> None:
    job_state_service = services_manager.services.cast_service(JobStateService)
    fetch_cursors = mocker.spy(job_state_service, "fetch_cursors_states")
    job_state_service.set_job_cursor_state(
        JobId("test-job"), cursor=Cursor("0"), cursor_state={"x": 0}
    )
    job_state_service._store.flush().__enter__()
    job_state_service.set_job_cursor_state(
        JobId("test-job"), cursor=Cursor("1"), cursor_state={"x": 1}
    )
    inmemory_cursors_fetcher.set_states({JobId("test-job"): {Cursor("2"): {"x": 2}}})
    fake_queue_item.config["job"] = {
        "buffer_flush_after": 7,
        "buffer_size": 2,
    }
    fake_queue_item.pipeline.info = PipelineInfo.from_pipeline(pipeline)
    fake_queue_item.config["job_state"] = {
        "cursors_states_enabled": True,
        "cursors_states_namespace": "test-job",
    }

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
    xqueue = executable_queue_maker(definition=fake_queue_item, topic=job)

    # Run the job manually
    results = []
    xmsgs: list[ExecutableMessage] = []
    async for xmsg in xqueue.run():
        async with xmsg._context:
            xmsgs.append(xmsg)
            results.append(xmsg.message.execute())
            await services_manager.services.s.hooks.pipeline_events_emitted.emit(
                PipelineEventsEmitted(
                    xmsg=xmsg, events=[CursorStateUpdated(state={"x": len(xmsgs) * 10})]
                )
            )

    # Assert everything loaded and ran in order.
    msgs = [i.message.message for i in xmsgs]
    assert [i.id for i in msgs] == ["0", "1", "2", "3", "4"]
    assert [i.metadata.get("job_state", {}).get("cursor_state") for i in msgs] == [
        {"x": 0},
        {"x": 1},
        {"x": 2},
        None,
        None,
    ]

    assert fetch_cursors.call_args_list == [
        call("test-job", cursors=[Cursor("0")]),
        call("test-job", cursors=[Cursor("1"), Cursor("2")]),
        call("test-job", cursors=[Cursor("3"), Cursor("a")]),
    ]

    state = job_state_service._store._current_state
    assert len(state.jobs) == 2
    job_state = state.jobs[fake_queue_item.name]
    assert job_state.cursor == '{"v": 1, "a": "4"}'
    assert job_state.completion

    assert results == ["0", "1", "2", None, None]

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
    xqueue = executable_queue_maker(definition=fake_queue_item, topic=job)

    xmsgs.clear()
    async for xmsg in xqueue.run():
        async with xmsg._context:
            xmsgs.append(xmsg)
    msgs = [i.message.message for i in xmsgs]

    # Assert everything loaded and ran in order.
    assert [i.metadata.get("job_state", {}).get("cursor_state") for i in msgs] == [
        {"x": 10},
        {"x": 20},
        {"x": 30},
        {"x": 40},
        {"x": 50},
    ]


@pytest.fixture
async def multiformat_job_state_service(
    services_manager_maker: t.Callable[[Config], t.Awaitable[ServicesManager]],
    fake_http_client_service_maker: t.Callable,
    config: Config,
) -> JobStateService:
    config = config.load_object(
        {"job_state": {"fetch_cursor_formats": [CursorFormat.RAW, CursorFormat.SHA256]}}
    )
    services_manager = await services_manager_maker(config)
    await fake_http_client_service_maker(services_manager=services_manager)
    return await services_manager._reload_service(JobStateService)


async def test_fetch_multiple_format(
    multiformat_job_state_service: JobStateService,
    http_client_mock: HttpClientMock,
) -> None:
    http_client_mock.post(
        "http://127.0.0.1:5000/api/jobs/_states/fetch"
    ).return_value = {
        "cursors": {
            "job-1": {
                "a": {"x": 1},
                H["b"]: {"x": 2},
            },
            "job-2": {
                "c": {"x": 3},
                H["c"]: {"x": 4},
            },
        }
    }
    fetch_1 = multiformat_job_state_service.fetch_cursors_states(
        JobId("job-1"), cursors=[Cursor("a"), Cursor("b")]
    )
    fetch_2 = multiformat_job_state_service.fetch_cursors_states(
        JobId("job-2"), cursors=[Cursor("c")]
    )
    states_1, states_2 = await asyncio.gather(fetch_1, fetch_2)

    assert states_1 == {"a": {"x": 1}, "b": {"x": 2}}
    assert states_2 == {"c": {"x": 4}}

    http_client_mock.post(
        "http://127.0.0.1:5000/api/jobs/_states/fetch"
    ).assert_called_once_with(
        json={
            "cursors": {
                "job-1": EqualAnyOrder(["a", "b", H["a"], H["b"]]),
                "job-2": ["c", H["c"]],
            }
        }
    )
