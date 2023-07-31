import typing as t

import dataclasses
from collections.abc import AsyncGenerator
from collections.abc import Generator
from unittest import mock

import pytest

from saturn_engine.core.api import QueueItemWithState
from saturn_engine.core.pipeline import PipelineOutput
from saturn_engine.core.pipeline import PipelineResults
from saturn_engine.core.topic import TopicMessage
from saturn_engine.utils.hooks import AsyncContextHook
from saturn_engine.utils.hooks import AsyncEventHook
from saturn_engine.utils.hooks import ContextHook
from saturn_engine.utils.hooks import EventHook
from saturn_engine.worker.executors.executable import ExecutableMessage
from saturn_engine.worker.executors.executable import ExecutableQueue
from saturn_engine.worker.services.hooks import Hooks
from saturn_engine.worker.services.hooks import ItemsBatch
from saturn_engine.worker.services.hooks import MessagePublished
from saturn_engine.worker.services.hooks import PipelineEventsEmitted
from saturn_engine.worker.services.hooks import ResultsProcessed


def test_event_hook() -> None:
    error_handler = mock.Mock()
    hook: EventHook[str] = EventHook(error_handler=error_handler)

    handler1 = mock.Mock()
    hook.register(handler1)

    handler2 = mock.Mock()
    hook.register(handler2)

    hook.emit("a")
    handler1.assert_called_once_with("a")
    handler2.assert_called_once_with("a")

    handler1.reset_mock()
    handler2.reset_mock()

    error = ValueError()
    handler1.side_effect = error
    hook.emit("d")
    error_handler.assert_called_once_with(error)
    handler2.assert_called_once_with("d")


@pytest.mark.asyncio
async def test_async_event_hook() -> None:
    error_handler = mock.AsyncMock()
    hook: AsyncEventHook[str] = AsyncEventHook(error_handler=error_handler)

    handler1 = mock.AsyncMock()
    hook.register(handler1)

    handler2 = mock.AsyncMock()
    hook.register(handler2)

    await hook.emit("a")
    handler1.assert_awaited_once_with("a")
    handler2.assert_awaited_once_with("a")

    handler1.reset_mock()
    handler2.reset_mock()

    error = ValueError()
    handler1.side_effect = error
    await hook.emit("d")
    error_handler.assert_awaited_once_with(error)
    handler2.assert_awaited_once_with("d")


def test_context_hook() -> None:
    error_handler = mock.Mock()
    m = mock.Mock()
    hook: ContextHook[str, str] = ContextHook(error_handler=error_handler)

    @hook.register
    def handler(a: str) -> Generator[None, str, None]:
        m(handler, "before")
        try:
            r = yield
            m(handler, "after", r)
        except Exception as e:
            m(handler, "error", e)

    @hook.register
    def handler_before(a: str) -> None:
        m(handler_before, "before")

    @hook.register
    def handler_after(a: str) -> Generator[None, str, None]:
        m(handler_after, "before")
        yield
        m(handler_after, "after")

    @hook.register
    def handler_noyield(a: str) -> Generator[None, str, None]:
        m(handler_noyield, "before")
        if False:
            yield

    @hook.emit
    def scope(a: str) -> str:
        m("in", a)
        return "c"

    assert scope("1") == "c"

    m.assert_has_calls(
        [
            mock.call(handler, "before"),
            mock.call(handler_before, "before"),
            mock.call(handler_after, "before"),
            mock.call(handler_noyield, "before"),
            mock.call("in", "1"),
            mock.call(handler_after, "after"),
            mock.call(handler, "after", "c"),
        ]
    )

    m.reset_mock()
    e = ValueError()

    with pytest.raises(ValueError):

        @hook.emit
        def scope_raising(a: str) -> str:
            m("in", a)
            raise e

        scope_raising("1")

    m.assert_has_calls(
        [
            mock.call(handler, "before"),
            mock.call(handler_before, "before"),
            mock.call(handler_after, "before"),
            mock.call(handler_noyield, "before"),
            mock.call("in", "1"),
            mock.call(handler, "error", e),
        ]
    )

    error_handler.assert_not_called()


@pytest.mark.asyncio
async def test_async_context_hook() -> None:
    error_handler = mock.AsyncMock()
    m = mock.Mock()
    hook: AsyncContextHook[str, str] = AsyncContextHook(error_handler=error_handler)

    @hook.register
    async def handler(a: str) -> AsyncGenerator[None, str]:
        m(handler, "before")
        try:
            r = yield
            m(handler, "after", r)
        except Exception as e:
            m(handler, "error", e)

    @hook.register
    async def handler_before(a: str) -> None:
        m(handler_before, "before")

    @hook.register
    async def handler_after(a: str) -> AsyncGenerator[None, str]:
        m(handler_after, "before")
        yield
        m(handler_after, "after")

    @hook.register
    async def handler_noyield(a: str) -> AsyncGenerator[None, str]:
        m(handler_noyield, "before")
        if False:
            yield

    @hook.emit
    async def scope(a: str) -> str:
        m("in", a)
        return "c"

    assert await scope("1") == "c"

    m.assert_has_calls(
        [
            mock.call(handler, "before"),
            mock.call(handler_before, "before"),
            mock.call(handler_after, "before"),
            mock.call(handler_noyield, "before"),
            mock.call("in", "1"),
            mock.call(handler_after, "after"),
            mock.call(handler, "after", "c"),
        ]
    )

    m.reset_mock()
    e = ValueError()

    with pytest.raises(ValueError):

        @hook.emit
        async def scope_raising(a: str) -> str:
            m("in", a)
            raise e

        await scope_raising("1")

    m.assert_has_calls(
        [
            mock.call(handler, "before"),
            mock.call(handler_before, "before"),
            mock.call(handler_after, "before"),
            mock.call(handler_noyield, "before"),
            mock.call("in", "1"),
            mock.call(handler, "error", e),
        ]
    )

    error_handler.assert_not_awaited()


def test_context_hook_errors() -> None:
    error_handler = mock.Mock()
    m = mock.Mock()
    hook: ContextHook[None, None] = ContextHook(error_handler=error_handler)

    @hook.register
    def call_raises(_: None) -> Generator[None, None, None]:
        m(call_raises)
        raise ValueError

    @hook.register
    def generator_raises(_: None) -> Generator[None, None, None]:
        m(generator_raises)
        raise ValueError
        yield

    @hook.register
    def generator_raises_after(_: None) -> Generator[None, None, None]:
        yield
        m(generator_raises_after)
        raise ValueError

    @hook.register
    def generator_many_yield(_: None) -> Generator[None, None, None]:
        yield
        m(generator_many_yield)
        yield
        m(None)
        raise ValueError

    @hook.register
    def generator_many_yield_wont_stop(_: None) -> Generator[None, None, None]:
        yield
        try:
            yield
            m(None)
        except GeneratorExit:
            m(generator_many_yield_wont_stop)
            yield
            m(None)

    @hook.emit
    def scope(_: None) -> None:
        pass

    scope(None)

    m.assert_has_calls(
        [
            mock.call(call_raises),
            mock.call(generator_raises),
            mock.call(generator_many_yield_wont_stop),
            mock.call(generator_many_yield),
            mock.call(generator_raises_after),
        ]
    )

    assert error_handler.call_count == 5


def test_context_hook_error_errors() -> None:
    error_handler = mock.Mock()
    m = mock.Mock()
    hook: ContextHook[None, None] = ContextHook(error_handler=error_handler)

    @hook.register
    def generator_raise_in_except(_: None) -> Generator[None, None, None]:
        try:
            yield
        except Exception:
            m(generator_raise_in_except)
            raise ValueError from None

    @hook.register
    def generator_yield_in_except(_: None) -> Generator[None, None, None]:
        try:
            yield
        except Exception:
            m(generator_yield_in_except)
            yield
            m(None)

    @hook.register
    def generator_wont_stop(_: None) -> Generator[None, None, None]:
        try:
            yield
        except Exception:
            try:
                yield
            except GeneratorExit:
                m(generator_wont_stop)
                yield
                m(None)

    @hook.register
    def generator_reraise(_: None) -> Generator[None, None, None]:
        try:
            yield
        except Exception:
            m(generator_reraise)
            raise

    e = ValueError()

    with pytest.raises(ValueError):

        @hook.emit
        def scope(_: None) -> None:
            raise e

        scope(None)
    m.assert_has_calls(
        [
            mock.call(generator_reraise),
            mock.call(generator_wont_stop),
            mock.call(generator_yield_in_except),
            mock.call(generator_raise_in_except),
        ]
    )

    assert error_handler.call_count == 3


@pytest.mark.asyncio
async def test_async_context_hook_errors() -> None:
    error_handler = mock.AsyncMock()
    m = mock.Mock()
    hook: AsyncContextHook[None, None] = AsyncContextHook(error_handler=error_handler)

    @hook.register
    async def call_raises(_: None) -> None:
        m(call_raises)
        raise ValueError

    @hook.register
    async def generator_raises(_: None) -> AsyncGenerator[None, None]:
        m(generator_raises)
        raise ValueError
        yield

    @hook.register
    async def generator_raises_after(_: None) -> AsyncGenerator[None, None]:
        yield
        m(generator_raises_after)
        raise ValueError

    @hook.register
    async def generator_many_yield(_: None) -> AsyncGenerator[None, None]:
        yield
        m(generator_many_yield)
        yield
        m(None)
        raise ValueError

    @hook.register
    async def generator_many_yield_wont_stop(_: None) -> AsyncGenerator[None, None]:
        yield
        try:
            yield
            m(None)
        except GeneratorExit:
            m(generator_many_yield_wont_stop)
            yield
            m(None)

    @hook.emit
    async def scope(_: None) -> None:
        pass

    await scope(None)

    m.assert_has_calls(
        [
            mock.call(call_raises),
            mock.call(generator_raises),
            mock.call(generator_many_yield_wont_stop),
            mock.call(generator_many_yield),
            mock.call(generator_raises_after),
        ]
    )

    assert error_handler.await_count == 5


@pytest.mark.asyncio
async def test_async_context_hook_error_errors() -> None:
    error_handler = mock.AsyncMock()
    m = mock.Mock()
    hook: AsyncContextHook[None, None] = AsyncContextHook(error_handler=error_handler)

    @hook.register
    async def generator_raise_in_except(_: None) -> AsyncGenerator[None, None]:
        try:
            yield
        except Exception:
            m(generator_raise_in_except)
            raise ValueError from None

    @hook.register
    async def generator_yield_in_except(_: None) -> AsyncGenerator[None, None]:
        try:
            yield
        except Exception:
            m(generator_yield_in_except)
            yield
            m(None)

    @hook.register
    async def generator_wont_stop(_: None) -> AsyncGenerator[None, None]:
        try:
            yield
        except Exception:
            try:
                yield
            except GeneratorExit:
                m(generator_wont_stop)
                yield
                m(None)

    @hook.register
    async def generator_reraise(_: None) -> AsyncGenerator[None, None]:
        try:
            yield
        except Exception:
            m(generator_reraise)
            raise

    e = ValueError()

    with pytest.raises(ValueError):

        @hook.emit
        async def scope(_: None) -> None:
            raise e

        await scope(None)
    m.assert_has_calls(
        [
            mock.call(generator_reraise),
            mock.call(generator_wont_stop),
            mock.call(generator_yield_in_except),
            mock.call(generator_raise_in_except),
        ]
    )

    assert error_handler.await_count == 3


HookMock = mock.AsyncMock()
SyncHookMock = mock.Mock()


def context_mock(name: str) -> t.Callable:
    async def handler(arg: str) -> t.AsyncGenerator[None, str]:
        hook_mock = getattr(HookMock, name)
        try:
            await hook_mock.before(arg)
            result = yield
            await hook_mock.after(result)
        except Exception as e:
            await hook_mock.error(e)
        await hook_mock.done()

    return handler


mock_message_executed_handler = context_mock("message_executed")
mock_message_executed_handler_msg = context_mock("message_executed_msg")
mock_message_published_handler = context_mock("message_published")
mock_results_processed_handler = context_mock("results_processed")
mock_output_blocked_handler = context_mock("output_blocked")
mock_work_queue_built_handler = context_mock("work_queue_built")


async def mock_work_queue_built_handler_nocontext(arg: t.Any) -> None:
    await HookMock.work_queue_built_nocontext(arg)
    return None


@pytest.fixture
def hook_options() -> Hooks.Options:
    HookMock.reset_mock()
    SyncHookMock.reset_mock()

    hook_names = [
        "hook_failed",
        "items_batched",
        "message_polled",
        "message_scheduled",
        "message_submitted",
        "pipeline_events_emitted",
    ]
    sync_hook_names = [
        "executor_initialized",
    ]
    ctx_hook_names = [
        "message_executed",
        "message_published",
        "output_blocked",
        "results_processed",
        "work_queue_built",
    ]

    hooks_options = {}
    for hook_name in hook_names:
        hooks_options[hook_name] = [f"{__name__}.HookMock.{hook_name}"]
    for hook_name in sync_hook_names:
        hooks_options[hook_name] = [f"{__name__}.SyncHookMock.{hook_name}"]
    for hook_name in ctx_hook_names:
        hooks_options[hook_name] = [f"{__name__}.mock_{hook_name}_handler"]

    return Hooks.Options(**hooks_options)


async def test_hooks_service(
    hook_options: Hooks.Options,
    fake_queue_item: QueueItemWithState,
    executable_maker: t.Callable[..., ExecutableMessage],
) -> None:
    hooks = Hooks(hook_options)

    error = Exception()
    batch = ItemsBatch(job=fake_queue_item, items=[])
    xmsg = executable_maker()
    msg_events = PipelineEventsEmitted(xmsg=xmsg, events=[])
    message_published = MessagePublished(xmsg=xmsg, topic=None, output=None)  # type: ignore
    results_processed = ResultsProcessed(xmsg=xmsg, results=None)  # type: ignore

    await hooks.hook_failed.emit(error)
    HookMock.hook_failed.assert_awaited_once_with(error)

    await hooks.items_batched.emit(batch)
    HookMock.items_batched.assert_awaited_once_with(batch)

    await hooks.message_polled.emit(xmsg)
    HookMock.message_polled.assert_awaited_once_with(xmsg)

    await hooks.message_scheduled.emit(xmsg)
    HookMock.message_scheduled.assert_awaited_once_with(xmsg)

    await hooks.message_submitted.emit(xmsg)
    HookMock.message_submitted.assert_awaited_once_with(xmsg)

    await hooks.pipeline_events_emitted.emit(msg_events)
    HookMock.pipeline_events_emitted.assert_awaited_once_with(msg_events)

    hooks.executor_initialized.emit("executor_initialized")  # type: ignore
    SyncHookMock.executor_initialized.assert_called_once_with("executor_initialized")

    async def scope(arg: t.Any) -> t.Any:
        return arg

    await hooks.message_executed.emit(scope)(xmsg)
    HookMock.message_executed.before.assert_awaited_once_with(xmsg)
    HookMock.message_executed.after.assert_awaited_once_with(xmsg)

    await hooks.message_published.emit(scope)(message_published)
    HookMock.message_published.before.assert_awaited_once_with(message_published)
    HookMock.message_published.after.assert_awaited_once_with(message_published)

    await hooks.results_processed.emit(scope)(results_processed)
    HookMock.results_processed.before.assert_awaited_once_with(results_processed)
    HookMock.results_processed.after.assert_awaited_once_with(results_processed)

    await hooks.output_blocked.emit(scope)("topic")  # type: ignore
    HookMock.output_blocked.before.assert_awaited_once_with("topic")
    HookMock.output_blocked.after.assert_awaited_once_with("topic")

    await hooks.work_queue_built.emit(scope)(fake_queue_item)
    HookMock.work_queue_built.before.assert_awaited_once_with(fake_queue_item)
    HookMock.work_queue_built.after.assert_awaited_once_with(fake_queue_item)

    HookMock.hook_failed.assert_awaited_once_with(error)


async def test_custom_hooks(
    hook_options: Hooks.Options,
    fake_queue_item: QueueItemWithState,
    executable_maker: t.Callable[..., ExecutableMessage],
    executable_queue_maker: t.Callable[..., ExecutableQueue],
) -> None:
    hook_options.work_queue_built.append(
        f"{__name__}.mock_work_queue_built_handler_nocontext"
    )
    fake_queue_item.config["hooks"] = dataclasses.asdict(hook_options)
    xqueue = executable_queue_maker(definition=fake_queue_item)
    xmsg = executable_maker(executable_queue=xqueue)
    xmsg_with_config = executable_maker(
        executable_queue=xqueue,
        message=TopicMessage(
            args={},
            config={
                "hooks": {
                    "message_submitted": [hook_options.message_submitted[0] + "_msg"],
                    "message_executed": [hook_options.message_executed[0] + "_msg"],
                }
            },
        ),
    )
    hooks = Hooks()

    batch = ItemsBatch(job=fake_queue_item, items=[])
    await hooks.items_batched.emit(batch)
    HookMock.items_batched.assert_awaited_once_with(batch)

    await hooks.message_polled.emit(xmsg)
    HookMock.message_polled.assert_awaited_once_with(xmsg)

    await hooks.message_scheduled.emit(xmsg)
    HookMock.message_scheduled.assert_awaited_once_with(xmsg)

    await hooks.message_submitted.emit(xmsg_with_config)
    HookMock.message_submitted.assert_awaited_once_with(xmsg_with_config)
    HookMock.message_submitted_msg.assert_awaited_once_with(xmsg_with_config)

    msg_events = PipelineEventsEmitted(xmsg=xmsg_with_config, events=[])
    await hooks.pipeline_events_emitted.emit(msg_events)
    HookMock.pipeline_events_emitted.assert_awaited_once_with(msg_events)

    pipeline_result = PipelineResults(
        outputs=[PipelineOutput(channel="default", message=TopicMessage(args={}))]
    )

    @hooks.message_executed.emit
    async def executed_scope(xmsg: ExecutableMessage) -> PipelineResults:
        await HookMock.message_executed()
        return pipeline_result

    await executed_scope(xmsg_with_config)

    HookMock.message_executed.before.assert_awaited_once_with(xmsg_with_config)
    HookMock.message_executed.after.assert_awaited_once_with(pipeline_result)
    HookMock.message_executed_msg.before.assert_awaited_once_with(xmsg_with_config)
    HookMock.message_executed_msg.after.assert_awaited_once_with(pipeline_result)
    HookMock.message_executed.assert_awaited_once_with()

    @hooks.message_published.emit
    async def publish_scope(msg: MessagePublished) -> None:
        raise ValueError()

    message_published = MessagePublished(xmsg=xmsg, topic=None, output=None)  # type: ignore
    error = None
    try:
        await publish_scope(message_published)
        raise AssertionError()
    except ValueError as e:
        error = e

    HookMock.message_published.before.assert_awaited_once_with(message_published)
    HookMock.message_published.error.assert_awaited_once_with(error)

    @hooks.results_processed.emit
    async def results_processed_scope(msg: ResultsProcessed) -> None:
        await HookMock.results_processed()
        return None

    results_processed = ResultsProcessed(xmsg=xmsg, results=None)  # type: ignore
    await results_processed_scope(results_processed)

    HookMock.results_processed.before.assert_awaited_once_with(results_processed)
    HookMock.results_processed.after.assert_awaited_once_with(None)
    HookMock.results_processed.assert_awaited_once_with()

    @hooks.work_queue_built.emit
    async def work_queue_scope(queue_item: QueueItemWithState) -> ExecutableQueue:
        await HookMock.work_queue_built(queue_item)
        return xqueue

    await work_queue_scope(fake_queue_item)

    HookMock.work_queue_built.before.assert_awaited_once_with(fake_queue_item)
    HookMock.work_queue_built.after.assert_awaited_once_with(xqueue)
    HookMock.work_queue_built_nocontext.assert_awaited_once_with(fake_queue_item)
    HookMock.work_queue_built.assert_awaited_once_with(fake_queue_item)

    HookMock.hook_failed.assert_not_awaited()
