import typing as t

import asyncio
import contextlib
import datetime
import sys

from saturn_engine.core import PipelineOutput
from saturn_engine.core import PipelineResults
from saturn_engine.utils import ExceptionGroup
from saturn_engine.utils.asyncutils import Cancellable
from saturn_engine.utils.asyncutils import TasksGroupRunner
from saturn_engine.utils.log import getLogger
from saturn_engine.worker.error_handling import HandledError
from saturn_engine.worker.error_handling import process_pipeline_exception
from saturn_engine.worker.resources.manager import ResourceUnavailable
from saturn_engine.worker.services import Services
from saturn_engine.worker.services.hooks import MessagePublished
from saturn_engine.worker.services.hooks import PipelineEventsEmitted
from saturn_engine.worker.services.hooks import ResultsProcessed

from . import Executor
from .executable import ExecutableMessage


class ExecutorQueue:
    CLOSE_TIMEOUT = datetime.timedelta(seconds=60)

    def __init__(
        self,
        executor: Executor,
        services: Services,
    ) -> None:
        self.logger = getLogger(__name__, self)
        self.submit_lock = asyncio.Lock()
        self.queue: asyncio.Queue[ExecutableMessage] = asyncio.Queue(maxsize=1)
        self.submit_tasks = TasksGroupRunner(name="executor-submit")
        self.processing_tasks = TasksGroupRunner(name="executor-queue")
        self.consuming_tasks = TasksGroupRunner(name="executor-consuming")
        self.message_executor = executor
        self.resources_manager = services.s.resources_manager
        self.executor = executor
        self.services = services
        self.poll = Cancellable(self.queue.get)

    def start(self) -> None:
        self.is_running = True
        for i in range(self.executor.concurrency):
            self.logger.debug("Spawning new queue task")
            self.processing_tasks.create_task(
                self.run_queue(), name=f"executor-queue-{i}"
            )
        self.consuming_tasks.start()
        self.submit_tasks.start()
        self.processing_tasks.start()

    async def run_queue(self) -> None:
        while self.is_running:
            processable = await self.poll()
            processable._executing_context.callback(self.queue.task_done)
            with contextlib.suppress(Exception), processable.saturn_context():
                async with (
                    processable._context,
                    processable._executing_context,
                ):

                    @self.services.s.hooks.message_executed.emit
                    async def scope(
                        xmsg: ExecutableMessage,
                    ) -> PipelineResults:
                        try:
                            return await self.executor.process_message(xmsg)
                        except Exception:
                            exc_type, exc_value, exc_traceback = sys.exc_info()
                            assert (  # noqa: S101
                                exc_type and exc_value and exc_traceback
                            )
                            process_pipeline_exception(
                                queue=xmsg.queue.definition,
                                message=xmsg.message.message,
                                exc_type=exc_type,
                                exc_value=exc_value,
                                exc_traceback=exc_traceback,
                            )
                            raise

                    results = None
                    error = None
                    try:
                        results = await scope(processable)
                    except HandledError as e:
                        results = e.results
                        error = e
                    finally:
                        if results:
                            self.consuming_tasks.create_task(
                                self.process_results(
                                    xmsg=processable,
                                    results=results,
                                    # Transfer the message context to the results
                                    # processing scope.
                                    context=processable._context.pop_all(),
                                    context_error=error
                                    if error and not error.handled
                                    else None,
                                )
                            )

                            processable.update_resources_used(results.resources)

                    if error:
                        error.reraise()

    async def process_results(
        self,
        *,
        xmsg: ExecutableMessage,
        results: PipelineResults,
        context: contextlib.AsyncExitStack,
        context_error: t.Optional[HandledError],
    ) -> None:
        @self.services.s.hooks.results_processed.emit
        async def scope(msg: ResultsProcessed) -> None:
            await self.consume_output(processable=xmsg, output=msg.results.outputs)

            await self.services.s.hooks.pipeline_events_emitted.emit(
                PipelineEventsEmitted(events=msg.results.events, xmsg=xmsg)
            )

        with contextlib.suppress(Exception), xmsg.saturn_context():
            async with context:
                await scope(
                    ResultsProcessed(
                        xmsg=xmsg,
                        results=results,
                    )
                )
                if context_error:
                    context_error.reraise()

    async def submit(self, processable: ExecutableMessage) -> None:
        # Get the lock to ensure we don't acquire resource if the submit queue
        # is already full.
        async with self.submit_lock:
            # Try first to check if we have the resources available so we can
            # then check if the executor queue is ready. That way, the scheduler
            # will pause until the executor is free again.
            if await self.acquire_resources(processable, wait=False):
                await self.queue_submit(processable)
            else:
                # Park the queue from which the processable comes from.
                # The queue should be unparked once the resources are acquired.
                processable.park()
                # To avoid blocking the executor queue while we wait on resource,
                # create a background task to wait on resources.
                self.submit_tasks.create_task(
                    self.delayed_submit(processable),
                    name=f"delayed-submit({processable})",
                )

    async def acquire_resources(
        self, processable: ExecutableMessage, *, wait: bool
    ) -> bool:
        missing_resources = processable.message.missing_resources
        if not missing_resources:
            return True

        self.logger.debug("locking resources: %s", missing_resources)
        try:
            resources_context = await self.resources_manager.acquire_many(
                missing_resources, wait=wait
            )
        except ResourceUnavailable:
            return False

        resources = await processable.attach_resources(resources_context)
        self.logger.debug("locked resources: %s", resources)
        return True

    async def delayed_submit(self, processable: ExecutableMessage) -> None:
        """Submit a pipeline after waiting to acquire its resources"""
        with processable.saturn_context():
            try:
                await self.acquire_resources(processable, wait=True)
            finally:
                await processable.unpark()

            async with self.submit_lock:
                await self.queue_submit(processable)

    async def queue_submit(self, processable: ExecutableMessage) -> None:
        await self.services.s.hooks.message_submitted.emit(processable)
        await self.queue.put(processable)

    async def consume_output(
        self, *, processable: ExecutableMessage, output: list[PipelineOutput]
    ) -> None:
        try:
            errors = []
            for item in output:
                topics = processable.output.get(item.channel, [])
                for topic in topics:

                    @self.services.s.hooks.message_published.emit
                    async def scope(message_published: MessagePublished) -> None:
                        if topic is None:
                            return
                        with contextlib.suppress(Exception):
                            if await topic.publish(item.message, wait=False):
                                return

                        @self.services.s.hooks.output_blocked.emit
                        async def scope(_: MessagePublished) -> None:
                            processable.park()
                            await topic.publish(item.message, wait=True)

                        await scope(message_published)

                    try:
                        await scope(
                            MessagePublished(xmsg=processable, topic=topic, output=item)
                        )
                    except Exception as e:
                        errors.append(e)
            if errors:
                raise ExceptionGroup("Failed to process outputs", errors)

        finally:
            await processable.unpark()

    async def close(self) -> None:
        self.is_running = False
        # Shutdown the queue task first so we don't process any new item.
        self.logger.debug("Closing processing tasks")
        self.poll.cancel()
        await self.processing_tasks.close(timeout=self.CLOSE_TIMEOUT.total_seconds())
        # Delayed tasks waiting on resource
        self.logger.debug("Closing submitting tasks")
        await self.submit_tasks.close()
        # Then close the output consuming task.
        self.logger.debug("Closing consuming tasks")
        await self.consuming_tasks.close(timeout=self.CLOSE_TIMEOUT.total_seconds())

        self.logger.debug("Closing executor")
        await self.executor.close()
