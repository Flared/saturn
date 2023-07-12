import asyncio
import contextlib
import datetime
import sys

from saturn_engine.core import PipelineOutput
from saturn_engine.core import PipelineResults
from saturn_engine.utils.asyncutils import TasksGroupRunner
from saturn_engine.utils.log import getLogger
from saturn_engine.worker.error_handling import process_pipeline_exception
from saturn_engine.worker.resources.manager import ResourceUnavailable
from saturn_engine.worker.services import Services
from saturn_engine.worker.services.hooks import MessagePublished
from saturn_engine.worker.services.hooks import PipelineEventsEmitted
from saturn_engine.worker.topic import Topic

from . import Executor
from .executable import ExecutableMessage


class ExecutorQueue:
    CLOSE_TIMEOUT = datetime.timedelta(seconds=10)

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
        self.is_running = False

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
            processable = await self.queue.get()
            processable._context.callback(self.queue.task_done)
            try:
                async with processable._context:

                    @self.services.s.hooks.message_executed.emit
                    async def scope(
                        xmsg: ExecutableMessage,
                    ) -> PipelineResults:
                        try:
                            return await self.executor.process_message(xmsg)
                        except Exception:
                            _, exc_value, exc_traceback = sys.exc_info()
                            # Shouldn't be None but we want to make mypy happy
                            if exc_value is None or exc_traceback is None:
                                raise
                            result = process_pipeline_exception(
                                xmsg.queue.definition.output, exc_value, exc_traceback
                            )
                            if not result:
                                raise
                            return result

                    try:
                        output = await scope(processable)
                    except Exception:  # noqa: S110
                        pass
                    else:
                        self.consuming_tasks.create_task(
                            self.services.s.hooks.pipeline_events_emitted.emit(
                                PipelineEventsEmitted(
                                    events=output.events, xmsg=processable
                                )
                            ),
                            name=f"emit-pipeline-events({processable})",
                        )
                        processable.update_resources_used(output.resources)
                        self.consuming_tasks.create_task(
                            self.consume_output(
                                processable=processable, output=output.outputs
                            ),
                            name=f"consume-output({processable})",
                        )
            except Exception:
                self.logger.exception("Failed to process queue item")

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
            for item in output:
                topics = processable.output.get(item.channel, [])
                for topic in topics:

                    @self.services.s.hooks.message_published.emit
                    async def scope(_: MessagePublished) -> None:
                        if topic is None:
                            return
                        with contextlib.suppress(Exception):
                            if await topic.publish(item.message, wait=False):
                                return

                        @self.services.s.hooks.output_blocked.emit
                        async def scope(topic: Topic) -> None:
                            processable.park()
                            await topic.publish(item.message, wait=True)

                        await scope(topic)

                    with contextlib.suppress(Exception):
                        await scope(
                            MessagePublished(xmsg=processable, topic=topic, output=item)
                        )
        finally:
            await processable.unpark()

    async def close(self) -> None:
        # Shutdown the queue task first so we don't process any new item.
        await self.processing_tasks.close(timeout=self.CLOSE_TIMEOUT.total_seconds())
        # Delayed tasks waiting on resource
        await self.submit_tasks.close()
        # Then close the output consuming task.
        await self.consuming_tasks.close(timeout=self.CLOSE_TIMEOUT.total_seconds())

        await self.executor.close()
