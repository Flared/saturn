import datetime
from typing import Union

from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from saturn_engine.core.api import QueueItem
from saturn_engine.database import AnyAsyncSession
from saturn_engine.database import AnySession
from saturn_engine.models import Queue


def create_queue(
    *,
    session: Union[AnySession],
    name: str,
    spec: QueueItem,
) -> Queue:
    queue = Queue(name=name, spec=spec)
    session.add(queue)
    return queue


async def get_assigned_queues(
    *,
    session: AnyAsyncSession,
    worker_id: str,
    assigned_after: datetime.datetime,
) -> list[Queue]:
    assigned_jobs: list[Queue] = (
        (
            await session.execute(
                select(Queue)
                .options(
                    joinedload(Queue.job),
                )
                .where(Queue.assigned_to == worker_id)
                .where(Queue.assigned_at >= assigned_after)
                .order_by(Queue.name)
            )
        )
        .scalars()
        .all()
    )
    return assigned_jobs


async def get_unassigned_queues(
    *,
    session: AnyAsyncSession,
    assigned_before: datetime.datetime,
    limit: int,
) -> list[Queue]:
    unassigned_queues: list[Queue] = (
        (
            await session.execute(
                select(Queue)
                .options(
                    joinedload(Queue.job),
                )
                .where(
                    or_(
                        Queue.assigned_at.is_(None),
                        Queue.assigned_at < assigned_before,
                    )
                )
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )
    return unassigned_queues
