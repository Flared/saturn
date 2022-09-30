from typing import Optional

import datetime

from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.orm import joinedload

from saturn_engine.database import AnySession
from saturn_engine.database import AnySyncSession
from saturn_engine.models import Queue


def create_queue(
    *,
    session: AnySession,
    name: str,
) -> Queue:
    queue = Queue(name=name)
    session.add(queue)
    return queue


def get_assigned_queues(
    *,
    session: AnySyncSession,
    worker_id: str,
    assigned_after: datetime.datetime,
) -> list[Queue]:
    assigned_jobs: list[Queue] = (
        session.execute(
            select(Queue)
            .options(joinedload(Queue.job))
            .where(
                Queue.enabled.is_(True),
                Queue.assigned_to == worker_id,
                Queue.assigned_at >= assigned_after,
            )
            .order_by(Queue.name)
        )
        .scalars()
        .all()
    )
    return assigned_jobs


def get_unassigned_queues(
    *,
    session: AnySyncSession,
    assigned_before: datetime.datetime,
    limit: int,
) -> list[Queue]:
    unassigned_queues: list[Queue] = (
        session.execute(
            select(Queue)
            .options(joinedload(Queue.job))
            .where(
                Queue.enabled.is_(True),
                or_(
                    Queue.assigned_at.is_(None),
                    Queue.assigned_at < assigned_before,
                ),
            )
            .limit(limit)
        )
        .scalars()
        .all()
    )
    return unassigned_queues


def get_queue(
    name: str,
    *,
    session: AnySyncSession,
) -> Optional[Queue]:
    return session.get(Queue, name)


def disable_queue(
    *,
    name: str,
    session: AnySyncSession,
) -> None:
    session.execute(update(Queue).where(Queue.name == name).values(enabled=False))


def reset_queue(
    *,
    name: str,
    session: AnySyncSession,
) -> None:
    session.execute(
        update(Queue)
        .where(Queue.name == name)
        .values(
            enabled=True,
            assigned_to=None,
            assigned_at=None,
        )
    )
