import dataclasses
from datetime import datetime
from datetime import timedelta

import desert
import flask
import marshmallow
from flask import Blueprint
from flask import jsonify
from flask import request

from saturn.database import async_session_scope
from saturn.models.queue import Queue
from saturn.stores import queues_store
from saturn.worker_manager.http_errors import abort

bp = Blueprint("lock", __name__, url_prefix="/api/lock")


@dataclasses.dataclass
class LockInput:
    worker_id: str

    @classmethod
    def schema(cls) -> marshmallow.Schema:
        return desert.schema(cls)


@bp.route("", methods=("POST",))
async def post_lock() -> flask.Response:
    try:
        lock_input: LockInput = LockInput.schema().load(request.json or dict())
    except marshmallow.ValidationError as validation_error:
        abort(
            http_code=400,
            error_code="BAD_LOCK_INPUT",
            message="Bad lock input",
            data=validation_error.messages,
        )

    # Note:
    # - For now, we just assign 10 items per worker.
    # - Leftover items remain unassigned.
    # - TODO(aviau): Acquire the "assignation lock".
    # - TODO(aviau) Don't assign jobs that are not due to run.
    # - TODO(aviau): Instead of assigning 10 items, assign based on
    #                worker capacity and/or number of active workers.

    assignation_expiration_cutoff: datetime = datetime.now() - timedelta(minutes=15)
    max_assigned_items: int = 10

    assigned_items: list[Queue] = []

    async with async_session_scope() as session:

        # Obtains items that were already assigned.
        assigned_items.extend(
            await queues_store.get_assigned_queues(
                session=session,
                worker_id=lock_input.worker_id,
                assigned_after=assignation_expiration_cutoff,
            )
        )

        # Unassign extra items.
        for unassigned_item in assigned_items[max_assigned_items:]:
            unassigned_item.assigned_at = None
            unassigned_item.assigned_to = None

        assigned_items = assigned_items[:10]

        # Obtain new queues
        if len(assigned_items) < max_assigned_items:
            assigned_items.extend(
                await queues_store.get_unassigned_queues(
                    session=session,
                    assigned_before=assignation_expiration_cutoff,
                    limit=max_assigned_items - len(assigned_items),
                )
            )

        # Refresh assignments
        new_assigned_at = datetime.now()
        for assigned_item in assigned_items:
            assigned_item.assigned_at = new_assigned_at
            assigned_item.assigned_to = lock_input.worker_id

        return jsonify(
            {
                "items": [
                    assigned_item.as_work_item() for assigned_item in assigned_items
                ]
            }
        )
