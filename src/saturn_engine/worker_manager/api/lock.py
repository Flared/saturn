import logging
import threading
from datetime import datetime
from datetime import timedelta

from flask import Blueprint

from saturn_engine.core.api import LockInput
from saturn_engine.core.api import LockResponse
from saturn_engine.database import session_scope
from saturn_engine.models.queue import Queue
from saturn_engine.stores import queues_store
from saturn_engine.utils.flask import Json
from saturn_engine.utils.flask import jsonify
from saturn_engine.utils.flask import marshall_request
from saturn_engine.worker_manager.app import current_app

bp = Blueprint("lock", __name__, url_prefix="/api/lock")

_LOCK_LOCK = threading.Lock()


@bp.route("", methods=("POST",))
def post_lock() -> Json[LockResponse]:
    with _LOCK_LOCK:
        logger = logging.getLogger(f"{__name__}.post_lock")
        lock_input = marshall_request(LockInput)

        # Note:
        # - Leftover items remain unassigned.

        assignation_expiration_cutoff: datetime = datetime.now() - timedelta(minutes=15)
        max_assigned_items: int = current_app.saturn.config.work_items_per_worker

        assigned_items: list[Queue] = []

        with session_scope() as session:

            # Obtains items that were already assigned.
            assigned_items.extend(
                queues_store.get_assigned_queues(
                    session=session,
                    worker_id=lock_input.worker_id,
                    assigned_after=assignation_expiration_cutoff,
                )
            )

            # Unassign extra items.
            for unassigned_item in assigned_items[max_assigned_items:]:
                unassigned_item.assigned_at = None
                unassigned_item.assigned_to = None

            assigned_items = assigned_items[:max_assigned_items]

            # Obtain new queues
            if len(assigned_items) < max_assigned_items:
                assigned_items.extend(
                    queues_store.get_unassigned_queues(
                        session=session,
                        assigned_before=assignation_expiration_cutoff,
                        limit=max_assigned_items - len(assigned_items),
                    )
                )

            for item in assigned_items:
                item.join_definitions(current_app.saturn.static_definitions)

            # Collect resource for assigned work
            static_definitions = current_app.saturn.static_definitions
            resources = {}
            # Copy list since the iteration could drop items from assigned_items.
            for item in assigned_items.copy():
                item_resources = {}
                for resource_type in item.queue_item.pipeline.info.resources.values():
                    pipeline_resources = static_definitions.resources_by_type.get(
                        resource_type
                    )
                    if not pipeline_resources:
                        logger.error(
                            "Skipping queue item, resource missing: item=%s, "
                            "resource=%s",
                            item.name,
                            resource_type,
                        )
                        # Do not update assign the object in the database.
                        assigned_items.remove(item)
                        break

                    item_resources.update({r.name: r for r in pipeline_resources})
                # Didn't break out due to resource missing.
                else:
                    resources.update(item_resources)

            # Refresh assignments
            new_assigned_at = datetime.now()
            for assigned_item in assigned_items:
                assigned_item.assigned_at = new_assigned_at
                assigned_item.assigned_to = lock_input.worker_id

            queue_items = []
            for item in assigned_items:
                queue_items.append(item.queue_item)

        return jsonify(
            LockResponse(
                items=queue_items,
                resources=list(sorted(resources.values(), key=lambda r: r.name)),
            )
        )
