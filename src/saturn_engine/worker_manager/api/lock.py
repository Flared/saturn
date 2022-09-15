import threading

from flask import Blueprint

from saturn_engine.core.api import LockInput
from saturn_engine.core.api import LockResponse
from saturn_engine.database import session_scope
from saturn_engine.utils.flask import Json
from saturn_engine.utils.flask import jsonify
from saturn_engine.utils.flask import marshall_request
from saturn_engine.worker_manager.app import current_app
from saturn_engine.worker_manager.services.lock import lock_jobs

bp = Blueprint("lock", __name__, url_prefix="/api/lock")

_LOCK_LOCK = threading.Lock()


@bp.route("", methods=("POST",))
def post_lock() -> Json[LockResponse]:
    with _LOCK_LOCK:
        lock_input = marshall_request(LockInput)
        max_assigned_items: int = current_app.saturn.config.work_items_per_worker
        static_definitions = current_app.saturn.static_definitions
        with session_scope() as session:
            lock_response = lock_jobs(
                lock_input,
                max_assigned_items=max_assigned_items,
                static_definitions=static_definitions,
                session=session,
            )

        return jsonify(lock_response)
