import dataclasses

import desert
import flask
import marshmallow
from flask import Blueprint
from flask import jsonify
from flask import request

from saturn.worker_manager.http_errors import abort

bp = Blueprint("lock", __name__, url_prefix="/api/lock")


@dataclasses.dataclass
class LockInput:
    worker_id: str

    @classmethod
    def schema(cls) -> marshmallow.Schema:
        return desert.schema(cls)


@dataclasses.dataclass
class LockResponse:
    items: list[str]  # TODO: this will be list[WorkItem]


@bp.route("", methods=("POST",))
async def post_lock() -> flask.Response:
    try:
        _: LockInput = LockInput.schema().load(request.json or dict())
    except marshmallow.ValidationError as validation_error:
        abort(
            http_code=400,
            error_code="BAD_LOCK_INPUT",
            message="Bad lock input",
            data=validation_error.messages,
        )
    return jsonify(LockResponse(items=[]))
