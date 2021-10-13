from typing import Any
from typing import NoReturn
from typing import Optional
from typing import Tuple

import flask
from flask import Flask
from flask import abort as flask_abort
from flask import jsonify
from werkzeug.exceptions import HTTPException
from werkzeug.sansio.response import Response as SansioResponse


class CustomHTTPException(HTTPException):
    """Our enriched HTTPException"""

    def __init__(
        self,
        *,
        description: Optional[str],
        response: Optional[SansioResponse],
        http_code: int,
        error_code: Optional[str],
        data: Any,
    ) -> None:
        super().__init__(
            description,
            response,
        )
        self.code = http_code
        self.error_code = error_code
        self.data = data


def abort(
    *,
    http_code: int = 500,
    error_code: Optional[str] = None,
    message: Optional[str] = None,
    data: Any = None,
) -> NoReturn:
    """Abort and raise our custom HTTPException"""
    try:
        flask_abort(http_code)
    except HTTPException as e:
        raise CustomHTTPException(
            description=message or e.description,
            response=e.response,
            http_code=http_code,
            error_code=error_code,
            data=data,
        ) from e


def get_exception_data(
    error_code: str, message: str, error_data: Any = None
) -> dict[str, Any]:
    error_data = error_data or dict()
    return {
        "error": {
            "code": error_code,
            "message": message,
            "data": error_data,
        },
    }


def http_exception_error_handler(
    error: HTTPException,
) -> Tuple[flask.Response, int]:
    http_code: int = error.code or 500

    if isinstance(error, CustomHTTPException):
        exception_data = get_exception_data(
            error_code=error.error_code or str(error.code),
            message=error.description or str(error),
            error_data=error.data,
        )
    else:
        error_data: dict = dict()
        if isinstance(data := getattr(error, "data", None), dict):
            error_data = data.get("messages", {})

        exception_data = get_exception_data(
            error_code=str(error.code),
            message=error.description or str(error),
            error_data=error_data,
        )

    return jsonify(exception_data), http_code


def register_http_exception_error_handler(app: Flask) -> None:
    app.errorhandler(HTTPException)(http_exception_error_handler)
