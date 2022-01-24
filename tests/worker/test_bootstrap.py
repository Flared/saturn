from typing import Optional

import pickle  # noqa: S403

from saturn_engine.worker.executors.bootstrap import RemoteException
from saturn_engine.worker.executors.bootstrap import wrap_remote_exception


class MyError(Exception):
    pass


def raises(
    e: Exception,
    *,
    cause: Optional[Exception] = None,
    context: Optional[Exception] = None
) -> None:
    x = {"foo": "bar"}  # noqa: F841
    try:
        if context:
            raise context
    finally:
        raise e from cause


def raises_remote(
    e: Exception,
    *,
    cause: Optional[Exception] = None,
    context: Optional[Exception] = None
) -> None:
    try:
        # That would be in a remote process
        with wrap_remote_exception():
            raises(e, cause=cause, context=context)
    except Exception as e:
        remote_error = pickle.dumps(e)
    else:
        raise AssertionError()

    # And we would be back in the host process
    local_error = pickle.loads(remote_error)  # noqa: S301
    raise local_error


def test_remote_exception() -> None:
    try:
        raises_remote(ValueError("test", 1))
        raise AssertionError
    except RemoteException as e:
        assert isinstance(e, RemoteException)
        assert e.remote_traceback.__cause__ is None
        assert e.remote_traceback.__context__ is None
        assert e.remote_traceback.exc_type == "ValueError"
        assert e.remote_traceback.stack[-1].line.strip() == "raise e from cause"
        assert e.remote_traceback.stack[-1].locals["x"] == "{'foo': 'bar'}"

    try:
        raises_remote(MyError("test", 1), cause=ValueError("cause"))
        raise AssertionError
    except RemoteException as e:
        assert isinstance(e, RemoteException)
        assert e.remote_traceback.__cause__ is not None
        assert e.remote_traceback.__cause__.exc_type == "ValueError"
        assert e.remote_traceback.__cause__.stack == []

        assert e.remote_traceback.__context__ is None
        assert e.remote_traceback.exc_type == "MyError"
        assert e.remote_traceback.stack[-1].line.strip() == "raise e from cause"
        assert e.remote_traceback.stack[-1].locals["x"] == "{'foo': 'bar'}"

    try:
        raises_remote(
            MyError("test", 1), cause=ValueError("cause"), context=ValueError("context")
        )
        raise AssertionError
    except RemoteException as e:
        assert isinstance(e, RemoteException)
        assert e.remote_traceback.__cause__ is not None
        assert e.remote_traceback.__cause__.exc_type == "ValueError"
        assert e.remote_traceback.__context__ is not None
        assert e.remote_traceback.__context__.exc_type == "ValueError"
        assert e.remote_traceback.__context__.stack[-1].line.strip() == "raise context"

        assert e.remote_traceback.exc_type == "MyError"
        assert e.remote_traceback.stack[-1].line.strip() == "raise e from cause"
        assert e.remote_traceback.stack[-1].locals
        assert e.remote_traceback.stack[-1].locals["x"] == "{'foo': 'bar'}"
