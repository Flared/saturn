from saturn_engine.core.pipeline import CancellationToken

def progress_work(message: str) -> bool: ...

def can_cancel(message: str, token: CancellationToken) -> None:
    while not token.is_cancelled:
        if progress_work(message):
            break
