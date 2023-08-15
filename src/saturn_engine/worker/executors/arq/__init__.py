QUEUE_TIMEOUT = 600
RESULT_TIMEOUT = 1200
EXECUTE_FUNC_NAME = "remote_execute"

healthcheck_interval = 10


def healthcheck_key(job_id: str) -> str:
    return f"saturn:arq:{job_id}:healthcheck"
