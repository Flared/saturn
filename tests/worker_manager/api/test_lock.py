from flask.testing import FlaskClient


def test_api_lock_bad_input(client: FlaskClient) -> None:
    resp = client.post("/api/lock", json={})
    assert resp.status_code == 400
    assert resp.json == {
        "error": {
            "code": "BAD_LOCK_INPUT",
            "message": "Bad lock input",
            "data": {"worker_id": ["Missing data for required field."]},
        },
    }


def test_api_lock(client: FlaskClient) -> None:
    resp = client.post(
        "/api/lock",
        json={
            "worker_id": "test",
        },
    )
    assert resp.status_code == 200
    assert resp.json == {"items": []}
