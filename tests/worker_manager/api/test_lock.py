from flask.testing import FlaskClient


def test_api_lock(client: FlaskClient) -> None:
    resp = client.post("/api/lock", json={})
    assert resp.status_code == 422
    assert resp.json == {
        "error": {
            "code": "422",
            "message": "The request was well-formed"
            " but was unable to be followed due to semantic errors.",
            "data": {"json": {"worker_id": ["Missing data for required field."]}},
        },
    }
