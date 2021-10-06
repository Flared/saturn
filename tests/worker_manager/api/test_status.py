from flask.testing import FlaskClient


def test_api_status(client: FlaskClient) -> None:
    resp = client.get("/api/status")
    assert resp.status_code == 200
