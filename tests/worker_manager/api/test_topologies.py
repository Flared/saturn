from flask.testing import FlaskClient


def test_put_topology_patch(client: FlaskClient) -> None:
    resp = client.put(
        "/api/topologies/patch",
        json={
            "apiVersion": "saturn.flared.io/v1alpha1",
            "kind": "SaturnTopic",
            "metadata": {"name": "test-topic"},
            "spec": {"type": "RabbitMQTopic", "options": {"queue_name": "queue_1"}},
        },
    )
    assert resp.status_code == 200
    assert resp.json == {
        "apiVersion": "saturn.flared.io/v1alpha1",
        "kind": "SaturnTopic",
        "metadata": {"name": "test-topic", "labels": {}},
        "spec": {"type": "RabbitMQTopic", "options": {"queue_name": "queue_1"}},
    }

    # We add a new patch, overriding the last one
    resp = client.put(
        "/api/topologies/patch",
        json={
            "apiVersion": "saturn.flared.io/v1alpha1",
            "kind": "SaturnTopic",
            "metadata": {"name": "test-topic"},
            "spec": {"type": "RabbitMQTopic", "options": {"queue_name": "queue_2"}},
        },
    )
    assert resp.status_code == 200
    assert resp.json == {
        "apiVersion": "saturn.flared.io/v1alpha1",
        "kind": "SaturnTopic",
        "metadata": {"name": "test-topic", "labels": {}},
        "spec": {"type": "RabbitMQTopic", "options": {"queue_name": "queue_2"}},
    }
