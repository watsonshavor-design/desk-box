"""Command-center shell: version, health, dashboard, and the hosted page."""


def test_version_health_and_dashboard_are_token_gated(client):
    assert client.get("/api/v2/version").status_code == 403
    assert client.get("/api/v2/health").status_code == 403
    assert client.get("/api/v2/dashboard").status_code == 403

    version = client.get("/api/v2/version", params={"token": "test-token"})
    assert version.status_code == 200
    body = version.json()
    assert body["frontend"] == "1.6.0"
    assert body["backend"] == "1.6.0"
    assert body["protocol"] == "2"

    health = client.get("/api/v2/health", params={"token": "test-token"}).json()
    assert health["ok"] is True
    assert health["rail"]["state"] == "mock"
    assert health["ace"]["state"] == "bridge"

    dashboard = client.get("/api/v2/dashboard", params={"token": "test-token"}).json()
    assert dashboard["tasks"] == []
    assert dashboard["briefing"] is None
    assert dashboard["priority"] is None
    assert dashboard["goal"]["progress"] is None
    assert dashboard["decisions"] == []
    assert "top_gainers" not in dashboard
    assert "connected_apps" not in dashboard
    assert dashboard["desk"]["anchor"]["state"] == "mock"


def test_shell_is_network_first_and_names_the_routes(client):
    page = client.get("/", params={"token": "test-token"})
    assert page.status_code == 200
    assert page.headers["cache-control"] == "no-store"
    assert "Shavor's Desk" in page.text
    assert "/static/app.js?v=1.6.0" in page.text
    assert "/static/app.css?v=1.5.0" in page.text or "/static/app.css?v=1.6.0" in page.text
    assert "maximum-scale" not in page.text

    script = client.get("/static/app.js").text
    for label in ("Home", "Desk", "Tasks", "Briefings", "More"):
        assert label in script
    assert 'aria-label="More desk options"' in script
    assert 'localStorage.getItem("desk_mode") || "one_answer"' in script
    assert "No desk tasks yet" in script
    assert "No briefing yet" in script
    assert "#A070F0" in client.get("/static/app.css").text


def test_dashboard_recent_uses_real_messages_only(client):
    with client.websocket_connect("/ws?token=test-token") as ws:
        ws.receive_json()
        ws.send_json({"type": "user", "text": "Note the close.", "funnel": False})
        from tests.helpers import drain_until_done
        drain_until_done(ws)
    recent = client.get("/api/v2/dashboard", params={"token": "test-token"}).json()["recent"]
    texts = [item["text"] for item in recent]
    assert "Note the close." in texts
    assert all(item["kind"] == "conversation" for item in recent)
