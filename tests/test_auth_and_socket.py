"""Auth, WebSocket connect, send, and history restore."""
import json

import pytest
from starlette.websockets import WebSocketDisconnect

import app as app_module
from tests.helpers import drain_until_done


def test_pages_and_desk_require_the_room_token(client):
    assert client.get("/").status_code == 403
    assert client.get("/api/desk").status_code == 403
    assert client.get("/api/memory").status_code == 403
    assert client.get("/api/ace-inbox").status_code == 403
    assert client.get("/api/recent").status_code == 403

    page = client.get("/", params={"token": "test-token"})
    assert page.status_code == 200
    assert "Shavor's Desk" in page.text

    desk = client.get("/api/desk", params={"token": "test-token"})
    assert desk.status_code == 200
    body = desk.json()
    assert "goal" in body and "glnd_lock" in body and "risk" in body


def test_websocket_rejects_a_bad_token(client):
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/ws?token=nope") as ws:
            ws.receive_json()


def test_send_broadcasts_and_reconnect_restores_history(client):
    with client.websocket_connect("/ws?token=test-token") as ws:
        hello = ws.receive_json()
        assert hello["type"] == "history"
        assert hello["messages"] == []
        ws.send_json({"type": "user", "text": "Check the open book.", "funnel": False})
        events = drain_until_done(ws)
    kinds = [event["type"] for event in events]
    assert "user" in kinds
    assert "reply" in kinds
    assert kinds[-1] == "done"
    providers = {event["provider"] for event in events if event["type"] == "reply"}
    assert providers == {"grok", "gemini"}

    with client.websocket_connect("/ws?token=test-token") as ws:
        restored = ws.receive_json()
    texts = [item["text"] for item in restored["messages"]]
    assert "Check the open book." in texts
    assert any(item["from"] == "grok" for item in restored["messages"])
    assert any(item["from"] == "gemini" for item in restored["messages"])


def test_history_file_restores_and_prunes_old_lines(client):
    old = "2000-01-01T00:00:00+00:00"
    fresh = app_module.now_iso()
    with open(app_module.LOG_PATH, "w") as handle:
        handle.write(json.dumps({"from": "shavor", "text": "stale", "ts": old}) + "\n")
        handle.write(json.dumps({"from": "shavor", "text": "kept", "ts": fresh}) + "\n")
        handle.write("not json\n")
    restored = app_module.load_history()
    assert [item["text"] for item in restored] == ["kept"]
    with open(app_module.LOG_PATH) as handle:
        rewritten = handle.read()
    assert "stale" not in rewritten
    assert "kept" in rewritten
