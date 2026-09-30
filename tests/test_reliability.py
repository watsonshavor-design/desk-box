"""Message ids, idempotent send, per-thread work, and exactly-once Ace jobs."""
import time
import uuid

import app as app_module
from tests.helpers import drain_until_done


def test_resend_does_not_duplicate_the_question_or_the_takes(client):
    message_id = str(uuid.uuid4())
    with client.websocket_connect("/ws?token=test-token") as ws:
        ws.receive_json()
        payload = {
            "type": "user",
            "text": "Same question twice.",
            "funnel": True,
            "message_id": message_id,
            "thread_id": "desk",
        }
        ws.send_json(payload)
        first = drain_until_done(ws)
        ws.send_json(payload)
        ack = ws.receive_json()
    assert ack["type"] == "ack"
    assert ack["duplicate"] is True
    assert ack["message_id"] == message_id
    users = [event for event in first if event["type"] == "user"]
    assert len(users) == 1
    assert users[0]["message_id"] == message_id
    stored = [item for item in app_module.store.list_entries() if item["from"] == "shavor"]
    assert len(stored) == 1
    hidden = [item for item in app_module.store.list_entries() if item.get("hidden")]
    assert len(hidden) == 2
    assert {item["for_message_id"] for item in hidden} == {message_id}


def test_ace_job_completes_once(client):
    message_id = str(uuid.uuid4())
    with client.websocket_connect("/ws?token=test-token") as ws:
        ws.receive_json()
        ws.send_json({
            "type": "user",
            "text": "Need one answer.",
            "funnel": True,
            "message_id": message_id,
        })
        drain_until_done(ws)
        claimed = client.get("/api/ace/jobs/next", params={"token": "test-token"}).json()["job"]
        assert claimed["message_id"] == message_id
        assert claimed["payload"]["question"] == "Need one answer."
        done = client.post(f"/api/ace/jobs/{claimed['id']}/complete", json={
            "token": "test-token",
            "result": {
                "answer": "Wait for the next print.",
                "why": "Both partners were heard.",
                "action": ["Do nothing yet."],
                "agreement": "agrees",
                "desk_notes": "Rail contributed. Anchor contributed.",
            },
        })
        assert done.status_code == 200
        event = ws.receive_json()
        again = client.post(f"/api/ace/jobs/{claimed['id']}/complete", json={
            "token": "test-token",
            "result": {"answer": "A second answer."},
        })
    assert event["type"] == "reply"
    assert event["provider"] == "ace"
    assert event["for_message_id"] == message_id
    assert event["agreement"] == "agrees"
    assert again.status_code == 409
    answers = [item for item in app_module.store.list_entries() if item.get("funnel_answer")]
    assert len(answers) == 1


def test_failed_job_can_be_retried_then_claimed(client):
    message_id = str(uuid.uuid4())
    with client.websocket_connect("/ws?token=test-token") as ws:
        ws.receive_json()
        ws.send_json({"type": "user", "text": "Hold this.", "funnel": True, "message_id": message_id})
        drain_until_done(ws)
    job = client.get("/api/ace/jobs/next", params={"token": "test-token"}).json()["job"]
    failed = client.post(f"/api/ace/jobs/{job['id']}/fail", json={
        "token": "test-token",
        "reason": "bridge timeout",
    })
    assert failed.json()["status"] == "pending"
    again = client.get("/api/ace/jobs/next", params={"token": "test-token"}).json()["job"]
    assert again["id"] == job["id"]
    assert again["attempts"] >= 2


def test_session_cookie_opens_the_shell_without_a_query_token(client):
    denied = client.get("/api/v2/dashboard")
    assert denied.status_code == 403
    opened = client.post("/api/v2/session", json={"token": "test-token"})
    assert opened.status_code == 200
    assert "desk_session" in opened.cookies
    assert "test-token" not in opened.text
    dashboard = client.get("/api/v2/dashboard")
    assert dashboard.status_code == 200
    page = client.get("/")
    assert page.status_code == 200
    revoked = client.post("/api/v2/session/revoke")
    assert revoked.status_code == 200
    assert client.get("/api/v2/dashboard").status_code == 403


def test_search_pin_and_decision(client):
    message_id = str(uuid.uuid4())
    with client.websocket_connect("/ws?token=test-token") as ws:
        ws.receive_json()
        ws.send_json({
            "type": "user",
            "text": "Pin the GLND rule.",
            "funnel": False,
            "message_id": message_id,
        })
        drain_until_done(ws)
    pinned = client.post(f"/api/v2/messages/{message_id}/pin", params={"token": "test-token"})
    assert pinned.json()["pinned"] is True
    found = client.get("/api/v2/search", params={"token": "test-token", "q": "GLND"}).json()
    assert any(item["message_id"] == message_id for item in found["messages"])
    decision = client.post("/api/v2/decisions", params={"token": "test-token"}, json={
        "title": "Keep the $7 sell",
        "status": "active",
        "message_id": message_id,
    })
    assert decision.status_code == 200
    titles = [item["title"] for item in client.get("/api/v2/dashboard", params={"token": "test-token"}).json()["decisions"]]
    assert "Keep the $7 sell" in titles


def test_legacy_log_backfills_once(client):
    app_module.store.wipe()
    fresh = app_module.now_iso()
    entries = [
        {"from": "shavor", "text": "older line", "ts": fresh},
        {"from": "ace", "text": "noted", "ts": fresh},
    ]
    assert app_module.store.backfill(entries) == 2
    assert app_module.store.backfill(entries) == 0
    assert app_module.store.count_messages() == 2


def test_unrelated_threads_overlap(client, monkeypatch):
    started = []

    async def slow(prompt, **kwargs):
        started.append(time.monotonic())
        import asyncio
        await asyncio.sleep(0.35)
        return "slow take"

    monkeypatch.setitem(app_module.PROVIDERS, "grok", slow)
    monkeypatch.setitem(app_module.PROVIDERS, "gemini", slow)
    app_module.store.create_thread("Trading", "trading", "trading")
    app_module.store.create_thread("Family", "family", "family")
    began = time.monotonic()
    with client.websocket_connect("/ws?token=test-token") as ws:
        ws.receive_json()
        ws.send_json({"type": "user", "text": "Thread A", "funnel": False, "thread_id": "trading", "message_id": str(uuid.uuid4())})
        ws.send_json({"type": "user", "text": "Thread B", "funnel": False, "thread_id": "family", "message_id": str(uuid.uuid4())})
        drain_until_done(ws, limit=40)
        drain_until_done(ws, limit=40)
    elapsed = time.monotonic() - began
    assert len(started) >= 2
    assert elapsed < 1.2


def test_second_funnel_reply_for_the_same_timestamp_is_ignored(client):
    with client.websocket_connect("/ws?token=test-token") as ws:
        ws.receive_json()
        ws.send_json({"type": "user", "text": "Once.", "funnel": True})
        events = drain_until_done(ws)
        user = next(event for event in events if event["type"] == "user")
        body = {
            "token": "test-token",
            "text": "First synthesis.",
            "funnel_answer": True,
            "for_ts": user["ts"],
            "for_message_id": user["message_id"],
        }
        assert client.post("/api/ace-reply", json=body).json()["ok"] is True
        second = client.post("/api/ace-reply", json=body).json()
    assert second["duplicate"] is True
    answers = [item for item in app_module.store.list_entries() if item.get("funnel_answer")]
    assert len(answers) == 1
