"""One-answer funnel mode and the actual Ace inbox filter."""
import app as app_module
from tests.helpers import drain_until_done


def _round(client, text, funnel):
    with client.websocket_connect("/ws?token=test-token") as ws:
        ws.receive_json()
        ws.send_json({
            "type": "user",
            "text": text,
            "funnel": funnel,
            "crosstalk": False,
        })
        return drain_until_done(ws)


def test_funnel_hides_partner_takes_and_links_them_by_timestamp(client):
    events = _round(client, "Should I add size here?", True)
    replies = [event for event in events if event["type"] == "reply"]
    assert replies == []
    assert any(event["type"] == "user" for event in events)
    assert events[-1]["type"] == "done"
    hidden = [item for item in app_module.history if item.get("hidden")]
    assert len(hidden) == 2
    assert {item["from"] for item in hidden} == {"grok", "gemini"}
    user = next(item for item in app_module.history if item["from"] == "shavor")
    assert all(item["for_ts"] == user["ts"] for item in hidden)
    assert all(item.get("funnel") for item in hidden)

    with client.websocket_connect("/ws?token=test-token") as ws:
        restored = ws.receive_json()
    assert any(item.get("hidden") for item in restored["messages"])


def test_panel_mode_shows_both_partners(client):
    events = _round(client, "Read the level back to me.", False)
    replies = [event for event in events if event["type"] == "reply"]
    assert {event["provider"] for event in replies} == {"grok", "gemini"}
    assert not any(item.get("hidden") for item in app_module.history)


def test_ace_inbox_returns_only_mentions(client):
    _round(client, "Status check, no mention.", True)
    inbox = client.get("/api/ace-inbox", params={"token": "test-token", "since": ""})
    assert inbox.status_code == 200
    assert inbox.json()["messages"] == []

    _round(client, "Ace, look at this @ace", False)
    inbox = client.get("/api/ace-inbox", params={"token": "test-token", "since": ""})
    messages = inbox.json()["messages"]
    assert len(messages) == 1
    assert messages[0]["from"] == "shavor"
    assert "@ace" in messages[0]["text"].lower()


def test_ace_reply_can_mark_a_funnel_answer(client):
    _round(client, "One answer please.", True)
    user = next(item for item in app_module.history if item["from"] == "shavor")
    with client.websocket_connect("/ws?token=test-token") as ws:
        ws.receive_json()
        posted = client.post("/api/ace-reply", json={
            "token": "test-token",
            "text": "Hold the add. Wait for the $7 print.",
            "funnel_answer": True,
            "for_ts": user["ts"],
        })
        assert posted.status_code == 200
        event = ws.receive_json()
    assert event["type"] == "reply"
    assert event["provider"] == "ace"
    assert event["text"].startswith("Hold the add.")
    saved = next(item for item in app_module.history if item["from"] == "ace")
    assert saved["funnel_answer"] is True
    assert saved["for_ts"] == user["ts"]


def test_mock_provider_is_used_for_the_round(client):
    events = _round(client, "Ping the mock desk.", False)
    texts = [event["text"] for event in events if event["type"] == "reply"]
    assert texts
    assert all(text.startswith("[mock ") for text in texts)
