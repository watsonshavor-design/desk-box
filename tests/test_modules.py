"""Desk tasks, briefing provenance, and honest gainer sources."""
from datetime import datetime, timedelta, timezone

import market
import app as app_module


def test_task_round_trip_updates_home_and_keeps_the_message_link(client):
    created = client.post("/api/v2/tasks", params={"token": "test-token"}, json={
        "title": "Review the open",
        "owner": "shavor",
        "source_message_id": "msg-1",
        "notes": "From the desk",
    })
    assert created.status_code == 200
    task = created.json()
    assert task["source_message_id"] == "msg-1"
    home = client.get("/api/v2/dashboard", params={"token": "test-token"}).json()
    assert home["tasks"][0]["title"] == "Review the open"
    done = client.post(f"/api/v2/tasks/{task['id']}/complete", params={"token": "test-token"})
    assert done.json()["status"] == "completed"
    home = client.get("/api/v2/dashboard", params={"token": "test-token"}).json()
    assert home["tasks"] == []
    undone = client.patch(f"/api/v2/tasks/{task['id']}", params={"token": "test-token"}, json={"status": "open"})
    assert undone.json()["status"] == "open"
    today = client.get("/api/v2/tasks", params={"token": "test-token", "view": "today"}).json()
    assert len(today["tasks"]) == 1
    assert today["label"] == "Desk tasks"


def test_upcoming_and_waiting_views(client):
    later = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat()
    client.post("/api/v2/tasks", params={"token": "test-token"}, json={
        "title": "Later", "due_at": later, "owner": "ace",
    })
    client.post("/api/v2/tasks", params={"token": "test-token"}, json={
        "title": "Waiting on the broker", "status": "waiting", "owner": "external",
    })
    upcoming = client.get("/api/v2/tasks", params={"token": "test-token", "view": "upcoming"}).json()
    waiting = client.get("/api/v2/tasks", params={"token": "test-token", "view": "waiting"}).json()
    assert [item["title"] for item in upcoming["tasks"]] == ["Later"]
    assert [item["title"] for item in waiting["tasks"]] == ["Waiting on the broker"]


def test_briefing_dedup_and_provenance(client):
    first = client.post("/api/v2/briefings/ingest", json={
        "token": "test-token",
        "briefing_id": "b1",
        "category": "markets",
        "title": "Pre-market brief",
        "summary": "First read.",
        "published_at": "2026-09-29T11:00:00+00:00",
        "source_items": [{"name": "Desk", "url": "https://example.com/brief"}],
        "related_symbols": ["GLND"],
    })
    assert first.status_code == 200
    assert first.json()["pinned"] is True
    assert first.json()["provenance"] == "news"
    second = client.post("/api/v2/briefings/ingest", json={
        "token": "test-token",
        "category": "markets",
        "title": "A different headline for the same link",
        "summary": "Updated read.",
        "source_items": [{"name": "Desk", "url": "https://example.com/brief"}],
    })
    assert second.json()["id"] == "b1"
    listed = client.get("/api/v2/briefings", params={"token": "test-token", "category": "markets"}).json()
    assert len(listed["briefings"]) == 1
    assert listed["briefings"][0]["summary"] == "Updated read."
    analysis = client.post("/api/v2/briefings/ingest", json={
        "token": "test-token",
        "category": "life",
        "title": "Notes without a source",
        "summary": "Desk wrote this.",
        "source_items": [],
    })
    assert analysis.json()["provenance"] == "desk_analysis"
    assert client.post("/api/v2/briefings/ingest", json={"title": "no token"}).status_code == 403


def test_gainers_stay_empty_until_a_source_is_connected(client):
    body = client.get("/api/v2/market/top-gainers", params={"token": "test-token", "source": "moomoo"}).json()
    assert body["state"] == "disconnected"
    assert body["rows"] == []
    assert body["source"] == "moomoo"
    webull = client.get("/api/v2/market/top-gainers", params={"token": "test-token", "source": "webull"}).json()
    assert webull["rows"] == []
    assert webull["source"] == "webull"


def test_combined_merges_duplicates_and_keeps_each_source(client):
    def moomoo():
        return {
            "state": "ok", "detail": "", "retrieved_at": "2026-09-29T13:00:00+00:00",
            "session": "regular", "quote_delay_seconds": 0,
            "rows": [{"ticker": "aaa", "last": 2.5, "change_pct": 18.0, "session": "regular"}],
        }

    def webull():
        return {
            "state": "ok", "detail": "", "retrieved_at": "2026-09-29T13:00:05+00:00",
            "session": "regular", "quote_delay_seconds": 0,
            "rows": [
                {"ticker": "AAA", "last": 2.55, "change_pct": 19.0, "session": "regular"},
                {"ticker": "BBB", "last": 4, "change_pct": 11.0, "session": "regular"},
            ],
        }

    market.use_loader("moomoo", moomoo)
    market.use_loader("webull", webull)
    body = client.get("/api/v2/market/top-gainers", params={"token": "test-token", "source": "combined"}).json()
    assert [row["ticker"] for row in body["rows"]] == ["AAA", "BBB"]
    aaa = body["rows"][0]
    assert {item["source"] for item in aaa["attributions"]} == {"moomoo", "webull"}
    assert "moomoo" in aaa["source"] and "webull" in aaa["source"]
    moomoo_only = client.get("/api/v2/market/top-gainers", params={"token": "test-token", "source": "moomoo"}).json()
    assert moomoo_only["rows"][0]["source"] == "moomoo"
    assert all(row["source"] == "moomoo" for row in moomoo_only["rows"])


def test_one_disconnected_broker_is_not_relabeled(client):
    market.use_loader("webull", lambda: {
        "state": "ok", "detail": "", "retrieved_at": "2026-09-29T13:00:00+00:00",
        "rows": [{"ticker": "CCC", "last": 1, "change_pct": 5, "session": "regular"}],
    })
    body = client.get("/api/v2/market/top-gainers", params={"token": "test-token"}).json()
    assert body["parts"]["moomoo"]["state"] == "disconnected"
    assert body["parts"]["webull"]["state"] == "ok"
    assert body["rows"][0]["source"] == "webull"
    assert "moomoo" not in body["rows"][0]["source"]


def test_app_launch_is_a_verified_destination_and_not_a_connected_account(client):
    listed = client.get("/api/v2/integrations", params={"token": "test-token"}).json()
    accounts = {item["id"]: item["account"] for item in listed["apps"]}
    assert accounts == {"youtube": "not_connected", "facebook": "not_connected", "snapchat": "not_connected"}
    youtube = client.post("/api/v2/integrations/youtube/launch", params={"token": "test-token"})
    assert youtube.status_code == 200
    assert youtube.json()["web"] == "https://www.youtube.com/"
    assert youtube.json()["android_package"] == "com.google.android.youtube"
    facebook = client.post("/api/v2/integrations/facebook/launch", params={"token": "test-token"}).json()
    snap = client.post("/api/v2/integrations/snapchat/launch", params={"token": "test-token"}).json()
    assert facebook["web"] == "https://www.facebook.com/"
    assert snap["android_package"] == "com.snapchat.android"
    assert client.post("/api/v2/integrations/moomoo/launch", params={"token": "test-token"}).status_code == 404
