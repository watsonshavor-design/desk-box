"""Versioned read endpoints for the command-center shell.

PR B exposes dashboard, health, and version only. Tasks, briefings,
gainers, and connected apps stay absent until a real source exists.
"""
import json
import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


def _version_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "version.json")


def read_version():
    with open(_version_path()) as handle:
        return json.load(handle)


def _denied():
    return JSONResponse({"error": "bad token"}, status_code=403)


def _authed(desk, token, request: Request):
    if token == desk.DESK_TOKEN:
        return True
    return desk.store.session_ok(request.cookies.get("desk_session"))


def _provider(desk, env_name):
    if os.environ.get("MOCK_PROVIDERS") == "1":
        return {
            "state": "mock",
            "detail": "Dev mode is on. This is not a live presence check.",
        }
    if os.environ.get(env_name):
        return {
            "state": "configured",
            "detail": "A credential is set. The desk does not poll presence.",
        }
    return {
        "state": "unconfigured",
        "detail": "No credential is configured on the server.",
    }


def register(app: FastAPI):
    @app.get("/api/v2/version")
    async def version(request: Request, token: str = ""):
        import app as desk
        if not _authed(desk, token, request):
            return _denied()
        payload = read_version()
        payload["protocol"] = str(payload.get("protocol", "1"))
        return payload

    @app.get("/api/v2/health")
    async def health(request: Request, token: str = ""):
        import app as desk
        if not _authed(desk, token, request):
            return _denied()
        return {
            "ok": True,
            "rail": _provider(desk, "XAI_API_KEY"),
            "anchor": _provider(desk, "GEMINI_API_KEY"),
            "ace": {
                "state": "bridge",
                "detail": "Ace joins through the bridge. Inbox delivery is still @ace only.",
            },
        }

    @app.get("/api/v2/dashboard")
    async def dashboard(request: Request, token: str = ""):
        import app as desk
        if not _authed(desk, token, request):
            return _denied()
        async with desk.history_lock:
            visible = [item for item in desk.history if not item.get("hidden")]
        recent = []
        for item in reversed(visible):
            if len(recent) >= 3:
                break
            text = (item.get("text") or "").strip()
            attachment = item.get("attachment") or None
            recent.append({
                "kind": "file" if attachment else "conversation",
                "from": item.get("from"),
                "text": text[:160],
                "ts": item.get("ts"),
                "attachment_kind": (attachment or {}).get("kind"),
            })
        memory_at = desk.room_memory.get("updated_ts") or None
        import market
        from datetime import datetime, timezone
        today = datetime.now(timezone.utc).date().isoformat()
        task_rows = []
        for task in desk.store.list_tasks("today", today)[:3]:
            task_rows.append({
                "id": task["id"],
                "title": task["title"],
                "owner": task["owner"],
                "due_label": task.get("due_at") or "No due time",
                "status": task["status"],
            })
        gainers = market.top_gainers("combined")
        apps = []
        for app_row in market.integrations()["apps"]:
            apps.append({
                "id": app_row["id"],
                "name": app_row["name"],
                "state": app_row["account"],
                "state_label": "Connected" if app_row["account"] == "connected" else "Not connected",
            })
        return {
            "connection": {"state": "ok"},
            "priority": None,
            "tasks": task_rows,
            "briefing": desk.store.latest_briefing(),
            "desk": {
                "ace": {
                    "state": "bridge",
                    "detail": "Via the Ace bridge. Not a live presence check.",
                },
                "rail": _provider(desk, "XAI_API_KEY"),
                "anchor": _provider(desk, "GEMINI_API_KEY"),
            },
            "goal": {
                "title": desk.DESK_CONTEXT.get("goal") or "",
                "lock": desk.DESK_CONTEXT.get("glnd_lock") or "",
                "risk": desk.DESK_CONTEXT.get("risk") or "",
                "notes": desk.DESK_CONTEXT.get("notes") or "",
                "progress": None,
                "updated_at": memory_at,
            },
            "recent": recent,
            "decisions": desk.store.list_decisions()[:5],
            "connected_apps": {"items": apps},
            "top_gainers": {
                "source": gainers["source"],
                "state": gainers["state"],
                "retrieved_at": gainers.get("retrieved_at"),
                "detail": gainers.get("detail") or "",
                "rows": gainers.get("rows") or [],
                "parts": gainers.get("parts") or {},
                "empty": gainers.get("detail") or "No verified gainers.",
            },
        }
