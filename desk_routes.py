"""Tasks, briefings, gainers, and connected-app routes."""
from datetime import datetime, timezone

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

import market


def _denied():
    return JSONResponse({"error": "bad token"}, status_code=403)


def _authed(desk, token, request):
    if token == desk.DESK_TOKEN:
        return True
    return desk.store.session_ok(request.cookies.get("desk_session"))


def _today():
    return datetime.now(timezone.utc).date().isoformat()


def register(app: FastAPI):
    @app.get("/api/v2/tasks")
    async def list_tasks(request: Request, token: str = "", view: str = "today"):
        import app as desk
        if not _authed(desk, token, request):
            return _denied()
        if view not in ("today", "upcoming", "waiting", "completed", "all"):
            view = "today"
        rows = desk.store.list_tasks(None if view == "all" else view, _today())
        return {"view": view, "label": "Desk tasks", "tasks": rows}

    @app.post("/api/v2/tasks")
    async def create_task(request: Request, token: str = ""):
        import app as desk
        if not _authed(desk, token, request):
            return _denied()
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "bad body"}, status_code=400)
        title = str(body.get("title") or "").strip()
        if not title:
            return JSONResponse({"error": "empty"}, status_code=400)
        task = desk.store.add_task(
            title, str(body.get("notes") or ""), body.get("due_at"),
            str(body.get("owner") or "shavor"), str(body.get("status") or "open"),
            str(body.get("priority") or "normal"), body.get("source_message_id"),
            body.get("thread_id"),
        )
        return task

    @app.patch("/api/v2/tasks/{task_id}")
    async def patch_task(task_id: str, request: Request, token: str = ""):
        import app as desk
        if not _authed(desk, token, request):
            return _denied()
        try:
            body = await request.json()
        except Exception:
            body = {}
        task = desk.store.update_task(task_id, body if isinstance(body, dict) else {})
        if not task:
            return JSONResponse({"error": "not found"}, status_code=404)
        return task

    @app.post("/api/v2/tasks/{task_id}/complete")
    async def complete_task(task_id: str, request: Request, token: str = ""):
        import app as desk
        if not _authed(desk, token, request):
            return _denied()
        task = desk.store.complete_task(task_id)
        if not task:
            return JSONResponse({"error": "not found"}, status_code=404)
        return task

    @app.get("/api/v2/briefings")
    async def list_briefings(request: Request, token: str = "", category: str = ""):
        import app as desk
        if not _authed(desk, token, request):
            return _denied()
        return {"briefings": desk.store.list_briefings(category or None)}

    @app.get("/api/v2/briefings/{briefing_id}")
    async def get_briefing(briefing_id: str, request: Request, token: str = ""):
        import app as desk
        if not _authed(desk, token, request):
            return _denied()
        item = desk.store.mark_briefing_read(briefing_id)
        if not item:
            return JSONResponse({"error": "not found"}, status_code=404)
        return item

    @app.post("/api/v2/briefings/ingest")
    async def ingest_briefing(request: Request):
        import app as desk
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "bad body"}, status_code=400)
        if body.get("token") != desk.DESK_TOKEN:
            return _denied()
        if not str(body.get("title") or "").strip():
            return JSONResponse({"error": "title required"}, status_code=400)
        return desk.store.upsert_briefing(body)

    @app.get("/api/v2/market/top-gainers")
    async def gainers(request: Request, token: str = "", source: str = "combined"):
        import app as desk
        if not _authed(desk, token, request):
            return _denied()
        return market.top_gainers(source)

    @app.get("/api/v2/integrations")
    async def list_integrations(request: Request, token: str = ""):
        import app as desk
        if not _authed(desk, token, request):
            return _denied()
        return market.integrations()

    @app.post("/api/v2/integrations/{provider}/launch")
    async def launch(provider: str, request: Request, token: str = ""):
        import app as desk
        if not _authed(desk, token, request):
            return _denied()
        target = market.launch_target(provider)
        if not target:
            return JSONResponse({"error": "no verified destination"}, status_code=404)
        return target