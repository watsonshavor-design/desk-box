"""Desk Box — one live room for Shavor's trading desk.

Shavor sends a message over WebSocket; the server fans it out to Grok and
Gemini in parallel (with the desk-context header), then pushes each labeled
reply back over the same socket. Optional cross-talk round: each model
reacts to the other's take before the round closes.

Security notes:
  - API keys live ONLY in server env vars (XAI_API_KEY, GEMINI_API_KEY).
    They are never sent to the browser.
  - A per-boot token guards the WebSocket. Set DESK_TOKEN yourself, or the
    server generates one and prints the URL.
  - One cross-talk round max per message; no bot-to-bot chaining.
  - Night Desk bots have no API, so they join via paste — the room keeps a
    session log (desk-log.jsonl) Shavor can paste back to them.

Run:  uvicorn app:app --host 0.0.0.0 --port 8000
"""
import asyncio
import json
import logging
import os
import secrets
from datetime import datetime, timezone

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from providers import PROVIDERS

log = logging.getLogger("desk-box")
logging.basicConfig(level=logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_PATH = os.path.join(BASE_DIR, "desk-log.jsonl")
HISTORY_KEEP = 200  # in-memory messages per room

DESK_TOKEN = os.environ.get("DESK_TOKEN") or secrets.token_urlsafe(24)

# Static desk context shown in the room's side panel (update as the desk evolves).
DESK_CONTEXT = {
    "goal": "$1M trading profit by Sep 2027 — working pace 8-10%/week",
    "glnd_lock": ("Sell GLND at $7.00 — waiting Sell 2300 @ $7 ETH stays on the book. "
                  "Rails: soft $7.15 / stall ~$6.90 / proceeds floor >= $15k. "
                  "Cost yardstick ~$5.199. Never suggest canceling/resizing the $7 sell."),
    "risk": ("Max 3 names, max $1,500/name, risk <=0.5%/trade (~$77). "
             "Kill switches: -$150/day, -$450/week, -$900/month."),
    "notes": ("Penny stocks allowed. Nothing-under-$5 retired. "
              "Day trades, flat overnight by default. Night Desk joins via paste relay."),
}

app = FastAPI(title="Desk Box")
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")),
           name="static")

history = []          # list of dicts: {from, provider?, text, ts}
history_lock = asyncio.Lock()
msg_queue = asyncio.Queue()  # one message processed at a time


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def append_log(entry):
    try:
        with open(LOG_PATH, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except OSError:
        pass


async def remember(entry):
    async with history_lock:
        history.append(entry)
        del history[:max(0, len(history) - HISTORY_KEEP)]
    append_log(entry)


class Room:
    """Tracks connected browsers; broadcasts to all of them."""

    def __init__(self):
        self.sockets = set()
        self.lock = asyncio.Lock()

    async def add(self, ws):
        async with self.lock:
            self.sockets.add(ws)

    async def remove(self, ws):
        async with self.lock:
            self.sockets.discard(ws)

    async def broadcast(self, payload):
        data = json.dumps(payload)
        async with self.lock:
            targets = list(self.sockets)
        dead = []
        for ws in targets:
            try:
                await ws.send_text(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            await self.remove(ws)


room = Room()


async def fan_out(text, crosstalk):
    """Ask Grok and Gemini in parallel; optionally run one cross-talk round."""
    first = {}

    async def one(name):
        try:
            await room.broadcast({"type": "status", "provider": name,
                                  "state": "thinking"})
            reply = await PROVIDERS[name](text)
            first[name] = reply
            await room.broadcast({"type": "reply", "provider": name,
                                  "round": 1, "text": reply})
            await remember({"from": name, "round": 1, "text": reply,
                            "ts": now_iso()})
        except Exception as e:  # one provider failing never blocks the other
            err = f"{name} failed: {e}"
            log.warning(err)
            await room.broadcast({"type": "status", "provider": name,
                                  "state": "error", "detail": str(e)})

    await asyncio.gather(*(one(n) for n in PROVIDERS))

    if crosstalk and "grok" in first and "gemini" in first:
        async def react(name, other):
            try:
                await room.broadcast({"type": "status", "provider": name,
                                      "state": "reacting"})
                reply = await PROVIDERS[name](
                    text, crosstalk=True, other_take=first[other])
                await room.broadcast({"type": "reply", "provider": name,
                                      "round": 2, "text": reply})
                await remember({"from": name, "round": 2, "text": reply,
                                "ts": now_iso()})
            except Exception as e:
                log.warning("%s cross-talk failed: %s", name, e)
                await room.broadcast({"type": "status", "provider": name,
                                      "state": "error", "detail": str(e)})

        await asyncio.gather(react("grok", "gemini"), react("gemini", "grok"))

    await room.broadcast({"type": "done"})


async def worker():
    while True:
        text, crosstalk = await msg_queue.get()
        try:
            await fan_out(text, crosstalk)
        finally:
            msg_queue.task_done()


@app.on_event("startup")
async def on_startup():
    asyncio.create_task(worker())
    if "DESK_TOKEN" not in os.environ:
        log.info("DESK_TOKEN not set — generated per-boot token.")
    log.info("Desk Box up. Open /?token=%s", DESK_TOKEN)


@app.get("/")
async def index(token: str = ""):
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    return FileResponse(os.path.join(BASE_DIR, "static", "index.html"))


@app.get("/api/desk")
async def desk(token: str = ""):
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    return JSONResponse(DESK_CONTEXT)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    token = ws.query_params.get("token", "")
    if token != DESK_TOKEN:
        await ws.close(code=4403)
        return
    await ws.accept()
    await room.add(ws)
    # send recent history so a fresh tab sees the room state
    async with history_lock:
        recent = list(history[-50:])
    await ws.send_text(json.dumps({"type": "history", "messages": recent}))
    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if msg.get("type") == "user" and msg.get("text", "").strip():
                text = msg["text"].strip()[:4000]
                crosstalk = bool(msg.get("crosstalk"))
                entry = {"from": "shavor", "text": text, "ts": now_iso()}
                await remember(entry)
                await room.broadcast({"type": "user", "text": text,
                                      "ts": entry["ts"]})
                await msg_queue.put((text, crosstalk))
    except WebSocketDisconnect:
        pass
    finally:
        await room.remove(ws)
