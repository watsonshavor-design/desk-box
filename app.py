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

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
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
    "notes": ("General-purpose room: trading, research, learning, daily life, "
              "any questions — everyone answers, no hierarchy. "
              "Penny stocks allowed. Nothing-under-$5 retired. "
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


def load_history():
    """Restore room history from the on-disk log so a redeploy/restart
    doesn't wipe the room. Returns at most HISTORY_KEEP entries."""
    entries = []
    try:
        with open(LOG_PATH) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if (isinstance(e, dict) and e.get("from")
                        and e.get("text") and e.get("ts")):
                    entries.append(e)
    except OSError:
        pass
    return entries[-HISTORY_KEEP:]


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
            entry = {"from": name, "round": 1, "text": reply,
                     "ts": now_iso()}
            await room.broadcast({"type": "reply", "provider": name,
                                  "round": 1, "text": reply,
                                  "ts": entry["ts"]})
            await remember(entry)
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
                entry = {"from": name, "round": 2, "text": reply,
                         "ts": now_iso()}
                await room.broadcast({"type": "reply", "provider": name,
                                      "round": 2, "text": reply,
                                      "ts": entry["ts"]})
                await remember(entry)
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
    # Restore room history from disk — otherwise every redeploy wipes it.
    async with history_lock:
        history.extend(load_history())
        del history[:max(0, len(history) - HISTORY_KEEP)]
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


# --- Ace bridge -----------------------------------------------------------
# There is no API for Muse, so Ace joins the room through these two
# endpoints instead:
#   GET  /api/ace-inbox?token=...&since=<iso-ts> -> {"messages": [...]}
#       Returns Shavor's messages mentioning @ace newer than `since`.
#   POST /api/ace-reply  {"token": ..., "text": ..., "discuss"?: bool}
#       -> {"ok": true}
#       Injects Ace's reply into the room (broadcast + session log).
#       When discuss=true, Ace's post is ALSO queued for the Grok/Gemini
#       fan-out (framed as Ace speaking, single round) so Rail and Anchor
#       can respond to it. Bot replies never re-enter the queue, so a
#       discuss post yields at most one bot round and can never loop.
# A scheduled check on Ace's side polls the inbox every couple of minutes
# and posts replies. Any app implementing these two endpoints gets Ace.
@app.get("/api/ace-inbox")
async def ace_inbox(token: str = "", since: str = ""):
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    since = since.replace(" ", "+")  # tolerate unencoded '+' in iso timestamps
    async with history_lock:
        msgs = [m for m in history
                if m.get("from") == "shavor"
                and "@ace" in m.get("text", "").lower()
                and m.get("ts", "") > since]
    return JSONResponse({"messages": msgs})


@app.post("/api/ace-reply")
async def ace_reply(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "bad body"}, status_code=400)
    if body.get("token") != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    text = str(body.get("text", "")).strip()[:4000]
    if not text:
        return JSONResponse({"error": "empty"}, status_code=400)
    entry = {"from": "ace", "text": text, "ts": now_iso()}
    await remember(entry)
    await room.broadcast({"type": "reply", "provider": "ace",
                          "round": 1, "text": text, "ts": entry["ts"]})
    # Two-way discussion: when the caller sets discuss=true, Ace's post is
    # also queued for the Grok/Gemini fan-out so Rail and Anchor respond.
    # Framed so the providers know the speaker is Ace, not Shavor.
    # Loop safety: bot replies are broadcast + logged only — they never go
    # back on msg_queue — so one discuss post yields at most one bot round.
    if body.get("discuss") is True:
        framed = ("[From Ace, your fellow desk partner — respond to him as a "
                  "peer, not as Shavor. Direct and plain.]\n\n" + text)
        await msg_queue.put((framed, False))
    return JSONResponse({"ok": True})


@app.get("/api/recent")
async def recent(token: str = "", limit: int = 20):
    """Last N room messages (all senders) — lets the Ace bridge worker see
    thread context before replying to an @ace mention."""
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    try:
        limit = max(1, min(int(limit or 20), 50))
    except (TypeError, ValueError):
        limit = 20
    async with history_lock:
        msgs = list(history[-limit:])
    return JSONResponse({"messages": msgs})


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
