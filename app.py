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
import base64
import json
import logging
import os
import re
import secrets
import uuid
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, File, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from providers import PROVIDERS
import providers as providers_mod
import gainers as gainers_mod


# Shared room memory, maintained by Ace and pushed from the main chat.
# Injected into every bot prompt so Rail and Anchor remember past
# conversations the way Ace does. Capped small to protect credits.
room_memory = {"digest": "", "updated_ts": ""}


def load_room_memory():
    try:
        with open(MEMORY_PATH) as f:
            return f.read().strip()
    except OSError:
        return ""


def save_room_memory(digest):
    room_memory["digest"] = digest
    room_memory["updated_ts"] = now_iso()
    providers_mod.ROOM_MEMORY = digest
    try:
        with open(MEMORY_PATH, "w") as f:
            f.write(f"# Room memory (updated {room_memory['updated_ts']})\n\n{digest}\n")
    except OSError:
        pass

log = logging.getLogger("desk-box")
logging.basicConfig(level=logging.INFO)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Durable data dir — set DESK_DATA_DIR=/data once a Railway volume is mounted
# there; otherwise everything lives next to the code (ephemeral on Railway).
DATA_DIR = os.environ.get("DESK_DATA_DIR", BASE_DIR)
LOG_PATH = os.path.join(DATA_DIR, "desk-log.jsonl")
MEMORY_PATH = os.path.join(DATA_DIR, "room-memory.md")
HISTORY_KEEP = 2000   # in-memory messages per room (14-day window)
HISTORY_SEND = 200    # messages sent to a newly connected UI
RETENTION_DAYS = 14   # chats are never deleted before this; pruned after

# Photo/video sharing: files live on the durable volume (when mounted),
# served token-gated, pruned with the 14-day retention window.
UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")
IMAGE_EXTS = {"jpg", "jpeg", "png", "gif", "webp"}
VIDEO_EXTS = {"mp4", "mov", "webm"}
IMAGE_MIMES = {"jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png",
               "gif": "image/gif", "webp": "image/webp"}
MAX_IMAGE_BYTES = 10 * 1024 * 1024
MAX_VIDEO_BYTES = 50 * 1024 * 1024
UPLOAD_RE = re.compile(r"^[0-9a-f]{32}\.(jpg|jpeg|png|gif|webp|mp4|mov|webm)$")


def prune_uploads():
    """Delete uploaded files older than the retention window."""
    cutoff = (datetime.now(timezone.utc)
              - timedelta(days=RETENTION_DAYS)).timestamp()
    try:
        os.makedirs(UPLOAD_DIR, exist_ok=True)
        for name in os.listdir(UPLOAD_DIR):
            p = os.path.join(UPLOAD_DIR, name)
            try:
                if os.path.isfile(p) and os.path.getmtime(p) < cutoff:
                    os.remove(p)
            except OSError:
                pass
    except OSError:
        pass


def resolve_attachment(att):
    """Validate a client-supplied attachment dict.
    Returns (attachment, image, kind) — image is a (mime, base64) tuple
    for provider vision, or None."""
    attachment, image, kind = None, None, None
    if isinstance(att, dict) and att.get("url"):
        safe = os.path.basename(att["url"])
        if UPLOAD_RE.match(safe):
            ext = safe.rsplit(".", 1)[-1].lower()
            kind = "image" if ext in IMAGE_EXTS else "video"
            attachment = {"url": f"/uploads/{safe}", "kind": kind,
                          "name": str(att.get("name") or safe)[:120]}
            if kind == "image":
                try:
                    with open(os.path.join(UPLOAD_DIR, safe), "rb") as f:
                        raw = f.read()
                    if raw and len(raw) <= MAX_IMAGE_BYTES:
                        image = (IMAGE_MIMES[ext],
                                 base64.b64encode(raw).decode())
                except OSError:
                    pass
    return attachment, image, kind

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
        cutoff = retention_cutoff()
        fresh = [e for e in history if e.get("ts", "") >= cutoff]
        del history[:]
        history.extend(fresh[-HISTORY_KEEP:])
    append_log(entry)


def load_history():
    """Restore room history from the on-disk log so a redeploy/restart
    doesn't wipe the room. Honors the 14-day retention window."""
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
    cutoff = retention_cutoff()
    entries = [e for e in entries if e.get("ts", "") >= cutoff]
    # Rewrite the log pruned so it never grows past the retention window.
    try:
        with open(LOG_PATH, "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")
    except OSError:
        pass
    return entries[-HISTORY_KEEP:]


def retention_cutoff():
    return (datetime.now(timezone.utc)
            - timedelta(days=RETENTION_DAYS)).isoformat()


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


# Framing added when a provider's take is collected for Ace's synthesis
# (funnel mode) rather than shown to Shavor directly.
FUNNEL_HEADER = (
    "[Your reply goes to Ace — not to Shavor. Ace will synthesize your take "
    "with the other partner's into ONE final answer for Shavor. Be direct, "
    "structured, and complete: lead with your conclusion, then your reasons. "
    "He reads Ace's synthesis, never this text, so make it count.]\n\n")


async def fan_out(job):
    """Ask Grok and Gemini in parallel; optionally run one cross-talk round.

    job is a dict: {text, crosstalk, image, funnel, for_ts}.
    image is an optional (mime_type, base64) tuple — both partners can see
    photos, so a chart screenshot gets two expert reads, not just pixels.
    In funnel mode the takes are saved hidden (never broadcast) for Ace to
    synthesize into one answer; a failing provider leaves a hidden
    error placeholder so the synthesis never waits forever."""
    text, crosstalk = job["text"], job["crosstalk"]
    image = job.get("image")
    funnel = job.get("funnel", False)
    for_ts = job.get("for_ts")
    first = {}

    async def one(name):
        try:
            await room.broadcast({"type": "status", "provider": name,
                                  "state": "thinking"})
            prompt = (FUNNEL_HEADER + text) if funnel else text
            reply = await PROVIDERS[name](prompt, image=image)
            first[name] = reply
            entry = {"from": name, "round": 1, "text": reply,
                     "ts": now_iso()}
            if funnel:
                entry["hidden"] = True
                entry["funnel"] = True
                entry["for_ts"] = for_ts
                await remember(entry)
                await room.broadcast({"type": "status", "provider": name,
                                      "state": "done"})
            else:
                await room.broadcast({"type": "reply", "provider": name,
                                      "round": 1, "text": reply,
                                      "ts": entry["ts"]})
                await remember(entry)
        except Exception as e:  # one provider failing never blocks the other
            err = f"{name} failed: {e}"
            log.warning(err)
            if funnel:
                entry = {"from": name, "round": 1, "text":
                         f"[{name} couldn't be reached — no take.]",
                         "ts": now_iso(), "hidden": True, "funnel": True,
                         "for_ts": for_ts, "error": True}
                await remember(entry)
                await room.broadcast({"type": "status", "provider": name,
                                      "state": "done"})
            else:
                await room.broadcast({"type": "status", "provider": name,
                                      "state": "error", "detail": str(e)})

    await asyncio.gather(*(one(n) for n in PROVIDERS))

    if crosstalk and not funnel and "grok" in first and "gemini" in first:
        async def react(name, other):
            try:
                await room.broadcast({"type": "status", "provider": name,
                                      "state": "reacting"})
                reply = await PROVIDERS[name](
                    text, crosstalk=True, other_take=first[other], image=image)
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
        job = await msg_queue.get()
        try:
            await fan_out(job)
        finally:
            msg_queue.task_done()


@app.on_event("startup")
async def on_startup():
    asyncio.create_task(worker())
    asyncio.create_task(gainers_mod.background_loop())
    # Restore room history from disk — otherwise every redeploy wipes it.
    async with history_lock:
        history.extend(load_history())
        del history[:max(0, len(history) - HISTORY_KEEP)]
        prune_uploads()
        # Restore the shared room memory so the bots keep remembering.
        digest = load_room_memory()
        if digest:
            # Strip the header line the saver writes.
            body = digest.split("\n\n", 1)
            save_room_memory(body[1] if len(body) > 1 else digest)
    if "DESK_TOKEN" not in os.environ:
        log.info("DESK_TOKEN not set — generated per-boot token.")
    log.info("Desk Box up. Open /?token=%s", DESK_TOKEN)


@app.get("/")
async def index(token: str = ""):
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    return FileResponse(
        os.path.join(BASE_DIR, "static", "index.html"),
        headers={"Cache-Control": "no-store"},
    )


@app.get("/api/desk")
async def desk(token: str = ""):
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    return JSONResponse(DESK_CONTEXT)


@app.get("/api/gainers")
async def gainers(token: str = "", source: str = "combined"):
    """Live top-gainers. Webull via public ranking API; Moomoo via OpenD
    when reachable. Cache ~2 minutes. Never invents prices — failures
    return empty items with an honest status/message."""
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    payload = await gainers_mod.get_gainers(source)
    return JSONResponse(payload)


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


@app.post("/api/memory")
async def set_memory(req: Request):
    """Ace pushes the shared room digest here (from the main chat).
    Both bots receive it in their prompts from then on."""
    body = await req.json()
    if body.get("token") != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    digest = (body.get("digest") or "")[:4000]
    async with history_lock:
        save_room_memory(digest)
    return {"ok": True}


@app.get("/api/memory")
async def get_memory(token: str = ""):
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    return room_memory


@app.post("/api/upload")
async def upload(token: str = "", file: UploadFile = File(...)):
    """Photo/video sharing. Stores the file on the durable volume,
    token-gated, pruned with the 14-day retention window."""
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    raw_name = file.filename or ""
    ext = raw_name.rsplit(".", 1)[-1].lower() if "." in raw_name else ""
    if ext in IMAGE_EXTS:
        kind, cap = "image", MAX_IMAGE_BYTES
    elif ext in VIDEO_EXTS:
        kind, cap = "video", MAX_VIDEO_BYTES
    else:
        return JSONResponse(
            {"error": "only photos (jpg/png/gif/webp) and videos (mp4/mov/webm)"},
            status_code=400)
    data = await file.read()
    if not data:
        return JSONResponse({"error": "empty file"}, status_code=400)
    if len(data) > cap:
        return JSONResponse(
            {"error": f"file too large (max {cap // 1024 // 1024} MB)"},
            status_code=400)
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    fname = f"{uuid.uuid4().hex}.{ext}"
    try:
        with open(os.path.join(UPLOAD_DIR, fname), "wb") as f:
            f.write(data)
    except OSError:
        return JSONResponse({"error": "storage failed"}, status_code=500)
    return {"ok": True, "url": f"/uploads/{fname}", "kind": kind,
            "name": raw_name[:120]}


@app.get("/uploads/{fname}")
async def get_upload(fname: str, token: str = ""):
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    safe = os.path.basename(fname)
    if not UPLOAD_RE.match(safe):
        return JSONResponse({"error": "not found"}, status_code=404)
    path = os.path.join(UPLOAD_DIR, safe)
    if not os.path.isfile(path):
        return JSONResponse({"error": "not found"}, status_code=404)
    return FileResponse(path)


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
    attachment, image, kind = resolve_attachment(body.get("attachment"))
    if attachment:
        entry["attachment"] = attachment
    if body.get("funnel_answer") is True:
        # Marks this as the single synthesized answer to a funnel question
        # so the bridge never synthesizes the same question twice.
        entry["funnel_answer"] = True
        entry["for_ts"] = str(body.get("for_ts") or "")
    await remember(entry)
    bcast = {"type": "reply", "provider": "ace",
             "round": 1, "text": text, "ts": entry["ts"]}
    if attachment:
        bcast["attachment"] = attachment
    await room.broadcast(bcast)
    # Two-way discussion: when the caller sets discuss=true, Ace's post is
    # also queued for the Grok/Gemini fan-out so Rail and Anchor respond.
    # Framed so the providers know the speaker is Ace, not Shavor.
    # Loop safety: bot replies are broadcast + logged only — they never go
    # back on msg_queue — so one discuss post yields at most one bot round.
    if body.get("discuss") is True:
        framed = ("[From Ace, your fellow desk partner — respond to him as a "
                  "peer, not as Shavor. Direct and plain.]\n\n" + text)
        if kind == "video":
            framed += "\n\n[Ace shared a video.]"
        await msg_queue.put({"text": framed, "crosstalk": False,
                             "image": image, "funnel": False,
                             "for_ts": entry["ts"]})
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
        recent = list(history[-HISTORY_SEND:])
    await ws.send_text(json.dumps({"type": "history", "messages": recent}))
    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if msg.get("type") == "user" and (msg.get("text", "").strip()
                                             or msg.get("attachment")):
                text = msg.get("text", "").strip()[:4000]
                crosstalk = bool(msg.get("crosstalk"))
                funnel = bool(msg.get("funnel"))
                attachment, image, kind = resolve_attachment(
                    msg.get("attachment"))
                entry = {"from": "shavor", "text": text, "ts": now_iso()}
                if funnel:
                    entry["funnel"] = True
                if attachment:
                    entry["attachment"] = attachment
                await remember(entry)
                bcast = {"type": "user", "text": text, "ts": entry["ts"]}
                if attachment:
                    bcast["attachment"] = attachment
                await room.broadcast(bcast)
                # Bots get the photo itself (vision); video is acknowledged
                # in words since the providers only take images.
                bot_text = text
                if kind == "image" and not bot_text:
                    bot_text = "[Shavor shared a photo — look at it and respond to what you see.]"
                elif kind == "video":
                    bot_text = (bot_text + "\n\n[Shavor shared a video.]"
                                if bot_text else
                                "[Shavor shared a video — acknowledge it.]")
                await msg_queue.put({"text": bot_text, "crosstalk": crosstalk,
                                     "image": image, "funnel": funnel,
                                     "for_ts": entry["ts"]})
    except WebSocketDisconnect:
        pass
    finally:
        await room.remove(ws)
