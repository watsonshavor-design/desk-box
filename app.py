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
import re
import secrets
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, File, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from providers import PROVIDERS
import providers as providers_mod


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
UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")
# Photos ≤10MB, video ≤50MB. jpeg is stored as .jpg.
IMAGE_EXT = {"jpg": "image/jpeg", "png": "image/png", "gif": "image/gif",
             "webp": "image/webp"}
VIDEO_EXT = {"mp4": "video/mp4", "mov": "video/quicktime", "webm": "video/webm"}
IMAGE_MAX = 10 * 1024 * 1024
VIDEO_MAX = 50 * 1024 * 1024
_UPLOAD_NAME = re.compile(
    r"^[a-f0-9]{16}\.(jpg|png|gif|webp|mp4|mov|webm)$")
HISTORY_KEEP = 2000   # in-memory messages per room (14-day window)
HISTORY_SEND = 200    # messages sent to a newly connected UI
RETENTION_DAYS = 14   # chats are never deleted before this; pruned after

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
                if not (isinstance(e, dict) and e.get("from") and e.get("ts")):
                    continue
                if e.get("attachment"):
                    att = normalize_attachment(e["attachment"])
                    if att:
                        e["attachment"] = att
                    else:
                        e.pop("attachment", None)
                if e.get("text") or e.get("attachment"):
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


def _kind_for(ext):
    if ext in IMAGE_EXT:
        return "image"
    if ext in VIDEO_EXT:
        return "video"
    return ""


def _looks_like(ext, blob):
    """Reject a renamed file whose header is not the claimed media type."""
    if ext == "jpg":
        return blob.startswith(b"\xff\xd8\xff")
    if ext == "png":
        return blob.startswith(b"\x89PNG\r\n\x1a\n")
    if ext == "gif":
        return blob.startswith(b"GIF87a") or blob.startswith(b"GIF89a")
    if ext == "webp":
        return (len(blob) >= 12 and blob[:4] == b"RIFF"
                and blob[8:12] == b"WEBP")
    if ext in ("mp4", "mov"):
        return len(blob) >= 12 and blob[4:8] == b"ftyp"
    if ext == "webm":
        return blob.startswith(b"\x1a\x45\xdf\xa3")
    return False


def _clean_display_name(name, fallback):
    base = os.path.basename(str(name or "")).replace("\x00", "").strip()
    return (base[:120] or fallback)


def normalize_attachment(raw):
    """Keep only attachments this server actually stored."""
    if not isinstance(raw, dict):
        return None
    url = str(raw.get("url") or "")
    prefix = "/uploads/"
    if not url.startswith(prefix):
        return None
    filename = url[len(prefix):]
    if not _UPLOAD_NAME.match(filename):
        return None
    ext = filename.rsplit(".", 1)[-1]
    kind = _kind_for(ext)
    if raw.get("kind") != kind:
        return None
    path = os.path.realpath(os.path.join(UPLOAD_DIR, filename))
    root = os.path.realpath(UPLOAD_DIR)
    if not path.startswith(root + os.sep) or not os.path.isfile(path):
        return None
    return {"url": prefix + filename, "kind": kind,
            "name": _clean_display_name(raw.get("name"), filename)}


def prompt_for(text, attachment):
    """Tell the models a file was attached. They never receive the bytes."""
    if not attachment:
        return text
    label = "photo" if attachment["kind"] == "image" else "video"
    note = (f"[Attached {label}: {attachment['name']}. "
            "You cannot see the file; respond to the caption.]")
    return f"{text}\n\n{note}".strip() if text else note


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
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    asyncio.create_task(worker())
    # Restore room history from disk — otherwise every redeploy wipes it.
    async with history_lock:
        history.extend(load_history())
        del history[:max(0, len(history) - HISTORY_KEEP)]
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


def _upload_ext(filename):
    name = os.path.basename(filename or "")
    if "." not in name:
        return ""
    ext = name.rsplit(".", 1)[-1].lower()
    return "jpg" if ext == "jpeg" else ext


@app.post("/api/upload")
async def upload_media(token: str = "", file: UploadFile = File(...)):
    """Store one photo or video. Field name is `file`."""
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    ext = _upload_ext(file.filename or "")
    kind = _kind_for(ext)
    if not kind:
        await file.close()
        return JSONResponse({"error": "unsupported type"}, status_code=400)
    limit = IMAGE_MAX if kind == "image" else VIDEO_MAX
    ctype = (file.content_type or "").split(";")[0].strip().lower()
    expected = IMAGE_EXT[ext] if kind == "image" else VIDEO_EXT[ext]
    allowed_types = {expected, "application/octet-stream", ""}
    if ext == "jpg":
        allowed_types.add("image/jpg")
    if ext == "mov":
        allowed_types.add("video/mp4")
    if ctype not in allowed_types:
        await file.close()
        return JSONResponse({"error": "unsupported type"}, status_code=400)

    os.makedirs(UPLOAD_DIR, exist_ok=True)
    filename = secrets.token_hex(8) + "." + ext
    dest = os.path.join(UPLOAD_DIR, filename)
    size = 0
    header = b""
    too_big = False
    try:
        try:
            with open(dest, "wb") as out:
                while True:
                    chunk = await file.read(1024 * 1024)
                    if not chunk:
                        break
                    if len(header) < 16:
                        header += chunk[:16 - len(header)]
                    size += len(chunk)
                    if size > limit:
                        too_big = True
                        break
                    out.write(chunk)
        except Exception:
            try:
                os.remove(dest)
            except OSError:
                pass
            raise
    finally:
        await file.close()

    if too_big or size == 0 or not _looks_like(ext, header):
        try:
            os.remove(dest)
        except OSError:
            pass
        if too_big:
            return JSONResponse({"error": "file too large"}, status_code=413)
        return JSONResponse({"error": "unsupported type"}, status_code=400)

    return {"ok": True, "url": "/uploads/" + filename, "kind": kind}


@app.get("/uploads/{filename}")
async def serve_upload(filename: str, token: str = ""):
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    if not _UPLOAD_NAME.match(filename):
        return JSONResponse({"error": "not found"}, status_code=404)
    path = os.path.realpath(os.path.join(UPLOAD_DIR, filename))
    root = os.path.realpath(UPLOAD_DIR)
    if not path.startswith(root + os.sep) or not os.path.isfile(path):
        return JSONResponse({"error": "not found"}, status_code=404)
    ext = filename.rsplit(".", 1)[-1]
    media = IMAGE_EXT.get(ext) or VIDEO_EXT.get(ext) or "application/octet-stream"
    return FileResponse(path, media_type=media,
                        headers={"Cache-Control": "private, max-age=3600"})


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
            if msg.get("type") == "user":
                text = str(msg.get("text") or "").strip()[:4000]
                attachment = normalize_attachment(msg.get("attachment"))
                if not text and not attachment:
                    continue
                crosstalk = bool(msg.get("crosstalk"))
                entry = {"from": "shavor", "text": text, "ts": now_iso()}
                if attachment:
                    entry["attachment"] = attachment
                await remember(entry)
                payload = {"type": "user", "text": text, "ts": entry["ts"]}
                if attachment:
                    payload["attachment"] = attachment
                await room.broadcast(payload)
                await msg_queue.put((prompt_for(text, attachment), crosstalk))
    except WebSocketDisconnect:
        pass
    finally:
        await room.remove(ws)
