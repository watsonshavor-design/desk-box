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
import time
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo

import httpx
import hmac
import secrets
import uuid
from urllib.parse import urlencode
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, File, Request, UploadFile, WebSocket, WebSocketDisconnect
from html import escape
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response
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
    "goal": ("$1M trading profit — 100%+/week target with compounding; "
             "flag $15k balance milestone immediately"),
    "glnd_lock": ("GLND: old $7 waiting-sell lock CANCELLED. Exit shares only at $6.18+ "
                  "(profit only — do not sell early below). Confirm live book for shares/calls "
                  "before briefing; do not brief 'Sell GLND at $7'."),
    "risk": ("Standing Hard Limits: KEEP THE ACCOUNT ALIVE. Always keep $2,000–$3,000 settled "
             "withdrawable cash (never deploy below $2k). If account approaches $6,000, alert "
             "Shavor — do NOT liquidate. No position size caps / no 3-name max / no kill switches. "
             "Nothing is banned — any ticker eligible. GLND: exit shares only at $6.18+. "
             "KALA: WATCH ONLY — do not sell without Shavor's explicit word; targets $0.80–$1.00+; "
             "updates/alerts only. Options incl. naked allowed if max loss stated and account-safe. "
             "Ask Shavor when in doubt."),
    "notes": ("General-purpose room: trading, research, learning, daily life — everyone answers, "
              "no hierarchy. Penny stocks allowed. Prefer flat overnight by default. "
              "KALA is watch-only (no sell without Shavor). Night Desk joins via paste relay."),
}

app = FastAPI(title="Desk Box")
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")),
           name="static")

history = []          # list of dicts: {from, provider?, text, ts}
history_lock = asyncio.Lock()
msg_queue = asyncio.Queue()  # one message processed at a time


def now_iso():
    return datetime.now(timezone.utc).isoformat()


_WEATHER_RE = re.compile(
    r"\b(weather|temperature|temp\b|forecast|humid|rain|snow|wind\b|"
    r"how hot|how cold|degrees)\b",
    re.I,
)


def looks_like_weather(text: str) -> bool:
    return bool(_WEATHER_RE.search(text or ""))


def sanitize_location(raw):
    """Accept client lat/lng/city only — never invent coordinates."""
    if not isinstance(raw, dict):
        return None
    try:
        lat = float(raw.get("lat"))
        lng = float(raw.get("lng"))
    except (TypeError, ValueError):
        return None
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lng <= 180.0):
        return None
    out = {"lat": round(lat, 5), "lng": round(lng, 5)}
    city = raw.get("city")
    if isinstance(city, str) and city.strip():
        out["city"] = city.strip()[:80]
    acc = raw.get("accuracy_m")
    if isinstance(acc, (int, float)) and acc >= 0:
        out["accuracy_m"] = round(float(acc), 1)
    return out


async def fetch_open_meteo(lat: float, lng: float) -> dict:
    """Live weather from Open-Meteo. Never invents temperatures on failure."""
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lng}"
        "&current=temperature_2m,apparent_temperature,relative_humidity_2m,"
        "precipitation,weather_code,wind_speed_10m"
        "&temperature_unit=fahrenheit&wind_speed_unit=mph&timezone=auto"
    )
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
        cur = data.get("current") or {}
        if "temperature_2m" not in cur:
            return {"ok": False, "error": "no current observation"}
        return {
            "ok": True,
            "source": "open-meteo",
            "latitude": data.get("latitude", lat),
            "longitude": data.get("longitude", lng),
            "timezone": data.get("timezone"),
            "temperature_f": cur.get("temperature_2m"),
            "feels_like_f": cur.get("apparent_temperature"),
            "humidity_pct": cur.get("relative_humidity_2m"),
            "precipitation": cur.get("precipitation"),
            "weather_code": cur.get("weather_code"),
            "wind_mph": cur.get("wind_speed_10m"),
            "observed_at": cur.get("time"),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


def format_location_weather_block(location, weather=None):
    city = location.get("city") or f"{location['lat']},{location['lng']}"
    lines = [
        f"[Device location: {city} (lat {location['lat']}, lng {location['lng']}).]"
    ]
    if weather and weather.get("ok"):
        lines.append(
            "[Live weather via Open-Meteo — use these numbers; do not invent temps: "
            f"{weather.get('temperature_f')}°F"
            f" (feels {weather.get('feels_like_f')}°F), "
            f"humidity {weather.get('humidity_pct')}%, "
            f"wind {weather.get('wind_mph')} mph, "
            f"precip {weather.get('precipitation')}, "
            f"code {weather.get('weather_code')}, "
            f"as of {weather.get('observed_at')} {weather.get('timezone')}.]"
        )
    elif weather and not weather.get("ok"):
        lines.append(
            "[Live weather fetch failed — say you could not get a reading; "
            "do not invent a temperature. "
            f"Error: {weather.get('error')}]"
        )
    return "\n".join(lines)



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


# Prior room threads live beside the live log on the durable volume.
# Live feed is desk-log.jsonl; archives are never loaded back into history.
ARCHIVE_DIR = os.path.join(DATA_DIR, "archives")
ARCHIVE_INDEX = os.path.join(ARCHIVE_DIR, "index.json")
ARCHIVE_ID_RE = re.compile(r"^feed-\d{8}T\d{6}Z(?:-\d+)?$")


def _load_archive_index():
    try:
        with open(ARCHIVE_INDEX) as f:
            data = json.load(f)
        if isinstance(data, list):
            return [m for m in data if isinstance(m, dict) and m.get("id")]
    except (OSError, json.JSONDecodeError):
        pass
    return []


def _save_archive_index(index):
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    tmp = ARCHIVE_INDEX + ".tmp"
    with open(tmp, "w") as f:
        json.dump(index, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, ARCHIVE_INDEX)


def list_archives():
    """Index plus any jsonl files the index missed. No message bodies."""
    by_id = {}
    for meta in _load_archive_index():
        aid = str(meta.get("id") or "")
        if ARCHIVE_ID_RE.match(aid):
            by_id[aid] = {
                "id": aid,
                "count": int(meta.get("count") or 0),
                "archived_at": meta.get("archived_at") or "",
                "note": meta.get("note") or "",
                "first_ts": meta.get("first_ts") or "",
                "last_ts": meta.get("last_ts") or "",
            }
    try:
        names = os.listdir(ARCHIVE_DIR)
    except OSError:
        names = []
    for name in names:
        if not name.endswith(".jsonl"):
            continue
        aid = name[:-6]
        if not ARCHIVE_ID_RE.match(aid) or aid in by_id:
            continue
        path = os.path.join(ARCHIVE_DIR, name)
        count = 0
        first_ts = last_ts = ""
        try:
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    count += 1
                    try:
                        e = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    ts = e.get("ts") or ""
                    if ts and not first_ts:
                        first_ts = ts
                    if ts:
                        last_ts = ts
        except OSError:
            continue
        by_id[aid] = {
            "id": aid, "count": count, "archived_at": "",
            "note": "", "first_ts": first_ts, "last_ts": last_ts,
        }
    return [by_id[k] for k in sorted(by_id)]


def read_archive(archive_id):
    if not ARCHIVE_ID_RE.match(archive_id or ""):
        return None
    path = os.path.join(ARCHIVE_DIR, archive_id + ".jsonl")
    if not os.path.isfile(path):
        return None
    messages = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(e, dict):
                messages.append(e)
    return messages


def _write_archive(entries, note):
    """Copy entries to a new archive file. Does not touch the live log."""
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    aid = f"feed-{stamp}"
    n = 2
    while os.path.exists(os.path.join(ARCHIVE_DIR, aid + ".jsonl")):
        aid = f"feed-{stamp}-{n}"
        n += 1
    path = os.path.join(ARCHIVE_DIR, aid + ".jsonl")
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        for e in entries:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    meta = {
        "id": aid,
        "count": len(entries),
        "archived_at": now_iso(),
        "note": (note or "")[:200],
        "first_ts": entries[0].get("ts") if entries else "",
        "last_ts": entries[-1].get("ts") if entries else "",
    }
    index = _load_archive_index()
    index.append(meta)
    _save_archive_index(index)
    return meta


def _truncate_live_log():
    tmp = LOG_PATH + ".tmp"
    with open(tmp, "w") as f:
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, LOG_PATH)


async def archive_and_reset(note=""):
    """Copy the live room, then clear it. Archive failure leaves the room."""
    async with history_lock:
        entries = list(history)
        meta = _write_archive(entries, note)
        _truncate_live_log()
        del history[:]
    await room.broadcast({"type": "history", "messages": []})
    return meta


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

# One-answer (funnel) completion tracking. Rail/Anchor takes are hidden;
# Ace (Muse bridge) must POST /api/ace-reply with funnel_answer=true.
# If Ace never posts, a watchdog posts an honest timeout Ace bubble so the
# room never silently drops after "Rail+Anchor briefed."
ACE_FUNNEL_TIMEOUT_SEC = float(os.environ.get("ACE_FUNNEL_TIMEOUT_SEC", "90"))
_pending_funnels = {}  # for_ts -> dict
_pending_funnels_lock = asyncio.Lock()


def _funnel_answered(for_ts):
    """True if Ace already posted a funnel_answer for this question ts."""
    if not for_ts:
        return False
    for m in history:
        if (m.get("from") == "ace"
                and m.get("funnel_answer") is True
                and str(m.get("for_ts") or "") == str(for_ts)):
            return True
    return False


def _format_timeout_ace(question, takes, waited):
    """Honest Ace bubble — no invented market numbers; disclose what we had."""
    names = {"grok": "Rail", "gemini": "Anchor"}
    lines = [
        f"Ace synthesis timed out after {int(waited)}s — no silent drop.",
        "Rail and Anchor briefed backstage, but the Ace bridge did not post "
        "a one-answer synthesis in time. Retest, or ask again.",
    ]
    q = (question or "").strip()
    if q:
        lines.append(f"Question: {q[:240]}")
    for key in ("grok", "gemini"):
        take = (takes or {}).get(key) or {}
        label = names[key]
        body = (take.get("text") or "").strip()
        if take.get("error") or not body:
            lines.append(f"{label}: no usable take.")
        else:
            # Relay partner text only — do not invent prices/facts.
            snippet = body.replace("\n", " ").strip()
            if len(snippet) > 280:
                snippet = snippet[:277] + "…"
            lines.append(f"{label} (brief, not a fresh quote): {snippet}")
    return "\n".join(lines)


async def _post_ace_funnel(for_ts, text, *, timed_out=False):
    """Inject one Ace funnel_answer into the room (idempotent on for_ts)."""
    text = (text or "").strip()[:4000]
    if not text or not for_ts:
        return False
    cancel_task = None
    async with _pending_funnels_lock:
        if _funnel_answered(for_ts):
            pend = _pending_funnels.pop(for_ts, None)
            if pend and pend.get("task"):
                cancel_task = pend["task"]
            posted = False
        else:
            # Claim the slot before releasing the lock so a twin watchdog
            # cannot also post.
            pend = _pending_funnels.pop(for_ts, None)
            if pend and pend.get("task") and pend["task"] is not asyncio.current_task():
                cancel_task = pend["task"]
            posted = True
            entry = {
                "from": "ace",
                "text": text,
                "ts": now_iso(),
                "funnel_answer": True,
                "for_ts": str(for_ts),
            }
            if timed_out:
                entry["timeout"] = True
    if cancel_task is not None:
        cancel_task.cancel()
    if not posted:
        return False
    await remember(entry)
    await room.broadcast({
        "type": "reply",
        "provider": "ace",
        "round": 1,
        "text": text,
        "ts": entry["ts"],
        "funnel_answer": True,
        "for_ts": str(for_ts),
        "timeout": bool(timed_out),
    })
    await room.broadcast({"type": "status", "provider": "ace", "state": "done"})
    return True


async def _funnel_watchdog(for_ts, timeout_sec):
    try:
        await asyncio.sleep(timeout_sec)
    except asyncio.CancelledError:
        return
    async with _pending_funnels_lock:
        pend = _pending_funnels.get(for_ts)
        if not pend or pend.get("done"):
            return
        if _funnel_answered(for_ts):
            _pending_funnels.pop(for_ts, None)
            return
        question = pend.get("question") or ""
        takes = pend.get("takes") or {}
        waited = pend.get("timeout_sec") or timeout_sec
        pend["done"] = True
    msg = _format_timeout_ace(question, takes, waited)
    log.warning("funnel timeout for_ts=%s after %ss — posting Ace timeout bubble",
                for_ts, int(waited))
    await _post_ace_funnel(for_ts, msg, timed_out=True)


async def register_funnel_pending(for_ts, question, takes):
    """Arm Ace synthesis wait + watchdog. Always ends in Ace bubble or timeout."""
    if not for_ts:
        return
    timeout_sec = max(1.0, float(ACE_FUNNEL_TIMEOUT_SEC))
    async with _pending_funnels_lock:
        if _funnel_answered(for_ts):
            return
        old = _pending_funnels.get(for_ts)
        if old and old.get("task"):
            old["task"].cancel()
        task = asyncio.create_task(_funnel_watchdog(for_ts, timeout_sec))
        _pending_funnels[for_ts] = {
            "question": question or "",
            "takes": takes or {},
            "opened_ts": now_iso(),
            "timeout_sec": timeout_sec,
            "task": task,
            "done": False,
        }
    await room.broadcast({
        "type": "status",
        "provider": "ace",
        "state": "thinking",
        "for_ts": for_ts,
        "detail": "synthesizing",
    })


async def complete_funnel_pending(for_ts):
    """Cancel watchdog once Ace posts a real funnel_answer."""
    if not for_ts:
        return
    async with _pending_funnels_lock:
        pend = _pending_funnels.pop(str(for_ts), None) or _pending_funnels.pop(for_ts, None)
        if pend and pend.get("task"):
            pend["task"].cancel()


def list_open_funnels():
    """Snapshot of pending one-answer jobs for the Ace bridge."""
    out = []
    for for_ts, pend in list(_pending_funnels.items()):
        if pend.get("done") or _funnel_answered(for_ts):
            continue
        out.append({
            "for_ts": for_ts,
            "question": pend.get("question") or "",
            "takes": pend.get("takes") or {},
            "opened_ts": pend.get("opened_ts") or "",
            "timeout_sec": pend.get("timeout_sec"),
            "needs_synthesis": True,
            "funnel": True,
        })
    return out


async def fan_out(job):
    """Ask Grok and Gemini in parallel; optionally run one cross-talk round.

    job is a dict: {text, crosstalk, image, funnel, for_ts}.
    image is an optional (mime_type, base64) tuple — both partners can see
    photos, so a chart screenshot gets two expert reads, not just pixels.
    In funnel mode the takes are saved hidden (never broadcast) for Ace to
    synthesize into one answer; a failing provider leaves a hidden
    error placeholder so the synthesis never waits forever. After both
    takes land, the server arms a pending funnel + watchdog so Ace always
    posts a reply or an honest timeout bubble — never a silent drop."""
    text, crosstalk = job["text"], job["crosstalk"]
    image = job.get("image")
    funnel = job.get("funnel", False)
    for_ts = job.get("for_ts")
    first = {}
    takes = {}

    async def one(name):
        await room.broadcast({"type": "status", "provider": name,
                              "state": "thinking"})
        prompt = (FUNNEL_HEADER + text) if funnel else text
        reply = None
        try:
            reply = await PROVIDERS[name](prompt, image=image)
        except Exception as e:  # one provider failing never blocks the other
            # Cloud failure → optional local Ollama fallback before placeholder.
            if name != "local" and "local" in PROVIDERS:
                try:
                    log.warning("%s failed (%s); retrying with local LLM",
                                name, e)
                    reply = await PROVIDERS["local"](prompt, image=image)
                    reply = "[local fallback] " + reply
                except Exception as local_e:
                    log.warning("%s local fallback failed: %s", name, local_e)
                    e = local_e
            if reply is None:
                err = f"{name} failed: {e}"
                log.warning(err)
                err_text = f"[{name} couldn't be reached — no take.]"
                takes[name] = {"text": err_text, "error": True}
                if funnel:
                    entry = {"from": name, "round": 1, "text": err_text,
                             "ts": now_iso(), "hidden": True, "funnel": True,
                             "for_ts": for_ts, "error": True}
                    await remember(entry)
                    await room.broadcast({"type": "status", "provider": name,
                                          "state": "done"})
                else:
                    await room.broadcast({"type": "status", "provider": name,
                                          "state": "error", "detail": str(e)})
                return
        first[name] = reply
        takes[name] = {"text": reply, "error": False}
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

    # Primary fan-out is cloud only; "local" is fallback, not a third seat.
    cloud = [n for n in PROVIDERS if n != "local"]
    await asyncio.gather(*(one(n) for n in cloud))

    if crosstalk and not funnel and "grok" in first and "gemini" in first:
        async def react(name, other):
            await room.broadcast({"type": "status", "provider": name,
                                  "state": "reacting"})
            reply = None
            try:
                reply = await PROVIDERS[name](
                    text, crosstalk=True, other_take=first[other], image=image)
            except Exception as e:
                if "local" in PROVIDERS:
                    try:
                        log.warning("%s cross-talk failed (%s); "
                                    "retrying with local LLM", name, e)
                        reply = await PROVIDERS["local"](
                            text, crosstalk=True, other_take=first[other],
                            image=image)
                        reply = "[local fallback] " + reply
                    except Exception as local_e:
                        log.warning("%s cross-talk local fallback failed: %s",
                                    name, local_e)
                        e = local_e
                if reply is None:
                    log.warning("%s cross-talk failed: %s", name, e)
                    await room.broadcast({"type": "status", "provider": name,
                                          "state": "error", "detail": str(e)})
                    return
            entry = {"from": name, "round": 2, "text": reply,
                     "ts": now_iso()}
            await room.broadcast({"type": "reply", "provider": name,
                                  "round": 2, "text": reply,
                                  "ts": entry["ts"]})
            await remember(entry)

        await asyncio.gather(react("grok", "gemini"), react("gemini", "grok"))

    if funnel and for_ts:
        # Rail+Anchor done (or errored). Wake Ace path + guarantee a bubble.
        await register_funnel_pending(for_ts, text, takes)

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



@app.get("/api/health")
async def health():
    """Liveness only — no token, no secrets."""
    return JSONResponse({"ok": True, "service": "desk-box", "archive": True})

@app.get("/")
async def index(token: str = ""):
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    return FileResponse(
        os.path.join(BASE_DIR, "static", "index.html"),
        headers={"Cache-Control": "no-store"},
    )


# Local desk-exec state. Railway does not have this tree; missing files stay unknown.
DESK_EXEC_ROOT = os.environ.get("DESK_EXEC_ROOT", "/workspace/desk-exec")
DESK_STATUS_STALE_SEC = 180
FLOOR_WATCH_LABEL = "Floor watch $6,000"
JOURNAL_PATH = os.path.join(DATA_DIR, "desk-journal.jsonl")
JOURNAL_MAX = 2000
ECON_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
_journal_lock = asyncio.Lock()
_econ_cache = {"at": 0.0, "ok": False, "events": []}
_OPEN_QUEUE_STATUSES = {"pending", "submitted", "working", "open", "queued"}
_TERMINAL_QUEUE_STATUSES = {
    "filled", "halted", "cancelled", "canceled", "rejected", "expired", "failed",
}


def session_name(now=None):
    """Weekday session clock in America/New_York. Weekends are closed."""
    now = now or datetime.now(ZoneInfo(TAYLORS_TZ))
    if now.tzinfo is None:
        now = now.replace(tzinfo=ZoneInfo(TAYLORS_TZ))
    else:
        now = now.astimezone(ZoneInfo(TAYLORS_TZ))
    if now.weekday() >= 5:
        return "closed"
    minutes = now.hour * 60 + now.minute
    if 4 * 60 <= minutes < 9 * 60 + 30:
        return "pre-market"
    if 9 * 60 + 30 <= minutes < 16 * 60:
        return "regular"
    if 16 * 60 <= minutes < 20 * 60:
        return "after-hours"
    return "closed"


def _desk_state_dir():
    root = os.path.realpath(DESK_EXEC_ROOT)
    state = os.path.realpath(os.path.join(root, "state"))
    if not state.startswith(root + os.sep):
        return None
    if not os.path.isdir(state):
        return None
    return state


def _read_json_file(path):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _parse_et(value):
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(TAYLORS_TZ))
    return parsed.astimezone(ZoneInfo(TAYLORS_TZ))


def _status_age_sec(payload):
    stamps = []
    for key in ("last_watchdog_tick", "updated_at"):
        parsed = _parse_et(payload.get(key) if isinstance(payload, dict) else None)
        if parsed is not None:
            stamps.append(parsed)
    if not stamps:
        return None
    newest = max(stamps)
    return (datetime.now(ZoneInfo(TAYLORS_TZ)) - newest).total_seconds()


def _queue_open_count(root):
    path = os.path.join(root, "queue", "approved_orders.json")
    data = _read_json_file(path)
    if not data or not isinstance(data.get("orders"), list):
        return None
    count = 0
    for order in data["orders"]:
        if not isinstance(order, dict):
            return None
        status = order.get("status")
        if not isinstance(status, str):
            return None
        status = status.strip().lower()
        if status in _OPEN_QUEUE_STATUSES:
            count += 1
        elif status not in _TERMINAL_QUEUE_STATUSES:
            return None
    return count


def _atomic_json(path, payload):
    parent = os.path.dirname(path)
    os.makedirs(parent, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False)
        handle.write("\n")
    os.replace(tmp, path)


def _parse_utc(value):
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _enum(value, allowed, default="unknown"):
    if isinstance(value, str):
        value = value.strip().lower()
        if value in allowed:
            return value
    return default


DESK_STATUS_PATH = os.path.join(DATA_DIR, "desk-trading-status.json")
DESK_ACCOUNT_PATH = os.path.join(DATA_DIR, "desk-account.json")
DESK_ACCOUNT_STALE_SEC = 180
ACCOUNT_FLOOR_USD = 6000.0
DESK_KILL_REQUEST_PATH = os.path.join(DATA_DIR, "desk-kill-request.json")
_DAEMON_STATES = {"alive", "dead", "unknown"}
_OPEND_STATES = {"connected", "down", "unknown"}
_UNLOCK_STATES = {"yes", "no", "unknown"}
_KILL_STATES = {"present", "absent", "unknown"}


def _blank_trading_status():
    return {
        "reachable": False,
        "daemon": "unknown",
        "daemon_as_of": None,
        "opend": "unknown",
        "trade_unlocked": "unknown",
        "open_orders": None,
        "open_orders_label": "unknown",
        "kill_file": "unknown",
        "kill_requested": False,
    }


def _kill_request_state():
    data = _read_json_file(DESK_KILL_REQUEST_PATH)
    if not isinstance(data, dict):
        return {}
    return data


def kill_request_pending():
    data = _kill_request_state()
    if data.get("pending") is True and data.get("acked") is not True:
        rid = data.get("request_id")
        if isinstance(rid, str) and rid:
            return {
                "pending": True,
                "request_id": rid,
                "requested_at": data.get("requested_at") if isinstance(data.get("requested_at"), str) else None,
            }
    return {"pending": False}


def record_kill_request():
    """Remember a second-tap halt. Does not touch positions or the broker."""
    current = kill_request_pending()
    if current.get("pending"):
        return {
            "ok": True,
            "written": False,
            "requested": True,
            "already": True,
            "request_id": current["request_id"],
        }
    payload = {
        "pending": True,
        "acked": False,
        "request_id": uuid.uuid4().hex,
        "requested_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        _atomic_json(DESK_KILL_REQUEST_PATH, payload)
    except OSError:
        return {"ok": False, "error": "could not record kill request", "written": False, "requested": False}
    return {
        "ok": True,
        "written": False,
        "requested": True,
        "already": False,
        "request_id": payload["request_id"],
    }


def ack_kill_request(request_id):
    if not isinstance(request_id, str) or not request_id.strip():
        return {"ok": False, "error": "request_id required"}
    request_id = request_id.strip()
    data = _kill_request_state()
    if data.get("request_id") != request_id or data.get("pending") is not True:
        return {"ok": False, "error": "no matching request"}
    data["pending"] = False
    data["acked"] = True
    data["acked_at"] = datetime.now(timezone.utc).isoformat()
    try:
        _atomic_json(DESK_KILL_REQUEST_PATH, data)
    except OSError:
        return {"ok": False, "error": "could not ack kill request"}
    return {"ok": True, "request_id": request_id}


def normalize_trading_push(body):
    """Keep only status facts. Drop equity, P/L, and any token field."""
    if not isinstance(body, dict):
        return None
    open_orders = body.get("open_orders")
    if isinstance(open_orders, bool) or not isinstance(open_orders, int) or open_orders < 0:
        open_orders = None
    as_of = body.get("daemon_as_of")
    if not isinstance(as_of, str) or len(as_of) > 80:
        as_of = None
    source = body.get("open_orders_source")
    if open_orders is None or not isinstance(source, str) or source.strip() != "desk queue":
        source = None
    return {
        "reachable": True,
        "daemon": _enum(body.get("daemon"), _DAEMON_STATES),
        "daemon_as_of": as_of,
        "opend": _enum(body.get("opend"), _OPEND_STATES),
        "trade_unlocked": _enum(body.get("trade_unlocked"), _UNLOCK_STATES),
        "open_orders": open_orders,
        "open_orders_label": "unknown" if open_orders is None else str(open_orders),
        "open_orders_source": source,
        "kill_file": _enum(body.get("kill_file"), _KILL_STATES),
        "pushed_at": datetime.now(timezone.utc).isoformat(),
        "source": "desk-exec",
    }


def store_trading_push(payload):
    _atomic_json(DESK_STATUS_PATH, payload)


def _pushed_trading_status():
    data = _read_json_file(DESK_STATUS_PATH)
    if not data:
        return None
    pushed = _parse_utc(data.get("pushed_at"))
    if pushed is None:
        return None
    age = (datetime.now(timezone.utc) - pushed).total_seconds()
    if age < 0 or age > DESK_STATUS_STALE_SEC:
        return None
    clean = normalize_trading_push(data)
    if clean is None:
        return None
    clean["pushed_at"] = data.get("pushed_at")
    clean["source"] = "desk-exec"
    return clean


def desk_trading_status():
    """Local desk files when this server has them, otherwise the last fresh push.

    Never invents equity or P/L. A stale or missing push stays unknown.
    """
    state = _desk_state_dir()
    unknown = {
        "reachable": False,
        "daemon": "unknown",
        "daemon_as_of": None,
        "opend": "unknown",
        "trade_unlocked": "unknown",
        "open_orders": None,
        "open_orders_label": "unknown",
        "kill_file": "unknown",
    }
    if state is None:
        return unknown
    root = os.path.dirname(state)
    kill_path = os.path.join(state, "KILL")
    kill_present = os.path.isfile(kill_path) and not os.path.islink(kill_path)
    payload = _read_json_file(os.path.join(state, "daemon_status.json"))
    age = _status_age_sec(payload) if payload else None
    fresh = age is not None and age <= DESK_STATUS_STALE_SEC
    if payload is None or age is None:
        daemon = "unknown"
    elif fresh:
        daemon = "alive"
    else:
        daemon = "dead"
    as_of = None
    if isinstance(payload, dict):
        as_of = payload.get("last_watchdog_tick") or payload.get("updated_at")
        if not isinstance(as_of, str):
            as_of = None
    if not fresh or not isinstance(payload, dict) or not isinstance(payload.get("opend_ok"), bool):
        opend = "unknown"
    else:
        opend = "connected" if payload["opend_ok"] else "down"
    if kill_present:
        unlocked = "no"
    elif fresh and isinstance(payload, dict):
        unlock = payload.get("unlock_present")
        live = payload.get("LIVE_TRADING_ENABLED")
        halted = payload.get("halted_for_kill")
        if unlock is True and live is True and halted is False:
            unlocked = "yes"
        elif unlock is False or live is False or halted is True:
            unlocked = "no"
        else:
            unlocked = "unknown"
    else:
        unlocked = "unknown"
    open_count = _queue_open_count(root)
    status = {
        "reachable": True,
        "daemon": daemon,
        "daemon_as_of": as_of,
        "opend": opend,
        "trade_unlocked": unlocked,
        "open_orders": open_count,
        "open_orders_label": "unknown" if open_count is None else str(open_count),
        "open_orders_source": None if open_count is None else "desk queue",
        "kill_file": "present" if kill_present else "absent",
        "source": "local",
    }
    return _with_kill_request(status)


def _with_kill_request(status):
    out = _blank_trading_status()
    if isinstance(status, dict):
        out.update(status)
    out["kill_requested"] = bool(kill_request_pending().get("pending"))
    return out


def desk_trading_status_public():
    state = _desk_state_dir()
    if state is not None:
        return desk_trading_status()
    pushed = _pushed_trading_status()
    if pushed is None:
        return _with_kill_request(_blank_trading_status())
    return _with_kill_request(pushed)


def _money(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed or parsed in (float("inf"), float("-inf")):
        return None
    return parsed


def _blank_account(reason=None):
    return {
        "book_loaded": False,
        "available": False,
        "equity": None,
        "cash": None,
        "pnl_today": None,
        "distance_to_floor": None,
        "floor_usd": ACCOUNT_FLOOR_USD,
        "floor_label": FLOOR_WATCH_LABEL,
        "positions": [],
        "working_orders": None,
        "as_of": None,
        "reason": reason,
        "source": None,
    }


def normalize_account_push(body):
    """Keep book facts only. Drop the token. Never invent equity."""
    if not isinstance(body, dict):
        return None
    equity = _money(body.get("equity"))
    cash = _money(body.get("cash"))
    distance = _money(body.get("distance_to_floor"))
    available = body.get("available") is True and equity is not None and cash is not None
    if available and distance is None:
        distance = equity - ACCOUNT_FLOOR_USD
    reason = body.get("reason")
    if available or not isinstance(reason, str):
        reason = None
    else:
        reason = reason.strip()[:180] or None
    as_of = body.get("as_of")
    if not isinstance(as_of, str) or len(as_of) > 80:
        as_of = None
    positions = []
    raw_positions = body.get("positions")
    if available and isinstance(raw_positions, list):
        for row in raw_positions[:12]:
            if not isinstance(row, dict):
                continue
            code = row.get("code")
            if not isinstance(code, str) or not code.strip() or len(code) > 24:
                continue
            positions.append({
                "code": code.strip(),
                "qty": _money(row.get("qty")),
                "market_val": _money(row.get("market_val")),
                "pl_val": _money(row.get("pl_val")),
                "nominal_price": _money(row.get("nominal_price")),
            })
    working = body.get("working_orders")
    if isinstance(working, bool) or not isinstance(working, int) or working < 0:
        working = None
    return {
        "available": available,
        "book_loaded": available,
        "equity": equity if available else None,
        "cash": cash if available else None,
        "pnl_today": None,
        "market_val": _money(body.get("market_val")) if available else None,
        "distance_to_floor": distance if available else None,
        "floor_usd": ACCOUNT_FLOOR_USD,
        "floor_label": FLOOR_WATCH_LABEL,
        "positions": positions if available else [],
        "working_orders": working if available else None,
        "as_of": as_of,
        "reason": reason,
        "pushed_at": datetime.now(timezone.utc).isoformat(),
        "source": "opend",
    }


def store_account_push(payload):
    _atomic_json(DESK_ACCOUNT_PATH, payload)


def desk_account_snapshot():
    """Last fresh OpenD push. Missing or stale stays unloaded — no invented NAV."""
    data = _read_json_file(DESK_ACCOUNT_PATH)
    if not data:
        return _blank_account("no snapshot yet")
    pushed = _parse_utc(data.get("pushed_at"))
    if pushed is None:
        return _blank_account("snapshot has no time")
    age = (datetime.now(timezone.utc) - pushed).total_seconds()
    if age < 0 or age > DESK_ACCOUNT_STALE_SEC:
        return _blank_account("snapshot stale")
    clean = normalize_account_push(data)
    if clean is None:
        return _blank_account("snapshot unreadable")
    clean["pushed_at"] = data.get("pushed_at")
    if not clean.get("available"):
        blank = _blank_account(clean.get("reason") or "book unavailable")
        blank["as_of"] = clean.get("as_of")
        blank["pushed_at"] = data.get("pushed_at")
        return blank
    return clean


def write_kill_file():
    """Create the same state/KILL halt file. Does not cancel or place orders."""
    state = _desk_state_dir()
    if state is None:
        return {"ok": False, "error": "desk state is not on this server", "written": False}
    path = os.path.join(state, "KILL")
    if os.path.islink(path):
        return {"ok": False, "error": "kill path is not a regular file", "written": False}
    if os.path.isfile(path):
        return {"ok": True, "written": False, "already": True}
    stamp = datetime.now(ZoneInfo(TAYLORS_TZ)).isoformat()
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags, 0o644)
    except FileExistsError:
        return {"ok": True, "written": False, "already": True}
    except OSError:
        return {"ok": False, "error": "could not write kill file", "written": False}
    try:
        os.write(fd, (stamp + "\n").encode("utf-8"))
    finally:
        os.close(fd)
    return {"ok": True, "written": True, "already": False}


def _journal_rows(limit=20):
    if not os.path.isfile(JOURNAL_PATH):
        return []
    rows = []
    try:
        with open(JOURNAL_PATH, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(item, dict) and isinstance(item.get("text"), str) and isinstance(item.get("ts"), str):
                    rows.append({"ts": item["ts"], "text": item["text"]})
    except OSError:
        return []
    return rows[-limit:]


async def _fetch_usd_econ_today(client):
    now = time.time()
    if _econ_cache["ok"] and (now - _econ_cache["at"]) < 600:
        return True, list(_econ_cache["events"])
    try:
        resp = await client.get(ECON_CALENDAR_URL, timeout=8.0, headers={"User-Agent": "desk-box"})
        resp.raise_for_status()
        data = resp.json()
    except (httpx.HTTPError, ValueError, json.JSONDecodeError):
        return False, []
    if not isinstance(data, list):
        return False, []
    today = datetime.now(ZoneInfo(TAYLORS_TZ)).date()
    events = []
    for item in data:
        if not isinstance(item, dict):
            continue
        if item.get("country") != "USD":
            continue
        when = _parse_et(item.get("date"))
        title = item.get("title")
        if when is None or when.date() != today or not isinstance(title, str) or not title.strip():
            continue
        events.append({
            "title": title.strip()[:160],
            "time_et": when.strftime("%-I:%M %p ET"),
            "impact": item.get("impact") if isinstance(item.get("impact"), str) else "",
            "_sort": when.isoformat(),
        })
    events.sort(key=lambda row: row["_sort"])
    for row in events:
        row.pop("_sort", None)
    _econ_cache["at"] = now
    _econ_cache["ok"] = True
    _econ_cache["events"] = events
    return True, events


@app.get("/api/desk")
async def desk(token: str = ""):
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    return JSONResponse(DESK_CONTEXT)


@app.get("/api/desk/trading")
async def desk_trading(token: str = ""):
    if not _token_match(token):
        return JSONResponse({"error": "bad token"}, status_code=403)
    return JSONResponse(desk_trading_status_public())


@app.get("/api/desk/account")
async def desk_account(token: str = ""):
    if not _token_match(token):
        return JSONResponse({"error": "bad token"}, status_code=403)
    return JSONResponse(desk_account_snapshot())


@app.get("/api/desk/morning")
async def desk_morning(token: str = ""):
    if not _token_match(token):
        return JSONResponse({"error": "bad token"}, status_code=403)
    now = datetime.now(ZoneInfo(TAYLORS_TZ))
    async with httpx.AsyncClient() as client:
        loaded, events = await _fetch_usd_econ_today(client)
    return JSONResponse({
        "session": session_name(now),
        "as_of": now.isoformat(),
        "timezone": TAYLORS_TZ,
        "econ_loaded": loaded,
        "econ_events": events if loaded else [],
        "movers_loaded": False,
        "movers": [],
        "movers_note": "none loaded",
    })


@app.post("/api/desk/kill")
async def desk_kill(request: Request, token: str = ""):
    """Second-step halt. Writes state/KILL only. Does not touch positions or orders."""
    if not _token_match(token):
        return JSONResponse({"error": "bad token"}, status_code=403)
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict) or body.get("confirm") != "KILL":
        return JSONResponse({"ok": False, "error": "confirm required"}, status_code=400)
    result = write_kill_file()
    if (
        not result.get("ok")
        and result.get("error") == "desk state is not on this server"
    ):
        # Railway cannot see the desk tree. Record the request; the local
        # watcher creates state/KILL. Still no orders and no liquidation.
        result = record_kill_request()
    status = 200 if result.get("ok") else 409
    return JSONResponse(result, status_code=status)


def _desk_token_from(request, body=None):
    if isinstance(body, dict) and _token_match(body.get("token") or ""):
        return True
    return _token_match(request.query_params.get("token") or "")


@app.post("/api/desk/account/ingest")
async def desk_account_ingest(request: Request):
    """Local OpenD book push. Token in JSON or query, never logged."""
    try:
        body = await request.json()
    except Exception:
        body = None
    if not isinstance(body, dict) or not _desk_token_from(request, body):
        return JSONResponse({"error": "bad token"}, status_code=403)
    payload = normalize_account_push(body)
    if payload is None:
        return JSONResponse({"error": "bad payload"}, status_code=400)
    try:
        store_account_push(payload)
    except OSError:
        return JSONResponse({"ok": False, "error": "could not store"}, status_code=500)
    public = desk_account_snapshot()
    return JSONResponse({
        "ok": True,
        "book_loaded": public.get("book_loaded"),
        "available": public.get("available"),
        "equity": public.get("equity"),
        "cash": public.get("cash"),
        "distance_to_floor": public.get("distance_to_floor"),
    })


@app.post("/api/desk/trading/ingest")
async def desk_trading_ingest(request: Request):
    """Local desk pushes heartbeat facts. Token in JSON or query, never logged."""
    try:
        body = await request.json()
    except Exception:
        body = None
    if not isinstance(body, dict) or not _desk_token_from(request, body):
        return JSONResponse({"error": "bad token"}, status_code=403)
    payload = normalize_trading_push(body)
    if payload is None:
        return JSONResponse({"error": "bad payload"}, status_code=400)
    try:
        store_trading_push(payload)
    except OSError:
        return JSONResponse({"ok": False, "error": "could not store"}, status_code=500)
    public = desk_trading_status_public()
    return JSONResponse({
        "ok": True,
        "daemon": public.get("daemon"),
        "opend": public.get("opend"),
        "trade_unlocked": public.get("trade_unlocked"),
        "open_orders": public.get("open_orders"),
        "kill_file": public.get("kill_file"),
        "reachable": public.get("reachable"),
    })


@app.get("/api/desk/kill/pending")
async def desk_kill_pending(token: str = ""):
    if not _token_match(token):
        return JSONResponse({"error": "bad token"}, status_code=403)
    return JSONResponse(kill_request_pending())


@app.post("/api/desk/kill/ack")
async def desk_kill_ack(request: Request):
    try:
        body = await request.json()
    except Exception:
        body = None
    if not isinstance(body, dict) or not _desk_token_from(request, body):
        return JSONResponse({"error": "bad token"}, status_code=403)
    result = ack_kill_request(body.get("request_id"))
    status = 200 if result.get("ok") else 409
    return JSONResponse(result, status_code=status)


@app.get("/api/journal")
async def journal_list(token: str = ""):
    if not _token_match(token):
        return JSONResponse({"error": "bad token"}, status_code=403)
    async with _journal_lock:
        notes = _journal_rows()
    return JSONResponse({"notes": notes})


@app.post("/api/journal")
async def journal_add(request: Request, token: str = ""):
    if not _token_match(token):
        return JSONResponse({"error": "bad token"}, status_code=403)
    try:
        body = await request.json()
    except Exception:
        body = {}
    text_in = body.get("text") if isinstance(body, dict) else None
    if not isinstance(text_in, str):
        return JSONResponse({"ok": False, "error": "text required"}, status_code=400)
    note = " ".join(text_in.replace("\r", " ").split())
    if not note:
        return JSONResponse({"ok": False, "error": "text required"}, status_code=400)
    if len(note) > JOURNAL_MAX:
        return JSONResponse({"ok": False, "error": "too long"}, status_code=400)
    row = {"ts": datetime.now(ZoneInfo(TAYLORS_TZ)).isoformat(), "text": note}
    async with _journal_lock:
        try:
            with open(JOURNAL_PATH, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
        except OSError:
            return JSONResponse({"ok": False, "error": "could not save"}, status_code=500)
    return JSONResponse({"ok": True, "note": row})


@app.get("/api/weather")
async def weather(token: str = "", lat: float = 0.0, lng: float = 0.0):
    """Live Open-Meteo reading for Ace / room. No invented temperatures."""
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    location = sanitize_location({"lat": lat, "lng": lng})
    if not location:
        return JSONResponse({"ok": False, "error": "lat/lng required"}, status_code=400)
    reading = await fetch_open_meteo(location["lat"], location["lng"])
    return JSONResponse({"location": location, "weather": reading})


# Taylors, SC — Home feed cards. Town coordinates, not a device ping.
TAYLORS_LAT = 34.9204
TAYLORS_LNG = -82.2962
TAYLORS_TZ = "America/New_York"
_HOME_TTL_SEC = 300
_home_cache = {"at": 0.0, "payload": None}
_MRSS = "{http://search.yahoo.com/mrss/}"
_WMO = {
    0: "Clear", 1: "Mostly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Freezing fog",
    51: "Light drizzle", 53: "Drizzle", 55: "Heavy drizzle",
    56: "Freezing drizzle", 57: "Freezing drizzle",
    61: "Light rain", 63: "Rain", 65: "Heavy rain",
    66: "Freezing rain", 67: "Freezing rain",
    71: "Light snow", 73: "Snow", 75: "Heavy snow", 77: "Snow grains",
    80: "Light showers", 81: "Showers", 82: "Heavy showers",
    85: "Snow showers", 86: "Heavy snow showers",
    95: "Thunderstorm", 96: "Thunderstorm", 99: "Thunderstorm",
}
_HEADLINE_FEEDS = (
    ("https://feeds.bbci.co.uk/news/rss.xml", "BBC News"),
    ("https://feeds.npr.org/1001/rss.xml", "NPR"),
    ("https://www.theguardian.com/world/rss", "The Guardian"),
)
_HEADLINE_CAP = 60
_HEADLINE_PER_FEED = 40
# Same diamond-in-a-circle as the installed icon. Original fill sampled from icon-192.
_MARK_BG = (13, 17, 23)
_MARK_ORIGINAL = "#a371f7"
_MARK_COLORS = {
    "original": _MARK_ORIGINAL,
    "green": "#34d399",
    "teal": "#2dd4bf",
    "violet": "#8b5cf6",
    "mint": "#6ee7b7",
    "indigo": "#818cf8",
    "cyan": "#22d3ee",
    "aurora": "#10b981",
    "lilac": "#c4b5fd",
    "sea": "#5eead4",
    "emerald": "#059669",
    "dusk": "#7c3aed",
    "lagoon": "#14b8a6",
}
# Window 0 of every 30-day cycle is the original mark. Later windows recolor it.
# Odd cycles use a second aurora palette after the return to original.
_MARK_PALETTES = (
    ("original", "green", "teal", "violet", "mint", "indigo", "cyan"),
    ("original", "aurora", "lilac", "sea", "emerald", "dusk", "lagoon"),
)
_MARK_EPOCH = datetime(2026, 1, 1, tzinfo=ZoneInfo(TAYLORS_TZ)).date()
_MARK_CYCLE_DAYS = 30
_MARK_WINDOW_DAYS = 4  # stable 3–5 day hold; 4 keeps one image across that window
_YOUTUBE_CLIENT_ENVS = (
    "YOUTUBE_CLIENT_ID",
    "GOOGLE_CLIENT_ID",
    "GOOGLE_OAUTH_CLIENT_ID",
    "YOUTUBE_OAUTH_CLIENT_ID",
)


def _fmt_sunset_local(raw):
    """Open-Meteo sunset is local wall time, e.g. 2026-10-02T19:11."""
    if not isinstance(raw, str) or "T" not in raw:
        return None
    try:
        hh, mm = raw.split("T", 1)[1][:5].split(":")
        h = int(hh)
        m = int(mm)
    except (ValueError, IndexError):
        return None
    ampm = "AM" if h < 12 else "PM"
    h12 = h % 12 or 12
    return f"{h12}:{m:02d} {ampm}"


def _precip_now(hourly):
    times = (hourly or {}).get("time") or []
    probs = (hourly or {}).get("precipitation_probability") or []
    if not times or not probs:
        return None
    now = datetime.now(ZoneInfo(TAYLORS_TZ)).strftime("%Y-%m-%dT%H:00")
    idx = 0
    if now in times:
        idx = times.index(now)
    else:
        for i, t in enumerate(times):
            if t <= now:
                idx = i
            else:
                break
    try:
        val = probs[idx]
        if val is None:
            return None
        return int(round(float(val)))
    except (IndexError, TypeError, ValueError):
        return None


def _strip_html(raw):
    text = re.sub(r"<[^>]+>", " ", raw or "")
    text = re.sub(r"\s+", " ", text)
    for a, b in (
        ("&amp;", "&"), ("&quot;", '"'), ("&#39;", "'"), ("&apos;", "'"),
        ("&lt;", "<"), ("&gt;", ">"), ("&nbsp;", " "),
    ):
        text = text.replace(a, b)
    return text.strip()


def _parse_rss_headlines(xml_bytes, source_name, limit=8):
    root = ET.fromstring(xml_bytes)
    out = []
    for item in root.iter("item"):
        title = _strip_html(item.findtext("title") or "")
        link = (item.findtext("link") or "").strip()
        if not title or not link.startswith("https://"):
            continue
        summary = _strip_html(item.findtext("description") or "")
        if summary.lower() == title.lower():
            summary = ""
        image = None
        thumb = item.find(f"{_MRSS}thumbnail")
        if thumb is not None:
            url = (thumb.attrib.get("url") or "").strip()
            if url.startswith("https://"):
                image = url
        if not image:
            enc = item.find("enclosure")
            if enc is not None:
                url = (enc.attrib.get("url") or "").strip()
                typ = (enc.attrib.get("type") or "")
                if url.startswith("https://") and typ.startswith("image/"):
                    image = url
        row = {"title": title[:240], "source": source_name, "link": link[:500]}
        if summary:
            row["summary"] = summary[:280]
        if image:
            row["image"] = image[:500]
        out.append(row)
        if len(out) >= limit:
            break
    return out


def _merge_headline_rows(batches, cap=_HEADLINE_CAP):
    """Round-robin real feed rows. Drops duplicate links. Does not invent titles."""
    seen = set()
    merged = []
    longest = max((len(b) for b in batches), default=0)
    for i in range(longest):
        for batch in batches:
            if i >= len(batch):
                continue
            item = batch[i]
            key = item.get("link") or ""
            if not key or key in seen:
                continue
            seen.add(key)
            merged.append(item)
            if len(merged) >= cap:
                return merged
    return merged


async def _fetch_headlines(client):
    async def one(url, source_name):
        try:
            r = await client.get(url, headers={"User-Agent": "DeskBox/1.0"})
            r.raise_for_status()
            return _parse_rss_headlines(r.content, source_name, limit=_HEADLINE_PER_FEED)
        except Exception:
            return []

    batches = await asyncio.gather(*[one(url, name) for url, name in _HEADLINE_FEEDS])
    merged = _merge_headline_rows(list(batches))
    if merged:
        return {"ok": True, "items": merged, "count": len(merged)}
    return {"ok": False, "items": [], "error": "no feed"}


async def build_home_payload(force=False):
    now_m = time.monotonic()
    cached = _home_cache.get("payload")
    if cached and not force and (now_m - float(_home_cache.get("at") or 0)) < _HOME_TTL_SEC:
        return cached

    lat, lng = TAYLORS_LAT, TAYLORS_LNG
    forecast_url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lng}"
        "&current=temperature_2m,weather_code"
        "&hourly=precipitation_probability"
        "&daily=sunset"
        "&temperature_unit=fahrenheit"
        "&timezone=America%2FNew_York"
        "&forecast_days=2"
    )
    aqi_url = (
        "https://air-quality-api.open-meteo.com/v1/air-quality"
        f"?latitude={lat}&longitude={lng}"
        "&current=us_aqi"
        "&timezone=America%2FNew_York"
    )
    weather = {"ok": False}
    sunset = {"ok": False}
    headlines = {"ok": False, "items": []}
    async with httpx.AsyncClient(timeout=12.0, follow_redirects=True) as client:
        forecast_res, aqi_res, headlines = await asyncio.gather(
            client.get(forecast_url),
            client.get(aqi_url),
            _fetch_headlines(client),
            return_exceptions=True,
        )
    if isinstance(headlines, Exception):
        headlines = {"ok": False, "items": []}
    if not isinstance(forecast_res, Exception):
        try:
            forecast_res.raise_for_status()
            data = forecast_res.json()
        except Exception:
            data = None
        if isinstance(data, dict):
            cur = data.get("current") or {}
            temp = cur.get("temperature_2m")
            if temp is not None:
                try:
                    code_i = int(cur.get("weather_code"))
                except (TypeError, ValueError):
                    code_i = None
                aqi = None
                if not isinstance(aqi_res, Exception):
                    try:
                        aqi_res.raise_for_status()
                        raw_aqi = (aqi_res.json().get("current") or {}).get("us_aqi")
                        if raw_aqi is not None:
                            aqi = int(round(float(raw_aqi)))
                    except Exception:
                        aqi = None
                weather = {
                    "ok": True,
                    "place": "Taylors",
                    "temperature_f": temp,
                    "condition": _WMO.get(code_i, "Unknown") if code_i is not None else "Unknown",
                    "precip_chance_pct": _precip_now(data.get("hourly") or {}),
                    "aqi": aqi,
                    "observed_at": cur.get("time"),
                }
            suns = ((data.get("daily") or {}).get("sunset") or [None])[0]
            # Prefer today's sunset. If the first entry is already past, still show it
            # (the card is "sunset today"). Open-Meteo daily[0] is the local date.
            label = _fmt_sunset_local(suns) if isinstance(suns, str) else None
            if label:
                sunset = {"ok": True, "label": label, "at": suns, "tz": TAYLORS_TZ}

    payload = {
        "place": "Taylors",
        "weather": weather,
        "sunset": sunset,
        "headlines": (headlines.get("items") or [])[:_HEADLINE_CAP] if isinstance(headlines, dict) else [],
        "headlines_ok": bool(isinstance(headlines, dict) and headlines.get("ok")),
    }
    if weather.get("ok") or sunset.get("ok") or payload["headlines_ok"]:
        _home_cache["at"] = time.monotonic()
        _home_cache["payload"] = payload
    return payload


@app.get("/api/home")
async def home_feed(token: str = ""):
    """Desk Home: Taylors weather, sunset, and public headlines.

    Auth is the room token as a query param. Does not invent readings.
    """
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    return JSONResponse(await build_home_payload())


def icon_phase(on=None):
    """Stable mark phase. Same image for a 4-day window. Original every 30 days.

    A later 30-day cycle uses the other aurora palette after it lands on original.
    """
    if on is None:
        on = datetime.now(ZoneInfo(TAYLORS_TZ)).date()
    days = (on - _MARK_EPOCH).days
    if days < 0:
        days = 0
    cycle = days // _MARK_CYCLE_DAYS
    day_in = days % _MARK_CYCLE_DAYS
    slot = day_in // _MARK_WINDOW_DAYS
    palette = _MARK_PALETTES[cycle % len(_MARK_PALETTES)]
    if slot == 0:
        variant = "original"
        twist = "plain"
    else:
        accents = palette[1:]
        variant = accents[(slot - 1) % len(accents)]
        twist = ("ring", "spark", "plain")[slot % 3]
    color = _MARK_COLORS[variant]
    until = 0 if day_in == 0 else _MARK_CYCLE_DAYS - day_in
    # October overlay sits on the same 4-day color. It does not replace the palette
    # or the return to the original purple at the start of each 30-day cycle.
    season = "october" if on.month == 10 else ""
    return {
        "variant": variant,
        "label": "Original" if variant == "original" else variant.title(),
        "color": color,
        "twist": twist,
        "season": season,
        "cycle": cycle,
        "day_in_cycle": day_in,
        "window_days": _MARK_WINDOW_DAYS,
        "cycle_days": _MARK_CYCLE_DAYS,
        "days_until_original": until,
        "palette": cycle % len(_MARK_PALETTES),
    }


def _hex_rgb(value):
    h = value.lstrip("#")
    return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4))


def _october_leaf_svg():
    """Small fall accent. Does not replace the diamond or its 4-day fill."""
    return (
        '<path fill="#ea580c" d="M48.6 9.4c1.7 2.1.7 4.2-.7 5.2 1.6.2 2.7 1.6 2.1 3.2'
        '-1.5.7-3-.3-3.8-1.4-.9 1.4-2.3 1.7-3.3.4.7-1.6.2-3.1 1.3-3.9'
        '-1.5-.8-1.6-2.9-.1-3.7 1.5.1 2.5.9 3.7.9.3-.7.6-1.1.8-.7z"/>'
    )


def mark_svg(phase=None):
    phase = phase or icon_phase()
    color = phase["color"]
    twist = phase["twist"]
    ring = ""
    spark = ""
    if twist == "ring":
        ring = f'<circle cx="32" cy="32" r="26.5" fill="none" stroke="{color}" stroke-width="2.4"/>'
    elif twist == "spark":
        spark = f'<circle cx="50" cy="13" r="3.6" fill="{color}"/>'
    leaf = _october_leaf_svg() if phase.get("season") == "october" else ""
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64" role="img" aria-label="Desk">'
        '<rect width="64" height="64" rx="14" fill="#0d1117"/>'
        f"{ring}"
        f'<circle cx="32" cy="32" r="22" fill="{color}"/>'
        '<path d="M32 18 46 32 32 46 18 32Z" fill="#0d1117"/>'
        f"{spark}{leaf}"
        "</svg>"
    )


def _png_chunk(tag, data):
    import struct
    import zlib
    return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)


def mark_png(size, phase=None):
    import struct
    import zlib
    phase = phase or icon_phase()
    size = int(size)
    if size >= 256:
        size = 512
    elif 150 <= size <= 180:
        size = 180
    else:
        size = 192
    fill = _hex_rgb(phase["color"])
    bg = _MARK_BG
    twist = phase["twist"]
    cx = (size - 1) / 2
    cy = cx
    radius = size * 0.34
    diamond = size * 0.20
    spark_r = size * 0.055
    sx = cx + radius * 0.62
    sy = cy - radius * 1.22
    rows = []
    for y in range(size):
        row = bytearray()
        for x in range(size):
            dx = x - cx
            dy = y - cy
            dist = (dx * dx + dy * dy) ** 0.5
            man = abs(dx) + abs(dy)
            px = bg
            if twist == "ring" and abs(dist - radius * 1.16) <= max(1.4, size * 0.015):
                px = fill
            if dist <= radius:
                px = fill
            if man <= diamond:
                px = bg
            if twist == "spark" and (x - sx) ** 2 + (y - sy) ** 2 <= spark_r ** 2:
                px = fill
            if phase.get("season") == "october":
                lx = cx + radius * 0.72
                ly = cy - radius * 1.18
                if abs(x - lx) * 0.9 + abs(y - ly) * 1.35 <= size * 0.055:
                    px = (234, 88, 12)
            row += bytes((px[0], px[1], px[2], 255))
        rows.append(bytes(row))
    raw = b"".join(b"\x00" + row for row in rows)
    ihdr = struct.pack(">IIBBBBB", size, size, 8, 6, 0, 0, 0)
    return (
        b"\x89PNG\r\n\x1a\n"
        + _png_chunk(b"IHDR", ihdr)
        + _png_chunk(b"IDAT", zlib.compress(raw, 9))
        + _png_chunk(b"IEND", b"")
    )


_mark_png_cache = {}


YOUTUBE_REDIRECT_URI = (
    os.environ.get("YOUTUBE_REDIRECT_URI")
    or "https://desk-box-production.up.railway.app/api/youtube/callback"
).strip()
YOUTUBE_SCOPE = "https://www.googleapis.com/auth/youtube.readonly"
YOUTUBE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
YOUTUBE_TOKEN_URL = "https://oauth2.googleapis.com/token"
YOUTUBE_PLAYLISTS_URL = "https://www.googleapis.com/youtube/v3/playlists"
YOUTUBE_TOKEN_PATH = os.path.join(DATA_DIR, "youtube-refresh.json")
YOUTUBE_STATE_PATH = os.path.join(DATA_DIR, "youtube-oauth-state.json")
_YOUTUBE_STATE_TTL = 600


def _token_match(got):
    if not isinstance(got, str) or not got:
        return False
    try:
        return hmac.compare_digest(got.encode(), DESK_TOKEN.encode())
    except Exception:
        return False


def _google_oauth_creds():
    """Client id and secret from env only. Never log or return these to the browser JSON."""
    client_id = ""
    for name in _YOUTUBE_CLIENT_ENVS:
        client_id = (os.environ.get(name) or "").strip()
        if client_id:
            break
    client_secret = (
        (os.environ.get("GOOGLE_CLIENT_SECRET") or "").strip()
        or (os.environ.get("YOUTUBE_CLIENT_SECRET") or "").strip()
    )
    return client_id, client_secret


def _secret_read(path):
    try:
        with open(path) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _secret_write(path, payload):
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    tmp = f"{path}.{os.getpid()}.tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(payload, fh)
            fh.write("\n")
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
        os.chmod(path, 0o600)
    except Exception:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def _youtube_configured():
    client_id, client_secret = _google_oauth_creds()
    return bool(client_id and client_secret)


def _youtube_connected():
    saved = _secret_read(YOUTUBE_TOKEN_PATH)
    return bool((saved.get("refresh_token") or "").strip())


def _save_oauth_state(state):
    now = time.time()
    data = _secret_read(YOUTUBE_STATE_PATH)
    items = data.get("states") if isinstance(data.get("states"), list) else []
    fresh = []
    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            at = float(item.get("at") or 0)
        except (TypeError, ValueError):
            continue
        if now - at < _YOUTUBE_STATE_TTL and item.get("state"):
            fresh.append({"state": item["state"], "at": at})
    fresh.append({"state": state, "at": now})
    _secret_write(YOUTUBE_STATE_PATH, {"states": fresh[-8:]})


def _consume_oauth_state(state):
    if not isinstance(state, str) or not state:
        return False
    now = time.time()
    data = _secret_read(YOUTUBE_STATE_PATH)
    items = data.get("states") if isinstance(data.get("states"), list) else []
    keep = []
    ok = False
    for item in items:
        if not isinstance(item, dict):
            continue
        try:
            at = float(item.get("at") or 0)
        except (TypeError, ValueError):
            continue
        if now - at >= _YOUTUBE_STATE_TTL:
            continue
        saved = item.get("state") or ""
        if (not ok) and saved and hmac.compare_digest(saved, state):
            ok = True
            continue
        if saved:
            keep.append({"state": saved, "at": at})
    try:
        _secret_write(YOUTUBE_STATE_PATH, {"states": keep})
    except OSError:
        pass
    return ok


def _youtube_authorize_url(state):
    client_id, _secret = _google_oauth_creds()
    query = urlencode({
        "client_id": client_id,
        "redirect_uri": YOUTUBE_REDIRECT_URI,
        "response_type": "code",
        "scope": YOUTUBE_SCOPE,
        "state": state,
        "access_type": "offline",
        "prompt": "consent",
        "include_granted_scopes": "false",
    })
    return f"{YOUTUBE_AUTH_URL}?{query}"


def _store_youtube_tokens(body, previous=None):
    previous = previous or {}
    refresh = (body.get("refresh_token") or previous.get("refresh_token") or "").strip()
    access = (body.get("access_token") or "").strip()
    if not refresh:
        return False
    try:
        expires_in = int(body.get("expires_in") or 3600)
    except (TypeError, ValueError):
        expires_in = 3600
    payload = {
        "refresh_token": refresh,
        "access_token": access,
        "expires_at": time.time() + max(0, expires_in),
        "scope": YOUTUBE_SCOPE,
    }
    _secret_write(YOUTUBE_TOKEN_PATH, payload)
    return True


async def _youtube_access_token(client):
    saved = _secret_read(YOUTUBE_TOKEN_PATH)
    access = (saved.get("access_token") or "").strip()
    try:
        exp = float(saved.get("expires_at") or 0)
    except (TypeError, ValueError):
        exp = 0
    if access and exp > time.time() + 60:
        return access, None
    refresh = (saved.get("refresh_token") or "").strip()
    client_id, client_secret = _google_oauth_creds()
    if not refresh:
        return None, "not_connected"
    if not client_id or not client_secret:
        return None, "not_configured"
    try:
        resp = await client.post(
            YOUTUBE_TOKEN_URL,
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh,
                "grant_type": "refresh_token",
            },
            timeout=20,
        )
    except Exception:
        log.info("youtube token refresh failed")
        return None, "refresh_failed"
    if resp.status_code != 200:
        log.info("youtube token refresh rejected status=%s", resp.status_code)
        return None, "refresh_failed"
    try:
        body = resp.json()
    except Exception:
        return None, "refresh_failed"
    if not isinstance(body, dict) or not _store_youtube_tokens(body, saved):
        return None, "refresh_failed"
    return (body.get("access_token") or "").strip() or None, None


def _playlist_row(row):
    if not isinstance(row, dict):
        return None
    playlist_id = row.get("id")
    snippet = row.get("snippet") if isinstance(row.get("snippet"), dict) else {}
    title = snippet.get("title")
    if not isinstance(playlist_id, str) or not playlist_id:
        return None
    if not isinstance(title, str) or not title.strip():
        return None
    item = {
        "id": playlist_id[:80],
        "title": title.strip()[:200],
        "url": f"https://www.youtube.com/playlist?list={playlist_id}",
    }
    thumbs = snippet.get("thumbnails") if isinstance(snippet.get("thumbnails"), dict) else {}
    for key in ("medium", "high", "default", "standard", "maxres"):
        thumb = thumbs.get(key)
        if not isinstance(thumb, dict):
            continue
        url = thumb.get("url")
        if isinstance(url, str) and url.startswith("https://"):
            item["thumbnail"] = url[:500]
            break
    return item


async def _fetch_youtube_playlists(client, access):
    rows = []
    page = None
    for _ in range(4):
        params = {"part": "snippet", "mine": "true", "maxResults": "50"}
        if page:
            params["pageToken"] = page
        try:
            resp = await client.get(
                YOUTUBE_PLAYLISTS_URL,
                params=params,
                headers={"Authorization": f"Bearer {access}"},
                timeout=20,
            )
        except Exception:
            log.info("youtube playlist fetch failed")
            return None
        if resp.status_code != 200:
            log.info("youtube playlist fetch status=%s", resp.status_code)
            return None
        try:
            body = resp.json()
        except Exception:
            return None
        if not isinstance(body, dict):
            return None
        for raw in body.get("items") or []:
            item = _playlist_row(raw)
            if item:
                rows.append(item)
        page = body.get("nextPageToken")
        if not isinstance(page, str) or not page:
            break
    return rows


async def youtube_status():
    """Connection state and real playlists only. Never returns OAuth secrets."""
    configured = _youtube_configured()
    if not configured:
        return {
            "configured": False,
            "connected": False,
            "playlists": [],
            "blocker": "google_oauth_client_id",
        }
    if not _youtube_connected():
        return {
            "configured": True,
            "connected": False,
            "playlists": [],
            "blocker": None,
        }
    async with httpx.AsyncClient() as client:
        access, err = await _youtube_access_token(client)
        if not access:
            return {
                "configured": True,
                "connected": False,
                "playlists": [],
                "blocker": err or "not_connected",
            }
        playlists = await _fetch_youtube_playlists(client, access)
    if playlists is None:
        return {
            "configured": True,
            "connected": True,
            "playlists": [],
            "blocker": "youtube_api",
        }
    return {
        "configured": True,
        "connected": True,
        "playlists": playlists,
        "blocker": None,
    }


async def _desk_authorized(request: Request, token: str = ""):
    if _token_match(token):
        return True
    if _token_match(request.query_params.get("token") or ""):
        return True
    if request.method == "POST":
        try:
            body = await request.json()
        except Exception:
            body = None
        if isinstance(body, dict) and _token_match(body.get("token") or ""):
            return True
    return False


@app.get("/api/theme")
async def theme_mark():
    """Public icon phase. No token and no secrets — color and window only."""
    return JSONResponse(icon_phase(), headers={"Cache-Control": "public, max-age=1800"})


@app.get("/icon.svg")
async def icon_svg():
    phase = icon_phase()
    return Response(
        content=mark_svg(phase),
        media_type="image/svg+xml",
        headers={"Cache-Control": "public, max-age=1800"},
    )


@app.get("/icon-mark.png")
async def icon_mark_png(size: int = 192):
    phase = icon_phase()
    if size >= 256:
        size = 512
    elif 150 <= size <= 180:
        size = 180
    else:
        size = 192
    key = (size, phase["variant"], phase["twist"], phase["cycle"], phase.get("season") or "")
    png = _mark_png_cache.get(key)
    if png is None:
        png = mark_png(size, phase)
        _mark_png_cache[key] = png
    return Response(content=png, media_type="image/png", headers={"Cache-Control": "public, max-age=1800"})


@app.get("/manifest.webmanifest")
async def web_manifest():
    phase = icon_phase()
    theme_color = "#171022" if phase["variant"] == "original" else phase["color"]
    body = {
        "name": "Desk — live room",
        "short_name": "Desk",
        "description": "Shavor trading desk: Grok + Gemini + Ace bridge",
        "start_url": "/",
        "scope": "/",
        "display": "standalone",
        "background_color": "#0d1117",
        "theme_color": theme_color,
        "orientation": "portrait-primary",
        "icons": [
            {"src": "/icon-mark.png?size=192", "sizes": "192x192", "type": "image/png", "purpose": "any"},
            {"src": "/icon-mark.png?size=512", "sizes": "512x512", "type": "image/png", "purpose": "any"},
            {"src": "/icon.svg", "sizes": "any", "type": "image/svg+xml", "purpose": "any"},
        ],
    }
    return JSONResponse(body, media_type="application/manifest+json", headers={"Cache-Control": "public, max-age=1800"})


@app.get("/api/youtube")
async def youtube_tab(token: str = ""):
    """Real playlists after OAuth. Empty until the account is connected."""
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    return JSONResponse(await youtube_status())


@app.api_route("/api/youtube/start", methods=["GET", "POST"])
async def youtube_start(request: Request, token: str = ""):
    """Send the browser to Google OAuth. Room token is query or JSON, never Bearer."""
    if not await _desk_authorized(request, token):
        return JSONResponse({"error": "bad token"}, status_code=403)
    if not _youtube_configured():
        return JSONResponse({"configured": False, "error": "not_configured"}, status_code=503)
    state = secrets.token_urlsafe(24)
    try:
        _save_oauth_state(state)
    except OSError:
        log.info("youtube oauth state could not be stored")
        return JSONResponse({"error": "state_store"}, status_code=500)
    log.info("youtube oauth start")
    return RedirectResponse(_youtube_authorize_url(state), status_code=302)


@app.get("/api/youtube/callback")
async def youtube_callback(code: str = "", state: str = "", error: str = ""):
    """Exchange the Google code and store the refresh token under DESK_DATA_DIR."""
    if error or not code or not state or not _consume_oauth_state(state):
        log.info("youtube oauth callback rejected")
        return RedirectResponse("/?yt=denied#youtube", status_code=302)
    client_id, client_secret = _google_oauth_creds()
    if not client_id or not client_secret:
        return RedirectResponse("/?yt=error#youtube", status_code=302)
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                YOUTUBE_TOKEN_URL,
                data={
                    "code": code,
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "redirect_uri": YOUTUBE_REDIRECT_URI,
                    "grant_type": "authorization_code",
                },
                timeout=20,
            )
    except Exception:
        log.info("youtube oauth exchange failed")
        return RedirectResponse("/?yt=error#youtube", status_code=302)
    if resp.status_code != 200:
        log.info("youtube oauth exchange status=%s", resp.status_code)
        return RedirectResponse("/?yt=error#youtube", status_code=302)
    try:
        body = resp.json()
    except Exception:
        return RedirectResponse("/?yt=error#youtube", status_code=302)
    if not isinstance(body, dict):
        return RedirectResponse("/?yt=error#youtube", status_code=302)
    scope = body.get("scope") or ""
    if isinstance(scope, str) and scope.strip():
        allowed = {
            "https://www.googleapis.com/auth/youtube.readonly",
            "youtube.readonly",
        }
        parts = scope.split()
        if not parts or any(part not in allowed for part in parts):
            log.info("youtube oauth scope rejected")
            return RedirectResponse("/?yt=error#youtube", status_code=302)
    try:
        stored = _store_youtube_tokens(body, _secret_read(YOUTUBE_TOKEN_PATH))
    except OSError:
        log.info("youtube refresh token could not be stored")
        return RedirectResponse("/?yt=error#youtube", status_code=302)
    if not stored:
        log.info("youtube oauth returned no refresh token")
        return RedirectResponse("/?yt=error#youtube", status_code=302)
    log.info("youtube oauth connected")
    return RedirectResponse("/?yt=connected#youtube", status_code=302)


DEFAULT_MAPS_ORIGIN = "347 S Pinecroft Dr, Taylors, SC 29687"
_MAP_QUERY_LIMIT = 240


def _maps_api_key():
    """Server env only. Never log this value."""
    return (os.environ.get("GOOGLE_MAPS_API_KEY") or "").strip()


def _clean_map_text(value):
    if not isinstance(value, str):
        return ""
    return " ".join(value.split())[:_MAP_QUERY_LIMIT]


_MAP_FRAME = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="referrer" content="origin">
<title>Map</title>
<style>
  html, body { margin: 0; height: 100%; background: #ece6f5; color: #21172d; font: 15px/1.45 sans-serif; }
  #map { height: 100%; width: 100%; }
  #wait, #err { margin: 0; padding: 16px 18px; }
  #err { color: #b64855; font-weight: 700; }
  #note { margin: 0; padding: 0 18px 12px; color: #6d6179; font-size: 13px; }
</style>
</head>
<body>
<p id="wait">Finding that place…</p>
<p id="err" hidden></p>
<p id="note" hidden></p>
<div id="map" hidden></div>
<script>window.__DESK_MAP = __MAP_CFG__;</script>
<script>
(function(){
  var cfg = window.__DESK_MAP || {};
  var waitEl = document.getElementById('wait');
  var errEl = document.getElementById('err');
  var noteEl = document.getElementById('note');
  var mapEl = document.getElementById('map');
  function tell(ok, error, extra){
    var msg = {source: 'desk-map', ok: !!ok, error: error || ''};
    extra = extra || {};
    if (typeof extra.durationSec === 'number' && isFinite(extra.durationSec) && extra.durationSec >= 0) {
      msg.durationSec = extra.durationSec;
    }
    if (extra.durationText) msg.durationText = String(extra.durationText).slice(0, 48);
    try {
      parent.postMessage(msg, location.origin);
    } catch (e) {}
  }
  function fail(msg){
    window.__deskMapStarted = true;
    waitEl.hidden = true;
    mapEl.hidden = true;
    noteEl.hidden = true;
    errEl.hidden = false;
    errEl.textContent = msg;
    tell(false, msg);
  }
  window.gm_authFailure = function(){
    fail('Google Maps refused this site.');
  };
  setTimeout(function(){
    if (!window.__deskMapStarted) fail('Google Maps did not load.');
  }, 15000);
  function geocode(geocoder, address){
    return new Promise(function(resolve){
      geocoder.geocode({address: address}, function(results, status){
        if (status === 'OK' && results && results[0] && results[0].geometry && results[0].geometry.location) {
          var loc = results[0].geometry.location;
          var lat = loc.lat();
          var lng = loc.lng();
          if (typeof lat === 'number' && typeof lng === 'number' && isFinite(lat) && isFinite(lng)) {
            resolve({ok: true, ll: loc, formatted: results[0].formatted_address || ''});
            return;
          }
        }
        resolve({ok: false, status: status || 'UNKNOWN'});
      });
    });
  }
  function geoError(which, status){
    if (status === 'REQUEST_DENIED') return 'Google Maps refused to look up that ' + which + '.';
    return 'Could not find that ' + which + '.';
  }
  window.deskInitMap = function(){
    window.__deskMapStarted = true;
    var geocoder = new google.maps.Geocoder();
    geocode(geocoder, cfg.origin).then(function(origin){
      if (!origin.ok) {
        fail(geoError('start address', origin.status));
        return null;
      }
      if (!cfg.destination) return {origin: origin, dest: null};
      return geocode(geocoder, cfg.destination).then(function(dest){
        if (!dest.ok) {
          fail(geoError('destination', dest.status));
          return null;
        }
        return {origin: origin, dest: dest};
      });
    }).then(function(places){
      if (!places) return;
      waitEl.hidden = true;
      mapEl.hidden = false;
      var center = places.dest ? places.dest.ll : places.origin.ll;
      var map = new google.maps.Map(mapEl, {
        center: center,
        zoom: places.dest ? 12 : 16,
        mapTypeControl: false,
        streetViewControl: false,
        fullscreenControl: !cfg.compact,
        gestureHandling: 'greedy'
      });
      if (!places.dest) {
        new google.maps.Marker({map: map, position: places.origin.ll, title: places.origin.formatted || cfg.origin});
        tell(true, '');
        return;
      }
      var renderer = new google.maps.DirectionsRenderer({map: map, suppressMarkers: false});
      new google.maps.DirectionsService().route({
        origin: places.origin.ll,
        destination: places.dest.ll,
        travelMode: google.maps.TravelMode.DRIVING
      }, function(result, status){
        if (status === 'OK' && result) {
          renderer.setDirections(result);
          var leg = result.routes && result.routes[0] && result.routes[0].legs && result.routes[0].legs[0];
          var extra = {};
          if (leg && leg.duration && typeof leg.duration.value === 'number' && isFinite(leg.duration.value)) {
            extra.durationSec = leg.duration.value;
            extra.durationText = leg.duration.text || '';
          }
          tell(true, '', extra);
          return;
        }
        new google.maps.Marker({map: map, position: places.origin.ll, title: places.origin.formatted || cfg.origin});
        new google.maps.Marker({map: map, position: places.dest.ll, title: places.dest.formatted || cfg.destination});
        var bounds = new google.maps.LatLngBounds();
        bounds.extend(places.origin.ll);
        bounds.extend(places.dest.ll);
        map.fitBounds(bounds);
        noteEl.hidden = false;
        noteEl.textContent = 'No driving route. Showing the two places.';
        tell(true, '');
      });
    }).catch(function(){
      fail('The map could not be drawn.');
    });
  };
})();
</script>
<script async defer src="https://maps.googleapis.com/maps/api/js?key=__MAP_KEY__&amp;callback=deskInitMap&amp;v=weekly&amp;loading=async"></script>
</body>
</html>
"""


def _maps_frame_document(origin, destination, compact):
    key = _maps_api_key()
    cfg = json.dumps(
        {"origin": origin, "destination": destination, "compact": bool(compact)},
        ensure_ascii=True,
    ).replace("<", "\\u003c")
    return (
        _MAP_FRAME
        .replace("__MAP_CFG__", cfg)
        .replace("__MAP_KEY__", escape(key, quote=True))
    )


@app.get("/api/maps/status")
async def maps_status(token: str = ""):
    if not _token_match(token):
        return JSONResponse({"error": "bad token"}, status_code=403)
    return JSONResponse({"configured": bool(_maps_api_key())})


@app.get("/api/maps/frame")
async def maps_frame(token: str = "", origin: str = "", destination: str = "", compact: str = ""):
    """Token-gated map page. The Maps JavaScript key is injected here only.

    Geocoding REST is unusable with this referrer-locked key, so the frame
    geocodes in the browser and refuses to draw a pin when lookup fails.
    """
    if not _token_match(token):
        return JSONResponse({"error": "bad token"}, status_code=403)
    headers = {
        "Cache-Control": "no-store",
        "Referrer-Policy": "origin",
        "X-Content-Type-Options": "nosniff",
    }
    if not _maps_api_key():
        page = (
            "<!doctype html><html><head><meta charset='utf-8'>"
            "<meta name='referrer' content='origin'></head><body>"
            "<p style='font:15px sans-serif;padding:16px;color:#b64855'>"
            "Google Maps key is not set on the server."
            "</p></body></html>"
        )
        return HTMLResponse(page, headers=headers)
    start = _clean_map_text(origin) or DEFAULT_MAPS_ORIGIN
    dest = _clean_map_text(destination)
    page = _maps_frame_document(start, dest, compact in {"1", "true", "yes"})
    return HTMLResponse(page, headers=headers)


@app.get("/api/gainers")
async def gainers(
    token: str = "",
    source: str = "combined",
    force: str = "",
    list: str = "",
    list_type: str = "",
):
    """Live top-gainers by source + session list.

    Webull via public ranking API (rankType 1d/preMarket/afterMarket);
    Moomoo via local OpenD ingest (preferred) or OpenAPI/OpenD fallback
    for today's board. list / list_type = premarket|today|afterhours
    (default today). force=1 bypasses cache. Never invents prices.
    """
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    do_force = str(force).strip().lower() in ("1", "true", "yes", "refresh")
    lt_raw = (list_type or list or "today").strip() or "today"
    try:
        payload = await asyncio.wait_for(
            gainers_mod.get_gainers(source, force=do_force, list_type=lt_raw),
            timeout=16.0,
        )
    except asyncio.TimeoutError:
        payload = {
            "ok": False,
            "source": (source or "combined").strip().lower() or "combined",
            "list_type": gainers_mod.normalize_list_type(lt_raw),
            "label": "Feed timed out",
            "items": [],
            "updated_at": now_iso(),
            "status": "error",
            "message": (
                "Gainers request timed out — try Refresh again. "
                "Showing no invented prices."
            ),
        }
    return JSONResponse(payload)


@app.post("/api/gainers/ingest")
async def gainers_ingest(request: Request):
    """Accept local OpenD top-gainers push into Moomoo cache by list_type.

    Railway cannot reach OpenD on the trading box (127.0.0.1:11111), so a
    5-minute loop on that box POSTs here. Token-authed. Does not touch the
    Webull live path.

    Single body:
      {"token": "...", "source": "moomoo", "list_type": "today",
       "items": [...], "status"?, "message"?, "updated_at"?, ...}

    Batch body (preferred for the three-session pusher):
      {"token": "...", "source": "moomoo",
       "lists": [{"list_type": "premarket", "items": [...]}, ...]}
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid json"}, status_code=400)
    if not isinstance(body, dict) or body.get("token") != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    source = str(body.get("source") or "moomoo").strip().lower() or "moomoo"
    if source not in ("moomoo", "webull"):
        return JSONResponse({"error": "source must be moomoo or webull"}, status_code=400)

    # Batch: three session lists in one POST
    lists = body.get("lists")
    if isinstance(lists, list) and lists:
        cleaned = []
        for entry in lists:
            if not isinstance(entry, dict):
                continue
            items = entry.get("items")
            if not isinstance(items, list):
                items = []
            if len(items) > gainers_mod.GAINERS_LIST_CAP:
                items = items[:gainers_mod.GAINERS_LIST_CAP]
            cleaned.append({**entry, "items": items})
        results = gainers_mod.ingest_gainers_batch(source, cleaned)
        return JSONResponse({
            "ok": True,
            "source": source,
            "count": sum(r.get("count") or 0 for r in results),
            "results": results,
            "updated_at": now_iso(),
        })

    items = body.get("items")
    if items is None:
        return JSONResponse(
            {"error": "items required (list) or lists required (batch)"},
            status_code=400,
        )
    if not isinstance(items, list):
        return JSONResponse({"error": "items must be a list"}, status_code=400)
    if len(items) > gainers_mod.GAINERS_LIST_CAP:
        items = items[:gainers_mod.GAINERS_LIST_CAP]
    lt = body.get("list_type") or body.get("list") or "today"
    stored = gainers_mod.ingest_gainers(
        source,
        items,
        list_type=str(lt),
        status=body.get("status"),
        message=body.get("message"),
        updated_at=body.get("updated_at"),
        auth=body.get("auth"),
        opend=body.get("opend"),
        endpoint=body.get("endpoint"),
        reason=body.get("reason"),
    )
    return JSONResponse({
        "ok": True,
        "source": source,
        "list_type": stored.get("list_type") or gainers_mod.normalize_list_type(str(lt)),
        "count": len(stored.get("items") or []),
        "status": stored.get("status"),
        "message": stored.get("message"),
        "updated_at": stored.get("updated_at"),
        "auth": stored.get("auth"),
    })


# --- Ace / CoS bridges ----------------------------------------------------
# There is no API for Muse, so Ace joins the room through these endpoints:
#   GET  /api/ace-inbox?token=...&since=<iso-ts> -> {"messages": [...]}
#       Returns Shavor's @ace mentions newer than `since`, PLUS open
#       one-answer (funnel) questions that still need Ace synthesis
#       (marked needs_synthesis / funnel, with partner takes attached).
#   GET  /api/ace-funnel?token=... -> {"pending": [...]}
#       Open funnel jobs only (question + takes + for_ts).
#   POST /api/ace-reply  {"token": ..., "text": ..., "discuss"?: bool,
#                         "funnel_answer"?: bool, "for_ts"?: ...}
#       Injects Ace's reply into the room (broadcast + session log).
#       funnel_answer+for_ts completes the pending one-answer job and
#       cancels the watchdog. Duplicate funnel_answer for the same for_ts
#       is rejected. When discuss=true, Ace's post is ALSO queued for the
#       Grok/Gemini fan-out (single round). Bot replies never re-enter
#       the queue, so a discuss post yields at most one bot round.
# Funnel harden: after Rail+Anchor brief, a server watchdog posts an
# honest Ace timeout bubble if synthesis never arrives — never silent drop.
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
        # Attach open funnel jobs Ace still owes a synthesis for.
        # Include even if older than `since` so a busy poller cannot miss
        # a one-answer question that never got an Ace bubble.
        open_by_ts = {p["for_ts"]: p for p in list_open_funnels()}
        seen = {m.get("ts") for m in msgs}
        for m in history:
            ts = m.get("ts")
            if m.get("from") != "shavor" or not m.get("funnel"):
                continue
            if ts not in open_by_ts:
                continue
            if ts in seen:
                # Enrich an @ace+funnel hit with takes.
                for i, existing in enumerate(msgs):
                    if existing.get("ts") == ts:
                        enriched = dict(existing)
                        enriched["needs_synthesis"] = True
                        enriched["funnel"] = True
                        enriched["takes"] = open_by_ts[ts].get("takes") or {}
                        msgs[i] = enriched
                        break
                continue
            payload = dict(m)
            payload["needs_synthesis"] = True
            payload["funnel"] = True
            payload["takes"] = open_by_ts[ts].get("takes") or {}
            msgs.append(payload)
            seen.add(ts)
    msgs.sort(key=lambda m: m.get("ts") or "")
    return JSONResponse({"messages": msgs})


@app.get("/api/ace-funnel")
async def ace_funnel(token: str = ""):
    """Open one-answer jobs waiting for Ace synthesis."""
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    return JSONResponse({"pending": list_open_funnels(),
                         "timeout_sec": ACE_FUNNEL_TIMEOUT_SEC})


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
        for_ts = str(body.get("for_ts") or "")
        entry["funnel_answer"] = True
        entry["for_ts"] = for_ts
        async with _pending_funnels_lock:
            if for_ts and _funnel_answered(for_ts):
                return JSONResponse(
                    {"error": "already answered", "for_ts": for_ts},
                    status_code=409)
            # Drop pending so the watchdog cannot also post while we write.
            pend = (_pending_funnels.pop(for_ts, None)
                    if for_ts else None)
            cancel_task = pend.get("task") if pend else None
        if cancel_task is not None:
            cancel_task.cancel()
    await remember(entry)
    bcast = {"type": "reply", "provider": "ace",
             "round": 1, "text": text, "ts": entry["ts"]}
    if attachment:
        bcast["attachment"] = attachment
    if entry.get("funnel_answer"):
        bcast["funnel_answer"] = True
        bcast["for_ts"] = entry.get("for_ts") or ""
    await room.broadcast(bcast)
    if entry.get("funnel_answer"):
        await room.broadcast({"type": "status", "provider": "ace",
                              "state": "done"})
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


# Chief of Staff (CoS) posts into the room the same way Ace does.
# POST /api/cos-reply {"token": ..., "text": ..., "discuss"?: bool}
#   from=cos, broadcast provider=cos. discuss=true queues one Rail/Anchor
#   fan-out round framed as from CoS (never re-enters the queue).
@app.post("/api/cos-reply")
async def cos_reply(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "bad body"}, status_code=400)
    if body.get("token") != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    text = str(body.get("text", "")).strip()[:4000]
    if not text:
        return JSONResponse({"error": "empty"}, status_code=400)
    entry = {"from": "cos", "text": text, "ts": now_iso()}
    attachment, image, kind = resolve_attachment(body.get("attachment"))
    if attachment:
        entry["attachment"] = attachment
    await remember(entry)
    bcast = {"type": "reply", "provider": "cos",
             "round": 1, "text": text, "ts": entry["ts"]}
    if attachment:
        bcast["attachment"] = attachment
    await room.broadcast(bcast)
    if body.get("discuss") is True:
        framed = ("[From Chief of Staff (CoS), your fellow desk partner — "
                  "respond as a peer, not as Shavor. Direct and plain.]\n\n"
                  + text)
        if kind == "video":
            framed += "\n\n[CoS shared a video.]"
        await msg_queue.put({"text": framed, "crosstalk": False,
                             "image": image, "funnel": False,
                             "for_ts": entry["ts"]})
    return JSONResponse({"ok": True})


@app.post("/api/archive-feed")
async def archive_feed(request: Request, token: str = ""):
    """Copy the live room onto the volume, then start an empty live feed."""
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}
    if (body.get("token") or token) != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    note = str(body.get("note") or "")[:200]
    try:
        meta = await archive_and_reset(note)
    except OSError:
        log.warning("archive-feed failed")
        return JSONResponse({"error": "archive failed"}, status_code=500)
    return JSONResponse({"ok": True, **meta})


@app.get("/api/archives")
async def archives(token: str = ""):
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    return JSONResponse({"archives": list_archives()})


@app.get("/api/archive/{archive_id}")
async def archive_one(archive_id: str, token: str = ""):
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    messages = read_archive(archive_id)
    if messages is None:
        return JSONResponse({"error": "not found"}, status_code=404)
    return JSONResponse({"id": archive_id, "messages": messages})


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
                location = sanitize_location(msg.get("location"))
                entry = {"from": "shavor", "text": text, "ts": now_iso()}
                if funnel:
                    entry["funnel"] = True
                if attachment:
                    entry["attachment"] = attachment
                if location:
                    entry["location"] = location
                await remember(entry)
                bcast = {"type": "user", "text": text, "ts": entry["ts"]}
                if attachment:
                    bcast["attachment"] = attachment
                if location:
                    bcast["location"] = location
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
                if location:
                    weather = None
                    if looks_like_weather(text):
                        weather = await fetch_open_meteo(
                            location["lat"], location["lng"])
                    bot_text = (
                        format_location_weather_block(location, weather)
                        + "\n\n" + bot_text
                    )
                elif looks_like_weather(text):
                    bot_text = (
                        "[No device location was provided. Do not invent a "
                        "temperature, city, or forecast. Tell Shavor you need "
                        "location enabled on the phone, or ask for a city name.]"
                        "\n\n" + bot_text
                    )
                await msg_queue.put({"text": bot_text, "crosstalk": crosstalk,
                                     "image": image, "funnel": funnel,
                                     "for_ts": entry["ts"]})
    except WebSocketDisconnect:
        pass
    finally:
        await room.remove(ws)
