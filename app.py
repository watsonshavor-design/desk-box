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
import secrets
import uuid
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, File, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, Response
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


@app.get("/api/desk")
async def desk(token: str = ""):
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    return JSONResponse(DESK_CONTEXT)


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
    return {
        "variant": variant,
        "label": "Original" if variant == "original" else variant.title(),
        "color": color,
        "twist": twist,
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
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64" role="img" aria-label="Desk">'
        '<rect width="64" height="64" rx="14" fill="#0d1117"/>'
        f"{ring}"
        f'<circle cx="32" cy="32" r="22" fill="{color}"/>'
        '<path d="M32 18 46 32 32 46 18 32Z" fill="#0d1117"/>'
        f"{spark}"
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


def youtube_status():
    """True only when a public OAuth client id is configured. Never returns the value."""
    configured = False
    for name in _YOUTUBE_CLIENT_ENVS:
        if (os.environ.get(name) or "").strip():
            configured = True
            break
    if configured:
        return {
            "configured": True,
            "playlists": [],
            "blocker": None,
        }
    return {
        "configured": False,
        "playlists": [],
        "blocker": "google_oauth_client_id",
    }


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
    key = (size, phase["variant"], phase["twist"], phase["cycle"])
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
    """Playlist shell. Empty until a Google OAuth client id exists. No invented rows."""
    if token != DESK_TOKEN:
        return JSONResponse({"error": "bad token"}, status_code=403)
    return JSONResponse(youtube_status())


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
