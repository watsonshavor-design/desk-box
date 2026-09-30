"""SQLite records for messages, requests, Ace jobs, and sessions.

The JSONL log remains a paste-relay copy. This database is the source of
truth for ids, idempotency, and exactly-once Ace completion.
"""
import hashlib
import json
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone


def now_iso():
    return datetime.now(timezone.utc).isoformat()


SCHEMA = """
CREATE TABLE IF NOT EXISTS threads (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  type TEXT NOT NULL DEFAULT 'general',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  thread_id TEXT NOT NULL,
  client_id TEXT,
  sender TEXT NOT NULL,
  text TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  mode TEXT,
  hidden INTEGER NOT NULL DEFAULT 0,
  funnel INTEGER NOT NULL DEFAULT 0,
  for_message_id TEXT,
  for_ts TEXT,
  round INTEGER,
  error INTEGER NOT NULL DEFAULT 0,
  attachment_json TEXT,
  pinned INTEGER NOT NULL DEFAULT 0,
  reply_to TEXT,
  request_id TEXT,
  structured_json TEXT,
  agreement TEXT,
  context_version INTEGER,
  funnel_answer INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS requests (
  id TEXT PRIMARY KEY,
  message_id TEXT NOT NULL,
  thread_id TEXT NOT NULL,
  state TEXT NOT NULL,
  mode TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ace_jobs (
  id TEXT PRIMARY KEY,
  message_id TEXT NOT NULL UNIQUE,
  thread_id TEXT NOT NULL,
  status TEXT NOT NULL,
  lease_until TEXT,
  payload_json TEXT NOT NULL,
  result_json TEXT,
  fail_reason TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  revoked INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS decisions (
  id TEXT PRIMARY KEY,
  message_id TEXT,
  thread_id TEXT,
  title TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS context_packs (
  name TEXT PRIMARY KEY,
  version INTEGER NOT NULL,
  body TEXT NOT NULL,
  provenance TEXT,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_thread ON messages(thread_id, created_at);
CREATE INDEX IF NOT EXISTS idx_messages_for ON messages(for_message_id);
"""


SCHEMA_PHASE_D = """
CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  notes TEXT NOT NULL DEFAULT '',
  due_at TEXT,
  owner TEXT NOT NULL,
  status TEXT NOT NULL,
  priority TEXT NOT NULL DEFAULT 'normal',
  source_message_id TEXT,
  thread_id TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT
);
CREATE TABLE IF NOT EXISTS briefings (
  id TEXT PRIMARY KEY,
  category TEXT NOT NULL,
  title TEXT NOT NULL,
  summary TEXT NOT NULL DEFAULT '',
  body_markdown TEXT NOT NULL DEFAULT '',
  published_at TEXT,
  retrieved_at TEXT NOT NULL,
  source_items_json TEXT NOT NULL DEFAULT '[]',
  related_symbols_json TEXT NOT NULL DEFAULT '[]',
  canonical_key TEXT NOT NULL,
  read INTEGER NOT NULL DEFAULT 0,
  pinned INTEGER NOT NULL DEFAULT 0,
  provenance TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_briefing_key ON briefings(canonical_key);
"""

OWNERS = {"shavor", "ace", "rail", "anchor", "external"}
TASK_STATUSES = {"open", "waiting", "completed"}


def task_view(task, today):
    if task.get("status") == "completed":
        return "completed"
    if task.get("status") == "waiting":
        return "waiting"
    due = task.get("due_at") or ""
    if today and due and due[:10] > today:
        return "upcoming"
    return "today"


def canonical_key(title, sources):
    for item in sources or []:
        if isinstance(item, dict) and item.get("url"):
            return "url:" + str(item["url"]).strip().lower()
    collapsed = " ".join((title or "").lower().split())
    return "title:" + collapsed


def infer_provenance(sources):
    for item in sources or []:
        if isinstance(item, dict) and (item.get("url") or item.get("name")):
            return "news"
    return "desk_analysis"


def pin_title(title):
    text = (title or "").lower()
    return "pre-market" in text or "sunday scan" in text


def briefing_row(row):
    if row is None:
        return None
    item = dict(row)
    item["source_items"] = json.loads(item.pop("source_items_json") or "[]")
    item["related_symbols"] = json.loads(item.pop("related_symbols_json") or "[]")
    item["read"] = bool(item["read"])
    item["pinned"] = bool(item["pinned"])
    return item


class Store:
    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(SCHEMA)
        self.db.executescript(SCHEMA_PHASE_D)
        self.db.commit()

    def wipe(self):
        with self._lock:
            for table in ("messages", "requests", "ace_jobs", "sessions",
                          "decisions", "threads", "context_packs", "tasks", "briefings"):
                self.db.execute(f"DELETE FROM {table}")
            self.db.commit()

    def count_messages(self):
        with self._lock:
            return self.db.execute("SELECT COUNT(*) AS n FROM messages").fetchone()["n"]

    def ensure_thread(self, thread_id="desk", title="Desk", kind="general"):
        now = now_iso()
        with self._lock:
            row = self.db.execute("SELECT * FROM threads WHERE id = ?", (thread_id,)).fetchone()
            if row:
                return dict(row)
            self.db.execute(
                "INSERT INTO threads (id, title, type, created_at, updated_at) VALUES (?,?,?,?,?)",
                (thread_id, title, kind, now, now),
            )
            self.db.commit()
        return {"id": thread_id, "title": title, "type": kind, "created_at": now, "updated_at": now}

    def create_thread(self, title, kind="general", thread_id=None):
        thread_id = thread_id or str(uuid.uuid4())
        return self.ensure_thread(thread_id, title[:80] or "Thread", kind or "general")

    def list_threads(self):
        with self._lock:
            rows = self.db.execute("SELECT * FROM threads ORDER BY updated_at DESC").fetchall()
        return [dict(row) for row in rows]

    def touch_thread(self, thread_id):
        with self._lock:
            self.db.execute("UPDATE threads SET updated_at = ? WHERE id = ?", (now_iso(), thread_id))
            self.db.commit()

    def get_message(self, message_id):
        if not message_id:
            return None
        with self._lock:
            row = self.db.execute("SELECT * FROM messages WHERE id = ?", (message_id,)).fetchone()
        return self.to_entry(row) if row else None

    def insert_message(self, entry):
        """Insert once. Returns False when the id is already stored."""
        message_id = entry.get("id") or entry.get("message_id") or str(uuid.uuid4())
        entry["id"] = message_id
        entry["message_id"] = message_id
        thread_id = entry.get("thread_id") or "desk"
        entry["thread_id"] = thread_id
        self.ensure_thread(thread_id)
        attachment = entry.get("attachment")
        structured = entry.get("structured")
        with self._lock:
            existing = self.db.execute("SELECT id FROM messages WHERE id = ?", (message_id,)).fetchone()
            if existing:
                return False
            self.db.execute(
                """INSERT INTO messages (
                    id, thread_id, client_id, sender, text, created_at, mode, hidden, funnel,
                    for_message_id, for_ts, round, error, attachment_json, pinned, reply_to,
                    request_id, structured_json, agreement, context_version, funnel_answer
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    message_id, thread_id, entry.get("client_id"), entry.get("from") or "desk",
                    entry.get("text") or "", entry.get("ts") or now_iso(), entry.get("mode"),
                    1 if entry.get("hidden") else 0, 1 if entry.get("funnel") else 0,
                    entry.get("for_message_id"), entry.get("for_ts"), entry.get("round"),
                    1 if entry.get("error") else 0,
                    json.dumps(attachment) if attachment else None,
                    1 if entry.get("pinned") else 0, entry.get("reply_to"),
                    entry.get("request_id"),
                    json.dumps(structured) if structured else None,
                    entry.get("agreement"), entry.get("context_version"),
                    1 if entry.get("funnel_answer") else 0,
                ),
            )
            self.db.execute("UPDATE threads SET updated_at = ? WHERE id = ?", (now_iso(), thread_id))
            self.db.commit()
        return True

    def list_entries(self, limit=2000, thread_id=None):
        sql = "SELECT * FROM messages"
        args = []
        if thread_id:
            sql += " WHERE thread_id = ?"
            args.append(thread_id)
        sql += " ORDER BY created_at DESC"
        if limit:
            sql += " LIMIT ?"
            args.append(int(limit))
        with self._lock:
            rows = self.db.execute(sql, args).fetchall()
        return [self.to_entry(row) for row in reversed(rows)]

    def latest(self, limit):
        with self._lock:
            rows = self.db.execute(
                "SELECT * FROM messages ORDER BY created_at DESC LIMIT ?", (int(limit),)
            ).fetchall()
        return [self.to_entry(row) for row in reversed(rows)]

    def has_funnel_answer(self, for_message_id=None, for_ts=None):
        with self._lock:
            if for_message_id:
                row = self.db.execute(
                    "SELECT id FROM messages WHERE funnel_answer = 1 AND for_message_id = ?",
                    (for_message_id,),
                ).fetchone()
                if row:
                    return row["id"]
            if for_ts:
                row = self.db.execute(
                    "SELECT id FROM messages WHERE funnel_answer = 1 AND for_ts = ?",
                    (for_ts,),
                ).fetchone()
                if row:
                    return row["id"]
        return None

    def set_request(self, request_id, message_id, thread_id, state, mode):
        now = now_iso()
        with self._lock:
            self.db.execute(
                """INSERT INTO requests (id, message_id, thread_id, state, mode, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?)
                   ON CONFLICT(id) DO UPDATE SET state = excluded.state, updated_at = excluded.updated_at""",
                (request_id, message_id, thread_id, state, mode, now, now),
            )
            self.db.commit()

    def pin(self, message_id):
        with self._lock:
            cur = self.db.execute(
                "UPDATE messages SET pinned = CASE pinned WHEN 1 THEN 0 ELSE 1 END WHERE id = ?",
                (message_id,),
            )
            self.db.commit()
            if cur.rowcount == 0:
                return None
            row = self.db.execute("SELECT pinned FROM messages WHERE id = ?", (message_id,)).fetchone()
        return bool(row["pinned"]) if row else None

    def search(self, query, limit=30):
        needle = f"%{query.strip()}%"
        with self._lock:
            rows = self.db.execute(
                """SELECT * FROM messages
                   WHERE hidden = 0 AND (text LIKE ? OR sender LIKE ?)
                   ORDER BY created_at DESC LIMIT ?""",
                (needle, needle, int(limit)),
            ).fetchall()
        return [self.to_entry(row) for row in rows]

    def create_job(self, message_id, thread_id, payload):
        now = now_iso()
        job_id = str(uuid.uuid4())
        with self._lock:
            existing = self.db.execute(
                "SELECT * FROM ace_jobs WHERE message_id = ?", (message_id,)
            ).fetchone()
            if existing:
                return dict(existing)
            self.db.execute(
                """INSERT INTO ace_jobs (
                    id, message_id, thread_id, status, payload_json, attempts, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?)""",
                (job_id, message_id, thread_id, "pending", json.dumps(payload), 0, now, now),
            )
            self.db.commit()
        return {"id": job_id, "message_id": message_id, "status": "pending", "payload": payload}

    def claim_job(self, lease_seconds=90):
        now = datetime.now(timezone.utc)
        until = (now + timedelta(seconds=lease_seconds)).isoformat()
        stamp = now.isoformat()
        with self._lock:
            row = self.db.execute(
                """SELECT * FROM ace_jobs
                   WHERE status = 'pending'
                      OR (status = 'leased' AND lease_until IS NOT NULL AND lease_until < ?)
                   ORDER BY created_at ASC LIMIT 1""",
                (stamp,),
            ).fetchone()
            if not row:
                return None
            self.db.execute(
                "UPDATE ace_jobs SET status = 'leased', lease_until = ?, attempts = attempts + 1, updated_at = ? WHERE id = ?",
                (until, stamp, row["id"]),
            )
            self.db.commit()
            fresh = self.db.execute("SELECT * FROM ace_jobs WHERE id = ?", (row["id"],)).fetchone()
        return self._job(fresh)

    def get_job(self, job_id):
        with self._lock:
            row = self.db.execute("SELECT * FROM ace_jobs WHERE id = ?", (job_id,)).fetchone()
        return self._job(row) if row else None

    def complete_job(self, job_id, result):
        """Return 'missing', 'duplicate', or 'ok'. A second completion is rejected."""
        with self._lock:
            row = self.db.execute("SELECT * FROM ace_jobs WHERE id = ?", (job_id,)).fetchone()
            if not row:
                return "missing"
            if row["status"] == "complete":
                return "duplicate"
            self.db.execute(
                "UPDATE ace_jobs SET status = 'complete', result_json = ?, updated_at = ?, lease_until = NULL WHERE id = ?",
                (json.dumps(result), now_iso(), job_id),
            )
            self.db.commit()
        return "ok"

    def fail_job(self, job_id, reason, retry=True):
        with self._lock:
            row = self.db.execute("SELECT * FROM ace_jobs WHERE id = ?", (job_id,)).fetchone()
            if not row:
                return "missing"
            if row["status"] == "complete":
                return "duplicate"
            status = "pending" if retry and row["attempts"] < 3 else "failed"
            self.db.execute(
                "UPDATE ace_jobs SET status = ?, fail_reason = ?, lease_until = NULL, updated_at = ? WHERE id = ?",
                (status, (reason or "")[:500], now_iso(), job_id),
            )
            self.db.commit()
        return status

    def create_session(self):
        session_id = uuid.uuid4().hex
        with self._lock:
            self.db.execute(
                "INSERT INTO sessions (id, created_at, revoked) VALUES (?,?,0)",
                (session_id, now_iso()),
            )
            self.db.commit()
        return session_id

    def session_ok(self, session_id):
        if not session_id:
            return False
        with self._lock:
            row = self.db.execute(
                "SELECT revoked FROM sessions WHERE id = ?", (session_id,)
            ).fetchone()
        return bool(row) and not row["revoked"]

    def revoke_session(self, session_id):
        with self._lock:
            self.db.execute("UPDATE sessions SET revoked = 1 WHERE id = ?", (session_id,))
            self.db.commit()

    def add_decision(self, title, status="active", message_id=None, thread_id=None):
        now = now_iso()
        decision_id = str(uuid.uuid4())
        status = status if status in ("active", "superseded", "completed", "canceled") else "active"
        with self._lock:
            self.db.execute(
                """INSERT INTO decisions (id, message_id, thread_id, title, status, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (decision_id, message_id, thread_id, title[:200], status, now, now),
            )
            self.db.commit()
        return {"id": decision_id, "title": title, "status": status, "message_id": message_id}

    def list_decisions(self):
        with self._lock:
            rows = self.db.execute("SELECT * FROM decisions ORDER BY updated_at DESC").fetchall()
        return [dict(row) for row in rows]

    def seed_context(self, packs):
        """Insert packs only when missing so a restart does not bump versions."""
        now = now_iso()
        with self._lock:
            for name, body in packs.items():
                row = self.db.execute("SELECT version FROM context_packs WHERE name = ?", (name,)).fetchone()
                if row:
                    continue
                self.db.execute(
                    """INSERT INTO context_packs (name, version, body, provenance, updated_at)
                       VALUES (?,?,?,?,?)""",
                    (name, 1, body, "desk", now),
                )
            self.db.commit()

    def context_version(self):
        with self._lock:
            row = self.db.execute("SELECT COALESCE(MAX(version), 0) AS v FROM context_packs").fetchone()
        return int(row["v"] or 0)

    def context_packs(self):
        with self._lock:
            rows = self.db.execute("SELECT * FROM context_packs ORDER BY name").fetchall()
        return [dict(row) for row in rows]

    def update_context(self, name, body, provenance="ace"):
        now = now_iso()
        with self._lock:
            row = self.db.execute("SELECT version FROM context_packs WHERE name = ?", (name,)).fetchone()
            version = (row["version"] if row else 0) + 1
            self.db.execute(
                """INSERT INTO context_packs (name, version, body, provenance, updated_at)
                   VALUES (?,?,?,?,?)
                   ON CONFLICT(name) DO UPDATE SET
                     version = excluded.version, body = excluded.body,
                     provenance = excluded.provenance, updated_at = excluded.updated_at""",
                (name, version, body, provenance, now),
            )
            self.db.commit()
        return version

    def prune(self, cutoff_iso):
        with self._lock:
            self.db.execute("DELETE FROM messages WHERE created_at < ?", (cutoff_iso,))
            self.db.commit()

    def add_task(self, title, notes="", due_at=None, owner="shavor", status="open",
                 priority="normal", source_message_id=None, thread_id=None):
        owner = owner if owner in OWNERS else "shavor"
        status = status if status in TASK_STATUSES else "open"
        now = now_iso()
        task_id = str(uuid.uuid4())
        with self._lock:
            self.db.execute(
                """INSERT INTO tasks (
                    id, title, notes, due_at, owner, status, priority, source_message_id,
                    thread_id, created_at, updated_at, completed_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (task_id, title[:200], (notes or "")[:2000], due_at or None, owner, status,
                 priority or "normal", source_message_id, thread_id, now, now,
                 now if status == "completed" else None),
            )
            self.db.commit()
        return self.get_task(task_id)

    def get_task(self, task_id):
        with self._lock:
            row = self.db.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        return dict(row) if row else None

    def list_tasks(self, view=None, today=None):
        with self._lock:
            rows = self.db.execute("SELECT * FROM tasks ORDER BY updated_at DESC").fetchall()
        tasks = [dict(row) for row in rows]
        if not view:
            return tasks
        return [task for task in tasks if task_view(task, today) == view]

    def update_task(self, task_id, fields):
        current = self.get_task(task_id)
        if not current:
            return None
        allowed = ("title", "notes", "due_at", "owner", "status", "priority")
        for key in allowed:
            if key in fields and fields[key] is not None:
                current[key] = fields[key]
        if current["owner"] not in OWNERS:
            current["owner"] = "shavor"
        if current["status"] not in TASK_STATUSES:
            current["status"] = "open"
        completed_at = current.get("completed_at")
        if current["status"] == "completed":
            completed_at = completed_at or now_iso()
        else:
            completed_at = None
        with self._lock:
            self.db.execute(
                """UPDATE tasks SET title=?, notes=?, due_at=?, owner=?, status=?, priority=?,
                   updated_at=?, completed_at=? WHERE id=?""",
                (current["title"], current["notes"], current["due_at"], current["owner"],
                 current["status"], current["priority"], now_iso(), completed_at, task_id),
            )
            self.db.commit()
        return self.get_task(task_id)

    def complete_task(self, task_id):
        return self.update_task(task_id, {"status": "completed"})

    def upsert_briefing(self, record):
        """Insert or collapse onto the existing canonical story."""
        now = now_iso()
        title = (record.get("title") or "").strip()[:200]
        sources = record.get("source_items") or []
        key = canonical_key(title, sources)
        provenance = record.get("provenance") or infer_provenance(sources)
        pinned = 1 if record.get("pinned") or pin_title(title) else 0
        category = record.get("category") or "for_you"
        with self._lock:
            existing = self.db.execute(
                "SELECT * FROM briefings WHERE canonical_key = ?", (key,)
            ).fetchone()
            if existing:
                prior = json.loads(existing["source_items_json"] or "[]")
                prior.append({
                    "summary": record.get("summary") or "",
                    "retrieved_at": now,
                    "source_items": sources,
                })
                self.db.execute(
                    """UPDATE briefings SET summary=?, body_markdown=?, retrieved_at=?,
                       source_items_json=?, published_at=COALESCE(?, published_at),
                       provenance=?, pinned=CASE pinned WHEN 1 THEN 1 ELSE ? END,
                       category=? WHERE id=?""",
                    (record.get("summary") or existing["summary"],
                     record.get("body_markdown") or existing["body_markdown"],
                     now, json.dumps(prior), record.get("published_at"), provenance,
                     pinned, category, existing["id"]),
                )
                self.db.commit()
                briefing_id = existing["id"]
            else:
                briefing_id = record.get("briefing_id") or str(uuid.uuid4())
                self.db.execute(
                    """INSERT INTO briefings (
                        id, category, title, summary, body_markdown, published_at, retrieved_at,
                        source_items_json, related_symbols_json, canonical_key, read, pinned, provenance
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,0,?,?)""",
                    (briefing_id, category, title, record.get("summary") or "",
                     record.get("body_markdown") or "", record.get("published_at"), now,
                     json.dumps(sources), json.dumps(record.get("related_symbols") or []),
                     key, pinned, provenance),
                )
                self.db.commit()
        return self.get_briefing(briefing_id)

    def get_briefing(self, briefing_id):
        with self._lock:
            row = self.db.execute("SELECT * FROM briefings WHERE id = ?", (briefing_id,)).fetchone()
        return briefing_row(row) if row else None

    def list_briefings(self, category=None):
        with self._lock:
            if category and category != "for_you":
                rows = self.db.execute(
                    "SELECT * FROM briefings WHERE category = ? ORDER BY pinned DESC, retrieved_at DESC",
                    (category,),
                ).fetchall()
            else:
                rows = self.db.execute(
                    "SELECT * FROM briefings ORDER BY pinned DESC, retrieved_at DESC"
                ).fetchall()
        return [briefing_row(row) for row in rows]

    def mark_briefing_read(self, briefing_id):
        with self._lock:
            self.db.execute("UPDATE briefings SET read = 1 WHERE id = ?", (briefing_id,))
            self.db.commit()
        return self.get_briefing(briefing_id)

    def latest_briefing(self):
        items = self.list_briefings()
        if not items:
            return None
        pinned = [item for item in items if item.get("pinned")]
        return pinned[0] if pinned else items[0]

    def backfill(self, entries):
        """Import legacy JSONL rows once. Returns how many new rows were stored."""
        added = 0
        for entry in entries:
            legacy = dict(entry)
            if not legacy.get("id"):
                raw = f"{legacy.get('ts')}|{legacy.get('from')}|{legacy.get('text')}"
                legacy["id"] = hashlib.sha256(raw.encode()).hexdigest()[:32]
            legacy["thread_id"] = legacy.get("thread_id") or "desk"
            if self.insert_message(legacy):
                added += 1
        return added

    @staticmethod
    def to_entry(row):
        if row is None:
            return None
        entry = {
            "id": row["id"],
            "message_id": row["id"],
            "thread_id": row["thread_id"],
            "from": row["sender"],
            "text": row["text"] or "",
            "ts": row["created_at"],
        }
        if row["client_id"]:
            entry["client_id"] = row["client_id"]
        if row["mode"]:
            entry["mode"] = row["mode"]
        if row["hidden"]:
            entry["hidden"] = True
        if row["funnel"]:
            entry["funnel"] = True
        if row["for_message_id"]:
            entry["for_message_id"] = row["for_message_id"]
        if row["for_ts"]:
            entry["for_ts"] = row["for_ts"]
        if row["round"]:
            entry["round"] = row["round"]
        if row["error"]:
            entry["error"] = True
        if row["attachment_json"]:
            entry["attachment"] = json.loads(row["attachment_json"])
        if row["pinned"]:
            entry["pinned"] = True
        if row["reply_to"]:
            entry["reply_to"] = row["reply_to"]
        if row["request_id"]:
            entry["request_id"] = row["request_id"]
        if row["structured_json"]:
            entry["structured"] = json.loads(row["structured_json"])
        if row["agreement"]:
            entry["agreement"] = row["agreement"]
        if row["context_version"]:
            entry["context_version"] = row["context_version"]
        if row["funnel_answer"]:
            entry["funnel_answer"] = True
        return entry

    @staticmethod
    def _job(row):
        if row is None:
            return None
        payload = json.loads(row["payload_json"] or "{}")
        result = json.loads(row["result_json"]) if row["result_json"] else None
        return {
            "id": row["id"],
            "message_id": row["message_id"],
            "thread_id": row["thread_id"],
            "status": row["status"],
            "lease_until": row["lease_until"],
            "attempts": row["attempts"],
            "fail_reason": row["fail_reason"],
            "payload": payload,
            "result": result,
        }
