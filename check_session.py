"""Morning-card session clock cases. Run: python check_session.py"""
from datetime import datetime
from zoneinfo import ZoneInfo

from app import session_name

ET = ZoneInfo("America/New_York")

# Week of 2026-10-03: Sat 3, Sun 4, Mon 5, Fri 9.
CASES = (
    ("2026-10-03T12:00", "closed"),
    ("2026-10-04T19:59", "closed"),
    ("2026-10-04T20:00", "overnight"),
    ("2026-10-05T10:00", "live"),
    ("2026-10-09T20:00", "closed"),
    ("2026-10-05T02:00", "overnight"),
    ("2026-10-05T04:00", "pre-market"),
    ("2026-10-05T09:30", "live"),
    ("2026-10-05T16:00", "overnight"),
    ("2026-10-08T22:00", "overnight"),
    ("2026-10-09T16:00", "overnight"),
    ("2026-10-09T19:59", "overnight"),
    ("2026-10-03T20:00", "closed"),
    ("2026-10-04T00:00", "closed"),
)


def main():
    failed = 0
    for stamp, want in CASES:
        now = datetime.fromisoformat(stamp).replace(tzinfo=ET)
        got = session_name(now)
        ok = got == want
        label = now.strftime("%a %Y-%m-%d %H:%M ET")
        print(f"{label} -> {got}" + ("" if ok else f"  WANT {want}"))
        if not ok:
            failed += 1
    return failed


if __name__ == "__main__":
    raise SystemExit(main())
