"""Top-gainers feeds for Desk Box (Webull + Moomoo/OpenD).

Honest live fetch only — never invents prices. On failure returns empty
items with a clear status/message.

Webull (works from Railway, no API key):
  GET https://quotes-gw.webullfintech.com/api/wlas/ranking/topGainers
      ?regionId=6&rankType=1d&pageIndex=1&pageSize=50
  Unofficial but stable public ranking endpoint used widely in the industry
  (same path as webull_unofficial / quotes-gw). regionId=6 = US.

Moomoo (OpenD quote gateway):
  Uses moomoo OpenQuoteContext.get_top_movers_rank(market=US) when OpenD is
  reachable at MOOMOO_OPEND_HOST:MOOMOO_OPEND_PORT (defaults 127.0.0.1:11111,
  same as Finance Tracker / Night Desk on the shared box).
  OpenD binds to localhost on the desk box, so Railway cannot reach it —
  Moomoo soft-fails with status=not_connected and a clear reason unless an
  OpenD gateway (or tunnel) is pointed at via those env vars. No new secrets.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import httpx

log = logging.getLogger("desk-box.gainers")

CACHE_TTL_SEC = 120
WEBULL_URL = (
    "https://quotes-gw.webullfintech.com/api/wlas/ranking/topGainers"
)
WEBULL_PARAMS = {
    "regionId": 6,       # US
    "rankType": "1d",    # 1-day change
    "pageIndex": 1,
    "pageSize": 50,
}
WEBULL_DOC = (
    "https://quotes-gw.webullfintech.com/api/wlas/ranking/topGainers"
    "?regionId=6&rankType=1d&pageIndex=1&pageSize=50"
)

LABELS = {
    "combined": "Combined broker ranking",
    "moomoo": "Moomoo ranking",
    "webull": "Webull ranking",
}

_lock = asyncio.Lock()
# Per-source cache: {items, status, message, updated_at, fetched_mono, reason?}
_cache: dict[str, dict[str, Any]] = {
    "webull": {},
    "moomoo": {},
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fmt_pct(pct: float | None) -> str | None:
    if pct is None:
        return None
    try:
        v = float(pct)
    except (TypeError, ValueError):
        return None
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.2f}%"


def _fmt_price(val: Any) -> str | None:
    if val is None or val == "":
        return None
    try:
        v = float(val)
    except (TypeError, ValueError):
        return str(val)
    if v >= 100:
        return f"{v:.2f}"
    if v >= 1:
        return f"{v:.4f}".rstrip("0").rstrip(".")
    return f"{v:.4f}"


def _num(val: Any) -> float | None:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _row(
    *,
    symbol: str,
    name: str | None,
    last: Any,
    change_pct: float | None,
    volume: Any = None,
    source: str,
) -> dict[str, Any]:
    last_s = _fmt_price(last)
    chg = _fmt_pct(change_pct)
    out: dict[str, Any] = {
        "symbol": symbol,
        "last": last_s if last_s is not None else last,
        "price": last_s if last_s is not None else last,
        "change": chg,
        "change_pct": round(change_pct, 4) if change_pct is not None else None,
        "source": source,
        "sources": [source],
    }
    if name:
        out["name"] = name
    vol = _num(volume)
    if vol is not None:
        out["volume"] = int(vol) if vol >= 1 else vol
    return out


async def fetch_webull() -> dict[str, Any]:
    """Fetch US top gainers from Webull public ranking API."""
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.get(
                WEBULL_URL,
                params=WEBULL_PARAMS,
                headers={
                    "User-Agent": "desk-box/1.0",
                    "Accept": "application/json",
                },
            )
            r.raise_for_status()
            payload = r.json()
    except Exception as e:
        log.warning("webull gainers fetch failed: %s", e)
        return {
            "items": [],
            "status": "error",
            "message": f"Webull feed failed: {e}",
            "reason": str(e),
            "updated_at": _now_iso(),
            "endpoint": WEBULL_DOC,
        }

    items: list[dict[str, Any]] = []
    for entry in payload.get("data") or []:
        ticker = entry.get("ticker") or {}
        values = entry.get("values") or {}
        symbol = (
            ticker.get("disSymbol")
            or ticker.get("symbol")
            or values.get("symbol")
        )
        if not symbol:
            continue
        # Webull changeRatio is a decimal ratio (1.3333 == +133.33%).
        ratio = _num(values.get("changeRatio"))
        if ratio is None:
            ratio = _num(ticker.get("changeRatio"))
        change_pct = (ratio * 100.0) if ratio is not None else None
        price = values.get("price")
        if price is None:
            price = ticker.get("close") or ticker.get("price")
        items.append(
            _row(
                symbol=str(symbol).strip().upper(),
                name=(ticker.get("name") or None),
                last=price,
                change_pct=change_pct,
                volume=ticker.get("volume") or values.get("volume"),
                source="webull",
            )
        )

    return {
        "items": items,
        "status": "ok" if items else "empty",
        "message": (
            f"Webull US top gainers ({len(items)} names)"
            if items else "Webull returned no gainers right now"
        ),
        "updated_at": _now_iso(),
        "endpoint": WEBULL_DOC,
    }


def _fetch_moomoo_sync() -> dict[str, Any]:
    """Blocking OpenD call — run in a thread. Soft-fails cleanly."""
    host = os.environ.get("MOOMOO_OPEND_HOST") or os.environ.get(
        "FUTU_OPEND_HOST", "127.0.0.1"
    )
    try:
        port = int(
            os.environ.get("MOOMOO_OPEND_PORT")
            or os.environ.get("FUTU_OPEND_PORT", "11111")
        )
    except ValueError:
        port = 11111

    reason_unreachable = (
        f"OpenD not reachable at {host}:{port}. "
        "Moomoo quotes need the OpenD gateway (or a tunnel) reachable from "
        "this host — on Railway that usually means pointing "
        "MOOMOO_OPEND_HOST/PORT at a reachable OpenD, or an API key gateway. "
        "Local desk box OpenD binds 127.0.0.1:11111 and is not exposed."
    )

    # Fast TCP probe so we don't hang waiting for the SDK on Railway.
    import socket
    try:
        with socket.create_connection((host, port), timeout=2.0):
            pass
    except OSError as e:
        return {
            "items": [],
            "status": "not_connected",
            "message": "Moomoo feed not connected (OpenD unreachable)",
            "reason": f"{reason_unreachable} ({e})",
            "updated_at": _now_iso(),
            "opend": f"{host}:{port}",
        }

    try:
        from moomoo import OpenQuoteContext, RET_OK, Market, RankSortDir
    except ImportError:
        try:
            from futu import OpenQuoteContext, RET_OK, Market, RankSortDir
        except ImportError as e:
            return {
                "items": [],
                "status": "not_connected",
                "message": "Moomoo SDK not installed on this host",
                "reason": (
                    f"OpenD is reachable at {host}:{port} but the moomoo/futu "
                    f"Python package is not installed ({e})."
                ),
                "updated_at": _now_iso(),
                "opend": f"{host}:{port}",
            }

    ctx = None
    try:
        ctx = OpenQuoteContext(host=host, port=port)
        ret, data = ctx.get_top_movers_rank(
            market=Market.US,
            sort_dir=RankSortDir.DESCENDING,
            count=50,
        )
        if ret != RET_OK:
            return {
                "items": [],
                "status": "error",
                "message": f"Moomoo top-movers failed: {data}",
                "reason": str(data),
                "updated_at": _now_iso(),
                "opend": f"{host}:{port}",
            }
        _all_count, df = data
        items: list[dict[str, Any]] = []
        for _, row in df.iterrows():
            sec = str(row.get("security") or "")
            # US.AAPL -> AAPL
            symbol = sec.split(".", 1)[-1] if sec else ""
            if not symbol:
                continue
            # Moomoo change_ratio is already percent points (14.16 == +14.16%).
            change_pct = _num(row.get("change_ratio"))
            items.append(
                _row(
                    symbol=symbol.strip().upper(),
                    name=(str(row.get("name")) if row.get("name") is not None else None),
                    last=row.get("cur_price"),
                    change_pct=change_pct,
                    volume=row.get("volume"),
                    source="moomoo",
                )
            )
        return {
            "items": items,
            "status": "ok" if items else "empty",
            "message": (
                f"Moomoo US top movers via OpenD ({len(items)} names)"
                if items else "Moomoo returned no movers right now"
            ),
            "updated_at": _now_iso(),
            "opend": f"{host}:{port}",
        }
    except Exception as e:
        log.warning("moomoo gainers fetch failed: %s", e)
        return {
            "items": [],
            "status": "not_connected",
            "message": "Moomoo feed not connected",
            "reason": f"{reason_unreachable} Detail: {e}",
            "updated_at": _now_iso(),
            "opend": f"{host}:{port}",
        }
    finally:
        if ctx is not None:
            try:
                ctx.close()
            except Exception:
                pass


async def fetch_moomoo() -> dict[str, Any]:
    return await asyncio.to_thread(_fetch_moomoo_sync)


def _stale(entry: dict[str, Any]) -> bool:
    if not entry:
        return True
    fetched = entry.get("fetched_mono")
    if fetched is None:
        return True
    return (time.monotonic() - fetched) > CACHE_TTL_SEC


async def _refresh_one(source: str) -> dict[str, Any]:
    if source == "webull":
        result = await fetch_webull()
    elif source == "moomoo":
        result = await fetch_moomoo()
    else:
        raise ValueError(source)
    result["fetched_mono"] = time.monotonic()
    async with _lock:
        _cache[source] = result
    return result


async def refresh_all() -> None:
    await asyncio.gather(
        _refresh_one("webull"),
        _refresh_one("moomoo"),
        return_exceptions=True,
    )


async def background_loop() -> None:
    """Refresh both feeds about every CACHE_TTL_SEC."""
    # Initial fetch shortly after boot so the first UI hit is warm.
    await asyncio.sleep(1)
    while True:
        try:
            await refresh_all()
        except Exception as e:
            log.warning("gainers background refresh failed: %s", e)
        await asyncio.sleep(CACHE_TTL_SEC)


async def _get_cached(source: str) -> dict[str, Any]:
    async with _lock:
        entry = dict(_cache.get(source) or {})
    if _stale(entry):
        entry = await _refresh_one(source)
    # Strip internal fields for callers.
    out = {k: v for k, v in entry.items() if k != "fetched_mono"}
    return out


def _merge(webull: dict[str, Any], moomoo: dict[str, Any]) -> dict[str, Any]:
    by_sym: dict[str, dict[str, Any]] = {}
    # Prefer higher change_pct when deduping; keep both source tags.
    for src_payload in (webull, moomoo):
        for item in src_payload.get("items") or []:
            sym = str(item.get("symbol") or "").upper()
            if not sym:
                continue
            existing = by_sym.get(sym)
            if existing is None:
                row = dict(item)
                row["sources"] = list(item.get("sources") or [item.get("source")])
                by_sym[sym] = row
                continue
            srcs = list(existing.get("sources") or [])
            for s in (item.get("sources") or [item.get("source")]):
                if s and s not in srcs:
                    srcs.append(s)
            existing["sources"] = srcs
            existing["source"] = ",".join(srcs)
            # Keep the quote with the larger absolute move as display price.
            old_pct = _num(existing.get("change_pct")) or 0.0
            new_pct = _num(item.get("change_pct")) or 0.0
            if abs(new_pct) > abs(old_pct):
                for k in ("last", "price", "change", "change_pct", "volume", "name"):
                    if k in item:
                        existing[k] = item[k]

    items = sorted(
        by_sym.values(),
        key=lambda r: (_num(r.get("change_pct")) is not None,
                       _num(r.get("change_pct")) or 0.0),
        reverse=True,
    )

    w_ok = webull.get("status") == "ok"
    m_ok = moomoo.get("status") == "ok"
    if w_ok and m_ok:
        status, message = "ok", (
            f"Combined Webull + Moomoo ({len(items)} names, deduped)"
        )
    elif w_ok:
        status, message = "partial", (
            f"Combined: Webull ok, Moomoo {moomoo.get('status')} — "
            f"{moomoo.get('message') or moomoo.get('reason') or 'unavailable'}"
        )
    elif m_ok:
        status, message = "partial", (
            f"Combined: Moomoo ok, Webull {webull.get('status')} — "
            f"{webull.get('message') or 'unavailable'}"
        )
    else:
        status, message = "not_connected", (
            "Neither broker feed returned movers. "
            f"Webull: {webull.get('message')}; Moomoo: {moomoo.get('message')}"
        )

    return {
        "items": items,
        "status": status,
        "message": message,
        "updated_at": _now_iso(),
        "sources": {
            "webull": {
                "status": webull.get("status"),
                "message": webull.get("message"),
                "count": len(webull.get("items") or []),
                "endpoint": webull.get("endpoint"),
            },
            "moomoo": {
                "status": moomoo.get("status"),
                "message": moomoo.get("message"),
                "reason": moomoo.get("reason"),
                "count": len(moomoo.get("items") or []),
                "opend": moomoo.get("opend"),
            },
        },
    }


async def get_gainers(source: str = "combined") -> dict[str, Any]:
    src = (source or "combined").strip().lower()
    if src not in ("combined", "moomoo", "webull"):
        src = "combined"

    if src == "webull":
        data = await _get_cached("webull")
    elif src == "moomoo":
        data = await _get_cached("moomoo")
    else:
        webull, moomoo = await asyncio.gather(
            _get_cached("webull"), _get_cached("moomoo")
        )
        data = _merge(webull, moomoo)

    return {
        "ok": True,
        "source": src,
        "label": LABELS[src],
        "items": data.get("items") or [],
        "updated_at": data.get("updated_at") or _now_iso(),
        "status": data.get("status") or "not_connected",
        "message": data.get("message") or f"{src} feed not connected",
        **{k: data[k] for k in ("reason", "endpoint", "opend", "sources")
           if k in data},
    }
