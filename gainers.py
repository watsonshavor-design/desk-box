"""Top-gainers feeds for Desk Box (Webull + Moomoo OpenAPI).

Honest live fetch only — never invents prices. On failure returns empty
items with a clear status/message.

Webull (works from Railway, no API key):
  GET https://quotes-gw.webullfintech.com/api/wlas/ranking/topGainers
      ?regionId=6&rankType=1d&pageIndex=1&pageSize=50
  Unofficial but stable public ranking endpoint used widely in the industry
  (same path as webull_unofficial / quotes-gw). regionId=6 = US.

Moomoo (preferred: gateway-free OpenAPI at webapi.moomoo.com):
  Traditional API Key auth — X-Api-Key = MOOMOO_APP_KEY, request signed with
  Ed25519 or RSA-SHA256 private key from MOOMOO_RSA_PRIVATE_KEY (or
  MOOMOO_PRIVATE_KEY). Soft-fails with status=not_connected if the AppKey is
  set but the private key is missing. Falls back to classic OpenD
  get_top_movers_rank when OpenAPI creds are absent and OpenD is reachable
  at MOOMOO_OPEND_HOST:MOOMOO_OPEND_PORT.

Ingest (Railway path):
  Local OpenD cannot be reached from Railway. A token-authed
  POST /api/gainers/ingest lets the trading box push Moomoo rows into an
  in-memory cache keyed by list_type (premarket|today|afterhours).
  GET /api/gainers?source=moomoo&list=today prefers that cache when fresh
  so the Moomoo tab stays populated. Webull uses public rankType
  1d / preMarket / afterMarket for the same three list types.
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

# Moomoo OpenAPI (gateway-free REST) — https://webapi.moomoo.com
MOOMOO_WEBAPI = "https://webapi.moomoo.com"
MOOMOO_STOCK_SCREEN_PATH = "/api/v1.0/quote/stock-screen"
# Stock-screen enums (from moomoo/futu stock_screen_const)
_SCR_FIELD_MARKET = 1
_SCR_MARKET_US = 2
_PROP_PRICE = 2201            # SimpleProperty.PRICE
_PROP_CHANGE_RATE = 2206      # SimpleProperty.PRICE_CHANGE_RATE (% points)
_PROP_AVG_VOLUME = 3104       # CumulativeProperty.AVG_VOLUME
_SORT_DESC = 2
# PRICE response ival is typically *1000
_PRICE_MULT = 1000.0

CACHE_TTL_SEC = 120
# Ingested Moomoo rows from the local OpenD pusher. Prefer while fresh;
# still serve (marked stale) so Last checked is never silent.
INGEST_FRESH_SEC = 600       # 10 min — prefer ingest over live OpenAPI
INGEST_STALE_SHOW_SEC = 3600  # 60 min — still show with stale banner
LIST_TYPES = ("premarket", "today", "afterhours")
LIST_TYPE_ALIASES = {
    "pre": "premarket",
    "pre_market": "premarket",
    "pre-market": "premarket",
    "premkt": "premarket",
    "regular": "today",
    "session": "today",
    "intraday": "today",
    "day": "today",
    "1d": "today",
    "ah": "afterhours",
    "after": "afterhours",
    "after_hours": "afterhours",
    "after-hours": "afterhours",
    "aftermarket": "afterhours",
    "post": "afterhours",
    "postmarket": "afterhours",
}
WEBULL_RANK_BY_LIST = {
    "today": "1d",
    "premarket": "preMarket",
    "afterhours": "afterMarket",
}
LIST_LABELS = {
    "premarket": "Pre-market top gainers",
    "today": "Today's top gainers",
    "afterhours": "After-hours top gainers",
}

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
# Local OpenD → Railway ingest cache (separate from live OpenAPI/OpenD fetch).
# Shape matches live payloads + ingested_mono / ingest_source.
_ingest_cache: dict[str, dict[str, Any]] = {}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_list_type(value: str | None) -> str:
    raw = (value or "today").strip().lower()
    if raw in LIST_TYPES:
        return raw
    return LIST_TYPE_ALIASES.get(raw, "today")


def _cache_key(source: str, list_type: str) -> str:
    return f"{(source or '').strip().lower()}:{normalize_list_type(list_type)}"


def _ingest_key(source: str, list_type: str) -> str:
    return _cache_key(source, list_type)



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


async def fetch_webull(list_type: str = "today") -> dict[str, Any]:
    """Fetch US top gainers from Webull public ranking API for a session list."""
    lt = normalize_list_type(list_type)
    rank_type = WEBULL_RANK_BY_LIST.get(lt, "1d")
    params = {
        "regionId": 6,
        "rankType": rank_type,
        "pageIndex": 1,
        "pageSize": 50,
    }
    endpoint = (
        f"{WEBULL_URL}?regionId=6&rankType={rank_type}&pageIndex=1&pageSize=50"
    )
    session_label = LIST_LABELS.get(lt, lt)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(
                WEBULL_URL,
                params=params,
                headers={
                    "User-Agent": "desk-box/1.0",
                    "Accept": "application/json",
                },
            )
            r.raise_for_status()
            payload = r.json()
    except Exception as e:
        log.warning("webull gainers fetch failed (%s): %s", lt, e)
        return {
            "items": [],
            "status": "error",
            "message": f"Webull {session_label} failed: {e}",
            "reason": str(e),
            "updated_at": _now_iso(),
            "endpoint": endpoint,
            "list_type": lt,
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
        row = _row(
            symbol=str(symbol).strip().upper(),
            name=(ticker.get("name") or None),
            last=price,
            change_pct=change_pct,
            volume=ticker.get("volume") or values.get("volume"),
            source="webull",
        )
        row["list_type"] = lt
        items.append(row)

    return {
        "items": items,
        "status": "ok" if items else "empty",
        "message": (
            f"Webull US {session_label} ({len(items)} names)"
            if items
            else f"Webull returned no {session_label.lower()} right now"
        ),
        "updated_at": _now_iso(),
        "endpoint": endpoint,
        "list_type": lt,
    }


def _env_nonempty(*names: str) -> str:
    for n in names:
        v = os.environ.get(n)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def _looks_like_pem(value: str) -> bool:
    u = value.upper()
    return (
        "BEGIN PRIVATE KEY" in u
        or "BEGIN RSA PRIVATE KEY" in u
        or "BEGIN OPENSSH PRIVATE KEY" in u
        or "BEGIN ED25519 PRIVATE KEY" in u
    )


def _moomoo_creds() -> dict[str, str]:
    """Detect AppKey vs PEM without logging secret values."""
    raw_key = _env_nonempty("MOOMOO_APP_KEY")
    pem = _env_nonempty(
        "MOOMOO_RSA_PRIVATE_KEY",
        "MOOMOO_PRIVATE_KEY",
        "MOOMOO_ED25519_PRIVATE_KEY",
    )
    secret = _env_nonempty("MOOMOO_APP_SECRET")
    app_key = ""
    private_pem = pem

    if raw_key and _looks_like_pem(raw_key):
        # User stored the PEM in MOOMOO_APP_KEY — treat as private key;
        # look for a separate AppKey id.
        private_pem = private_pem or raw_key
        app_key = _env_nonempty("MOOMOO_APP_KEY_ID", "MOOMOO_APPID", "MOOMOO_CLIENT_ID")
    elif raw_key:
        app_key = raw_key

    if not private_pem and secret and _looks_like_pem(secret):
        private_pem = secret

    return {
        "app_key": app_key,
        "private_pem": private_pem,
        "app_key_len": str(len(app_key)),
        "private_pem_len": str(len(private_pem)),
    }


def _sign_moomoo_request(
    *,
    private_pem: str,
    timestamp_ms: str,
    method: str,
    path: str,
    query_string: str,
    body: bytes,
) -> str:
    """Sign per open.moomoo.com Getting Started (Ed25519 or RSA-SHA256)."""
    import base64
    import hashlib

    if body:
        body_part = hashlib.sha256(body).hexdigest()
    else:
        body_part = ""
    signing = (
        f"{timestamp_ms}\n{method.upper()}\n{path}\n{query_string}\n{body_part}"
    )
    signing_bytes = signing.encode("utf-8")

    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ed25519, padding, rsa
    from cryptography.hazmat.primitives.serialization import load_pem_private_key

    # Accept PEM or OpenSSH private key bytes.
    key_bytes = private_pem.encode("utf-8") if isinstance(private_pem, str) else private_pem
    # Normalize escaped newlines from env/Railway.
    if b"\\n" in key_bytes and b"-----BEGIN" in key_bytes:
        key_bytes = key_bytes.replace(b"\\n", b"\n")
    try:
        key = load_pem_private_key(key_bytes, password=None)
    except Exception:
        # OpenSSH format
        from cryptography.hazmat.primitives.serialization import (
            load_ssh_private_key,
        )
        key = load_ssh_private_key(key_bytes, password=None)

    if isinstance(key, ed25519.Ed25519PrivateKey):
        sig = key.sign(signing_bytes)
    elif isinstance(key, rsa.RSAPrivateKey):
        sig = key.sign(signing_bytes, padding.PKCS1v15(), hashes.SHA256())
    else:
        raise TypeError(f"unsupported private key type: {type(key).__name__}")
    return base64.b64encode(sig).decode("ascii")


def _parse_screen_result_value(result_obj: dict[str, Any], *, prop_id: int) -> float | None:
    """Extract a float from a stock-screen results[] wrapper."""
    if not isinstance(result_obj, dict):
        return None
    # Prefer typed wrappers from REST
    for wrap_key in (
        "simple_property_result",
        "cumulative_property_result",
        "basic_property_result",
    ):
        wrap = result_obj.get(wrap_key)
        if isinstance(wrap, dict):
            result_obj = wrap
            break

    res = result_obj.get("res") if isinstance(result_obj.get("res"), dict) else {}
    # dval is already float when present (OpenD-style / some REST)
    for candidate in (
        result_obj.get("dval"),
        res.get("dval"),
        result_obj.get("value"),
        res.get("sval"),
        result_obj.get("sval"),
        res.get("ival"),
        result_obj.get("ival"),
    ):
        n = _num(candidate)
        if n is not None:
            # PRICE ival is typically multiplied by 1000 on REST
            if prop_id == _PROP_PRICE and abs(n) >= 10000:
                return n / _PRICE_MULT
            # CHANGE_RATE sometimes arrives as micro-percent (15000 -> 15.0)
            if prop_id == _PROP_CHANGE_RATE and abs(n) >= 1000:
                return n / _PRICE_MULT
            return n
    return None


def _fetch_moomoo_openapi() -> dict[str, Any] | None:
    """Try gateway-free Moomoo OpenAPI. Returns None to fall through to OpenD."""
    creds = _moomoo_creds()
    app_key = creds["app_key"]
    private_pem = creds["private_pem"]

    if not app_key and not private_pem:
        return None  # no OpenAPI creds — try OpenD

    if app_key and not private_pem:
        return {
            "items": [],
            "status": "not_connected",
            "message": (
                "Moomoo OpenAPI AppKey is set but the signing private key is "
                "missing"
            ),
            "reason": (
                "MOOMOO_APP_KEY is present "
                f"(len={creds['app_key_len']}) but Moomoo Traditional API Key "
                "auth also requires the Ed25519/RSA private key that matches "
                "the public key uploaded at https://open.moomoo.com/dashboard. "
                "Set MOOMOO_RSA_PRIVATE_KEY (PEM, including BEGIN/END lines; "
                " Railway: use \\n for newlines) on the desk-box service. "
                "Optional alias: MOOMOO_PRIVATE_KEY."
            ),
            "updated_at": _now_iso(),
            "endpoint": f"{MOOMOO_WEBAPI}{MOOMOO_STOCK_SCREEN_PATH}",
            "auth": "openapi_appkey_missing_private_key",
        }

    if private_pem and not app_key:
        return {
            "items": [],
            "status": "not_connected",
            "message": "Moomoo private key is set but AppKey id is missing",
            "reason": (
                "Found a PEM private key but no AppKey id. Set MOOMOO_APP_KEY "
                "to the AppKey from https://open.moomoo.com/dashboard "
                "(or MOOMOO_APP_KEY_ID if the PEM is stored in MOOMOO_APP_KEY)."
            ),
            "updated_at": _now_iso(),
            "endpoint": f"{MOOMOO_WEBAPI}{MOOMOO_STOCK_SCREEN_PATH}",
            "auth": "openapi_private_key_missing_appkey",
        }

    # US top gainers via stock-screen sorted by PRICE_CHANGE_RATE DESC
    body_obj = {
        "limit": 50,
        "screen_queries": [
            {
                "simple_field_query": {
                    "simple_field": _SCR_FIELD_MARKET,
                    "screen_value_list": [_SCR_MARKET_US],
                }
            },
            # Prefer names that actually moved today
            {
                "simple_property_query": {
                    "property": {"name": _PROP_CHANGE_RATE},
                    "filterMin": {"value": 0.01, "includes": True},
                }
            },
        ],
        "retrieve_queries": [
            {"simple_property": {"name": _PROP_PRICE}},
            {"simple_property": {"name": _PROP_CHANGE_RATE}},
            {"cumulative_property": {"name": _PROP_AVG_VOLUME, "days": 1}},
        ],
        "sort": {
            "direction": _SORT_DESC,
            "simple_property": {"name": _PROP_CHANGE_RATE},
        },
    }
    import json as _json
    import secrets as _secrets

    body = _json.dumps(body_obj, separators=(",", ":")).encode("utf-8")
    path = MOOMOO_STOCK_SCREEN_PATH
    method = "POST"
    query_string = ""
    timestamp_ms = str(int(time.time() * 1000))
    nonce = _secrets.token_hex(16)

    try:
        signature = _sign_moomoo_request(
            private_pem=private_pem,
            timestamp_ms=timestamp_ms,
            method=method,
            path=path,
            query_string=query_string,
            body=body,
        )
    except Exception as e:
        log.warning("moomoo openapi sign failed: %s", type(e).__name__)
        return {
            "items": [],
            "status": "not_connected",
            "message": "Moomoo OpenAPI private key could not be loaded/signed",
            "reason": (
                f"Failed to load/sign with MOOMOO_RSA_PRIVATE_KEY "
                f"({type(e).__name__}). Expect PEM Ed25519 or RSA private key "
                "matching the public key uploaded for this AppKey."
            ),
            "updated_at": _now_iso(),
            "endpoint": f"{MOOMOO_WEBAPI}{path}",
            "auth": "openapi_sign_error",
        }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Api-Key": app_key,
        "X-Timestamp": timestamp_ms,
        "X-Nonce": nonce,
        "Authorization": signature,
        "User-Agent": "desk-box/1.0",
    }

    try:
        with httpx.Client(timeout=8.0) as client:
            r = client.post(
                f"{MOOMOO_WEBAPI}{path}",
                content=body,
                headers=headers,
            )
            # Do not log response body if it might echo auth errors with keys
            if r.status_code >= 400:
                # Keep message short; never include Authorization/AppKey
                snippet = (r.text or "")[:180].replace(app_key, "***")
                return {
                    "items": [],
                    "status": "error",
                    "message": f"Moomoo OpenAPI HTTP {r.status_code}",
                    "reason": snippet or r.reason_phrase,
                    "updated_at": _now_iso(),
                    "endpoint": f"{MOOMOO_WEBAPI}{path}",
                    "auth": "openapi_http_error",
                }
            payload = r.json()
    except Exception as e:
        log.warning("moomoo openapi fetch failed: %s", type(e).__name__)
        return {
            "items": [],
            "status": "error",
            "message": f"Moomoo OpenAPI request failed: {type(e).__name__}",
            "reason": str(e)[:240],
            "updated_at": _now_iso(),
            "endpoint": f"{MOOMOO_WEBAPI}{path}",
            "auth": "openapi_request_error",
        }

    ret_code = payload.get("ret_code")
    if ret_code not in (0, "0", None):
        return {
            "items": [],
            "status": "error",
            "message": f"Moomoo OpenAPI ret_code={ret_code}",
            "reason": str(payload.get("ret_msg") or payload.get("error") or "")[:240],
            "updated_at": _now_iso(),
            "endpoint": f"{MOOMOO_WEBAPI}{path}",
            "auth": "openapi_ret_error",
        }

    data = payload.get("data") or {}
    raw_items = data.get("items") or payload.get("items") or []
    items: list[dict[str, Any]] = []
    for entry in raw_items:
        if not isinstance(entry, dict):
            continue
        code = str(entry.get("code") or "")
        # US.AAPL -> AAPL
        symbol = code.split(".", 1)[-1] if code else ""
        if not symbol:
            continue
        results = entry.get("results") or []
        price = None
        change_pct = None
        volume = None
        # retrieve order: price, change_rate, avg_volume
        if len(results) > 0:
            price = _parse_screen_result_value(results[0], prop_id=_PROP_PRICE)
        if len(results) > 1:
            change_pct = _parse_screen_result_value(
                results[1], prop_id=_PROP_CHANGE_RATE
            )
        if len(results) > 2:
            volume = _parse_screen_result_value(
                results[2], prop_id=_PROP_AVG_VOLUME
            )
        # Fallback: scan results for property ids
        if price is None or change_pct is None:
            for res in results:
                wrap = res
                for k in (
                    "simple_property_result",
                    "cumulative_property_result",
                ):
                    if isinstance(res.get(k), dict):
                        wrap = res[k]
                        break
                prop = (wrap.get("property") or {}) if isinstance(wrap, dict) else {}
                name = prop.get("name")
                if name == _PROP_PRICE and price is None:
                    price = _parse_screen_result_value(wrap, prop_id=_PROP_PRICE)
                elif name == _PROP_CHANGE_RATE and change_pct is None:
                    change_pct = _parse_screen_result_value(
                        wrap, prop_id=_PROP_CHANGE_RATE
                    )
                elif name == _PROP_AVG_VOLUME and volume is None:
                    volume = _parse_screen_result_value(
                        wrap, prop_id=_PROP_AVG_VOLUME
                    )
        name = entry.get("name") or entry.get("sc_name") or entry.get("tc_name")
        items.append(
            _row(
                symbol=symbol.strip().upper(),
                name=(str(name) if name else None),
                last=price,
                change_pct=change_pct,
                volume=volume,
                source="moomoo",
            )
        )

    return {
        "items": items,
        "status": "ok" if items else "empty",
        "message": (
            f"Moomoo US top gainers via OpenAPI ({len(items)} names)"
            if items
            else "Moomoo OpenAPI returned no gainers right now"
        ),
        "updated_at": _now_iso(),
        "endpoint": f"{MOOMOO_WEBAPI}{path}",
        "auth": "openapi_appkey",
    }


def _fetch_moomoo_opend_sync() -> dict[str, Any]:
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
        "Prefer Moomoo OpenAPI (MOOMOO_APP_KEY + MOOMOO_RSA_PRIVATE_KEY) on "
        "Railway. Classic OpenD needs MOOMOO_OPEND_HOST/PORT pointed at a "
        "reachable gateway — local desk-box OpenD binds 127.0.0.1:11111."
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
            "auth": "opend",
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
                "auth": "opend",
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
                "auth": "opend",
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
            "auth": "opend",
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
            "auth": "opend",
        }
    finally:
        if ctx is not None:
            try:
                ctx.close()
            except Exception:
                pass


def _fetch_moomoo_sync() -> dict[str, Any]:
    """Prefer OpenAPI AppKey path; fall back to OpenD when no OpenAPI creds."""
    openapi = _fetch_moomoo_openapi()
    if openapi is not None:
        return openapi
    return _fetch_moomoo_opend_sync()


async def fetch_moomoo() -> dict[str, Any]:
    return await asyncio.to_thread(_fetch_moomoo_sync)



def ingest_gainers(
    source: str,
    items: list[dict[str, Any]] | None,
    *,
    list_type: str = "today",
    status: str | None = None,
    message: str | None = None,
    updated_at: str | None = None,
    auth: str | None = None,
    opend: str | None = None,
    endpoint: str | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    """Store a pusher payload for source+list_type (typically moomoo). Sync-safe."""
    src = (source or "moomoo").strip().lower()
    if src not in ("moomoo", "webull"):
        src = "moomoo"
    lt = normalize_list_type(list_type)
    session_label = LIST_LABELS.get(lt, lt)
    clean: list[dict[str, Any]] = []
    for raw in items or []:
        if not isinstance(raw, dict):
            continue
        sym = str(raw.get("symbol") or "").strip().upper()
        if not sym:
            continue
        row = dict(raw)
        row["symbol"] = sym
        row.setdefault("source", src)
        row.setdefault("sources", [src])
        row["list_type"] = lt
        clean.append(row)
    if status is None:
        status = "ok" if clean else "empty"
    if message is None:
        if src == "moomoo" and clean:
            message = (
                f"Moomoo US {session_label} via local OpenD ingest "
                f"({len(clean)} names)"
            )
        elif not clean:
            message = f"{src} {session_label.lower()} ingest empty"
        else:
            message = f"{src} {session_label.lower()} ingest ({len(clean)} names)"
    payload: dict[str, Any] = {
        "items": clean,
        "status": status,
        "message": message,
        "updated_at": updated_at or _now_iso(),
        "fetched_mono": time.monotonic(),
        "ingested_mono": time.monotonic(),
        "ingest_source": "local_opend_push",
        "auth": auth or "opend_ingest",
        "list_type": lt,
    }
    if opend:
        payload["opend"] = opend
    if endpoint:
        payload["endpoint"] = endpoint
    if reason:
        payload["reason"] = reason
    key = _ingest_key(src, lt)
    _ingest_cache[key] = payload
    # Backward-compat: bare source key mirrors "today"
    if lt == "today":
        _ingest_cache[src] = payload
    return {
        k: v
        for k, v in payload.items()
        if k not in ("fetched_mono", "ingested_mono")
    }


def ingest_gainers_batch(
    source: str,
    lists: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Ingest multiple list_type payloads; each keeps its own updated_at."""
    results: list[dict[str, Any]] = []
    for entry in lists or []:
        if not isinstance(entry, dict):
            continue
        lt = normalize_list_type(
            entry.get("list_type") or entry.get("list") or "today"
        )
        stored = ingest_gainers(
            source,
            entry.get("items") if isinstance(entry.get("items"), list) else [],
            list_type=lt,
            status=entry.get("status"),
            message=entry.get("message"),
            updated_at=entry.get("updated_at"),
            auth=entry.get("auth"),
            opend=entry.get("opend"),
            endpoint=entry.get("endpoint"),
            reason=entry.get("reason"),
        )
        results.append(
            {
                "ok": True,
                "source": (source or "moomoo").strip().lower() or "moomoo",
                "list_type": lt,
                "count": len(stored.get("items") or []),
                "status": stored.get("status"),
                "message": stored.get("message"),
                "updated_at": stored.get("updated_at"),
                "auth": stored.get("auth"),
            }
        )
    return results


def _ingest_age_sec(entry: dict[str, Any]) -> float | None:
    mono = entry.get("ingested_mono") or entry.get("fetched_mono")
    if mono is None:
        return None
    return time.monotonic() - float(mono)


def peek_ingest(source: str, list_type: str = "today") -> dict[str, Any] | None:
    """Return ingest payload without mono clocks, or None if missing/expired."""
    src = (source or "").strip().lower()
    lt = normalize_list_type(list_type)
    entry = _ingest_cache.get(_ingest_key(src, lt)) or {}
    # Legacy: older single-key cache treated as today
    if not entry and lt == "today":
        entry = _ingest_cache.get(src) or {}
    if not entry:
        return None
    age = _ingest_age_sec(entry)
    if age is None or age > INGEST_STALE_SHOW_SEC:
        return None
    out = {
        k: v
        for k, v in entry.items()
        if k not in ("fetched_mono", "ingested_mono")
    }
    out.setdefault("list_type", lt)
    out["_ingest_age_sec"] = age
    out["_ingest_fresh"] = age <= INGEST_FRESH_SEC
    if age > INGEST_FRESH_SEC:
        base = out.get("message") or f"{src} {lt} ingest"
        out["message"] = (
            f"{base} (stale ingest · {int(age)}s old — waiting for next local push)"
        )
        if out.get("status") == "ok":
            out["status"] = "partial"
    return out


async def _moomoo_with_ingest(
    *, force: bool = False, list_type: str = "today"
) -> dict[str, Any]:
    """Prefer local OpenD ingest cache for Moomoo; fall back to live fetch.

    Live OpenAPI/OpenD on Railway only covers the regular-session (today)
    board. Premarket/afterhours rely on the local ingest pusher; we still
    stamp Last checked from ingest even when the session list is empty.
    """
    lt = normalize_list_type(list_type)
    ingest = peek_ingest("moomoo", lt)
    if ingest and ingest.get("_ingest_fresh") and not force:
        return {k: v for k, v in ingest.items() if not k.startswith("_")}

    # Premarket/AH: do not call Railway OpenAPI path (regular-session only).
    if lt != "today":
        if ingest:
            return {k: v for k, v in ingest.items() if not k.startswith("_")}
        return {
            "items": [],
            "status": "empty",
            "message": (
                f"Moomoo {LIST_LABELS.get(lt, lt)} waiting for local OpenD ingest"
            ),
            "updated_at": _now_iso(),
            "list_type": lt,
            "auth": "opend_ingest",
        }

    live = await _get_cached("moomoo", force=force, list_type=lt)
    live_ok = (live.get("status") == "ok") and bool(live.get("items"))

    if live_ok:
        live = dict(live)
        live["list_type"] = lt
        return live

    if ingest:
        # Live empty/not_connected/error — serve ingest (fresh or stale banner).
        return {k: v for k, v in ingest.items() if not k.startswith("_")}

    live = dict(live)
    live.setdefault("list_type", lt)
    return live


def _stale(entry: dict[str, Any]) -> bool:
    if not entry:
        return True
    fetched = entry.get("fetched_mono")
    if fetched is None:
        return True
    return (time.monotonic() - fetched) > CACHE_TTL_SEC


async def _refresh_one(
    source: str, *, list_type: str = "today"
) -> dict[str, Any]:
    lt = normalize_list_type(list_type)
    key = _cache_key(source, lt)
    if source == "webull":
        result = await fetch_webull(lt)
    elif source == "moomoo":
        # Live Moomoo path is regular-session only.
        if lt != "today":
            result = {
                "items": [],
                "status": "empty",
                "message": (
                    f"Moomoo live OpenAPI covers today's board only — "
                    f"use local ingest for {LIST_LABELS.get(lt, lt)}"
                ),
                "updated_at": _now_iso(),
                "list_type": lt,
                "auth": "openapi_appkey",
            }
        else:
            result = await fetch_moomoo()
            result = dict(result)
            result["list_type"] = lt
    else:
        raise ValueError(source)
    result["fetched_mono"] = time.monotonic()
    result.setdefault("list_type", lt)
    async with _lock:
        _cache[key] = result
        # Mirror legacy bare-source key for today so older callers keep working.
        if lt == "today":
            _cache[source] = result
    return result


async def refresh_all() -> None:
    # Warm today's boards (UI default). Premarket/AH come from ingest.
    await asyncio.gather(
        _refresh_one("webull", list_type="today"),
        _refresh_one("moomoo", list_type="today"),
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


def _source_phrase(name: str, payload: dict[str, Any]) -> str:
    """Short honest fragment for Combined labels (Webull; Moomoo empty)."""
    status = (payload.get("status") or "not_connected").lower()
    count = len(payload.get("items") or [])
    if status == "ok" and count:
        return f"{name}"
    if status == "empty":
        return f"{name} empty"
    if status == "error":
        return f"{name} error"
    if status == "partial":
        return f"{name} partial"
    return f"{name} not connected"


def _merge(
    webull: dict[str, Any],
    moomoo: dict[str, Any],
    *,
    list_type: str = "today",
) -> dict[str, Any]:
    lt = normalize_list_type(list_type)
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
                # Single-source rows keep a plain source label.
                row["source"] = (
                    row["sources"][0]
                    if len(row["sources"]) == 1
                    else ",".join(row["sources"])
                )
                row["list_type"] = lt
                by_sym[sym] = row
                continue
            srcs = list(existing.get("sources") or [])
            for s in (item.get("sources") or [item.get("source")]):
                if s and s not in srcs:
                    srcs.append(s)
            existing["sources"] = srcs
            existing["source"] = (
                ",".join(srcs)
                if len(srcs) > 1
                else (srcs[0] if srcs else existing.get("source"))
            )
            # Keep the quote with the larger absolute move as display price.
            old_pct = _num(existing.get("change_pct")) or 0.0
            new_pct = _num(item.get("change_pct")) or 0.0
            if abs(new_pct) > abs(old_pct):
                for k in ("last", "price", "change", "change_pct", "volume", "name"):
                    if k in item:
                        existing[k] = item[k]

    items = sorted(
        by_sym.values(),
        key=lambda r: (
            _num(r.get("change_pct")) is not None,
            _num(r.get("change_pct")) or 0.0,
        ),
        reverse=True,
    )

    w_status = (webull.get("status") or "not_connected").lower()
    m_status = (moomoo.get("status") or "not_connected").lower()
    # empty = auth/query ok, soft no-rows (e.g. after hours) — not a disconnect.
    w_live = w_status in ("ok", "empty")
    m_live = m_status in ("ok", "empty")
    w_ok = w_status == "ok" and bool(webull.get("items"))
    m_ok = m_status == "ok" and bool(moomoo.get("items"))

    w_phrase = _source_phrase("Webull", webull)
    m_phrase = _source_phrase("Moomoo", moomoo)
    session = LIST_LABELS.get(lt, lt)
    honest_label = f"Combined {session} ({w_phrase}; {m_phrase})"

    if w_ok and m_ok:
        status, message = "ok", (
            f"{honest_label} · {len(items)} names, deduped"
        )
    elif w_ok or m_ok:
        status, message = "partial", (
            f"{honest_label} · showing "
            f"{'Webull' if w_ok else 'Moomoo'} rows only"
            + (
                f" — {moomoo.get('message')}" if w_ok and not m_ok else ""
            )
            + (
                f" — {webull.get('message')}" if m_ok and not w_ok else ""
            )
        )
    elif w_live or m_live:
        status, message = "empty", (
            f"{honest_label} · no gainers right now"
        )
    else:
        status, message = "not_connected", (
            f"{honest_label}. "
            f"Webull: {webull.get('message')}; Moomoo: {moomoo.get('message')}"
        )

    # Prefer the freshest updated_at among sources when both stamped.
    stamps = [
        t for t in (webull.get("updated_at"), moomoo.get("updated_at")) if t
    ]
    updated = max(stamps) if stamps else _now_iso()

    return {
        "items": items,
        "status": status,
        "message": message,
        "label": honest_label,
        "updated_at": updated,
        "list_type": lt,
        "sources": {
            "webull": {
                "status": webull.get("status"),
                "message": webull.get("message"),
                "count": len(webull.get("items") or []),
                "updated_at": webull.get("updated_at"),
                "endpoint": webull.get("endpoint"),
                "list_type": lt,
            },
            "moomoo": {
                "status": moomoo.get("status"),
                "message": moomoo.get("message"),
                "reason": moomoo.get("reason"),
                "count": len(moomoo.get("items") or []),
                "updated_at": moomoo.get("updated_at"),
                "opend": moomoo.get("opend"),
                "endpoint": moomoo.get("endpoint"),
                "auth": moomoo.get("auth"),
                "list_type": lt,
            },
        },
    }


async def _get_cached(
    source: str, *, force: bool = False, list_type: str = "today"
) -> dict[str, Any]:
    lt = normalize_list_type(list_type)
    key = _cache_key(source, lt)
    async with _lock:
        entry = dict(_cache.get(key) or _cache.get(source) or {})
        # If legacy bare key is for a different list, ignore it.
        if entry.get("list_type") and normalize_list_type(entry.get("list_type")) != lt:
            if key not in _cache:
                entry = {}
    if force or _stale(entry):
        try:
            entry = await asyncio.wait_for(
                _refresh_one(source, list_type=lt), timeout=12.0
            )
        except asyncio.TimeoutError:
            log.warning("%s/%s gainers refresh timed out", source, lt)
            if entry.get("items") is not None and entry.get("status"):
                entry = dict(entry)
                entry["message"] = (
                    f"{entry.get('message') or source} "
                    f"(refresh timed out — showing last check)"
                )
            else:
                entry = {
                    "items": [],
                    "status": "error",
                    "message": f"{source} {LIST_LABELS.get(lt, lt)} timed out",
                    "reason": "upstream refresh exceeded 12s budget",
                    "updated_at": _now_iso(),
                    "fetched_mono": time.monotonic(),
                    "list_type": lt,
                }
                async with _lock:
                    if not _cache.get(key):
                        _cache[key] = entry
    out = {k: v for k, v in entry.items() if k != "fetched_mono"}
    out.setdefault("list_type", lt)
    return out


async def get_gainers(
    source: str = "combined",
    force: bool = False,
    list_type: str = "today",
) -> dict[str, Any]:
    """Return gainers for one tab + session list.

    force=True bypasses cache (Refresh button). Caps total wait so
    Railway/proxy does not 502 the browser. On timeout, returns last-known
    rows when available with an honest message — never invents prices.
    """
    src = (source or "combined").strip().lower()
    if src not in ("combined", "moomoo", "webull"):
        src = "combined"
    lt = normalize_list_type(list_type)
    session = LIST_LABELS.get(lt, lt)

    async def _load() -> dict[str, Any]:
        if src == "webull":
            return await _get_cached("webull", force=force, list_type=lt)
        if src == "moomoo":
            return await _moomoo_with_ingest(force=force, list_type=lt)
        webull, moomoo = await asyncio.gather(
            _get_cached("webull", force=force, list_type=lt),
            _moomoo_with_ingest(force=force, list_type=lt),
        )
        return _merge(webull, moomoo, list_type=lt)

    try:
        data = await asyncio.wait_for(_load(), timeout=14.0)
    except asyncio.TimeoutError:
        log.warning("get_gainers(%s/%s) overall timeout", src, lt)
        async with _lock:
            w = {
                k: v
                for k, v in (
                    _cache.get(_cache_key("webull", lt))
                    or _cache.get("webull")
                    or {}
                ).items()
                if k != "fetched_mono"
            }
            m = {
                k: v
                for k, v in (
                    _cache.get(_cache_key("moomoo", lt))
                    or _cache.get("moomoo")
                    or {}
                ).items()
                if k != "fetched_mono"
            }
        if src == "webull":
            data = w or {
                "items": [],
                "status": "error",
                "message": f"Webull {session} timed out — try Refresh again",
                "updated_at": _now_iso(),
                "list_type": lt,
            }
        elif src == "moomoo":
            ingest = peek_ingest("moomoo", lt)
            if ingest:
                data = {k: v for k, v in ingest.items() if not k.startswith("_")}
                data["message"] = (
                    (data.get("message") or "Moomoo ingest")
                    + " — live refresh timed out, showing ingest"
                )
            else:
                data = m or {
                    "items": [],
                    "status": "error",
                    "message": f"Moomoo {session} timed out — try Refresh again",
                    "updated_at": _now_iso(),
                    "list_type": lt,
                }
        else:
            data = _merge(
                w
                or {
                    "items": [],
                    "status": "error",
                    "message": "Webull timed out",
                    "list_type": lt,
                },
                m
                or {
                    "items": [],
                    "status": "error",
                    "message": "Moomoo timed out",
                    "list_type": lt,
                },
                list_type=lt,
            )
            data["message"] = (
                (data.get("message") or "Combined feed timed out")
                + " — try Refresh again"
            )

    label = data.get("label") or f"{LABELS[src]} · {session}"
    return {
        "ok": True,
        "source": src,
        "list_type": lt,
        "label": label,
        "items": data.get("items") or [],
        "updated_at": data.get("updated_at") or _now_iso(),
        "status": data.get("status") or "not_connected",
        "message": data.get("message") or f"{src} feed not connected",
        **{
            k: data[k]
            for k in ("reason", "endpoint", "opend", "auth", "sources")
            if k in data
        },
    }
