"""Top gainers from authorized moomoo and Webull routes only.

Nothing here invents a ticker. A source with no configured route stays
disconnected, and Combined never relabels the other feed as that broker.
"""
import os

import httpx

_LOADERS = {}


def use_loader(name, fn):
    """Test hook. Production leaves this empty."""
    if fn is None:
        _LOADERS.pop(name, None)
    else:
        _LOADERS[name] = fn


def _empty(state, detail):
    return {
        "state": state,
        "detail": detail,
        "retrieved_at": None,
        "session": None,
        "quote_delay_seconds": None,
        "rows": [],
    }


def _configured(name):
    if name in _LOADERS:
        return _LOADERS[name]()
    env = {"moomoo": "MOOMOO_GAINERS_URL", "webull": "WEBULL_GAINERS_URL"}.get(name)
    url = os.environ.get(env or "", "").strip()
    if not url:
        return _empty("disconnected", "No authorized data route is configured.")
    try:
        with httpx.Client(timeout=8) as client:
            response = client.get(url)
            response.raise_for_status()
            payload = response.json()
    except Exception:
        return _empty("error", "The source did not respond.")
    return _normalize(payload)


def _normalize(payload):
    if not isinstance(payload, dict):
        return _empty("error", "The source returned an unreadable payload.")
    rows = []
    for raw in payload.get("rows") or []:
        if not isinstance(raw, dict):
            continue
        ticker = str(raw.get("ticker") or "").strip().upper()
        if not ticker or raw.get("last") is None or raw.get("change_pct") is None:
            continue
        try:
            last = float(raw["last"])
            change = float(raw["change_pct"])
        except (TypeError, ValueError):
            continue
        rows.append({
            "ticker": ticker,
            "last": last,
            "change_pct": change,
            "session": raw.get("session") or payload.get("session"),
            "volume": raw.get("volume"),
            "relative_volume": raw.get("relative_volume"),
            "float": raw.get("float"),
            "catalyst": raw.get("catalyst"),
        })
    return {
        "state": "ok",
        "detail": "",
        "retrieved_at": payload.get("retrieved_at"),
        "session": payload.get("session"),
        "quote_delay_seconds": payload.get("quote_delay_seconds"),
        "rows": rows,
    }


def _public(row, source, retrieved_at):
    ticker = str(row["ticker"]).strip().upper()
    return {
        "ticker": ticker,
        "last": row["last"],
        "change_pct": row["change_pct"],
        "session": row.get("session"),
        "volume": row.get("volume"),
        "relative_volume": row.get("relative_volume"),
        "float": row.get("float"),
        "catalyst": row.get("catalyst"),
        "source": source,
        "retrieved_at": retrieved_at,
        "attributions": [{
            "source": source,
            "last": row["last"],
            "change_pct": row["change_pct"],
            "retrieved_at": retrieved_at,
        }],
    }


def top_gainers(source):
    source = source if source in ("combined", "moomoo", "webull") else "combined"
    if source != "combined":
        block = _configured(source)
        rows = [_public(row, source, block.get("retrieved_at")) for row in block.get("rows") or []]
        for index, row in enumerate(rows, 1):
            row["rank"] = index
        block = dict(block)
        block["source"] = source
        block["rows"] = rows
        block["parts"] = {source: {"state": block["state"], "detail": block.get("detail"),
                                   "retrieved_at": block.get("retrieved_at")}}
        return block

    parts = {name: _configured(name) for name in ("moomoo", "webull")}
    merged = {}
    for name, block in parts.items():
        if block.get("state") != "ok":
            continue
        for row in block.get("rows") or []:
            public = _public(row, name, block.get("retrieved_at"))
            key = (public["ticker"], public.get("session") or "")
            current = merged.get(key)
            if current is None:
                merged[key] = public
                continue
            current["attributions"].append(public["attributions"][0])
            if name not in current["source"].split(","):
                current["source"] = current["source"] + "," + name
    rows = sorted(merged.values(), key=lambda item: item["change_pct"], reverse=True)
    for index, row in enumerate(rows, 1):
        row["rank"] = index
    retrieved = [part.get("retrieved_at") for part in parts.values() if part.get("retrieved_at")]
    any_ok = any(part.get("state") == "ok" and part.get("rows") for part in parts.values())
    return {
        "source": "combined",
        "state": "ok" if any_ok else "disconnected",
        "detail": "" if any_ok else "No connected gainer source.",
        "retrieved_at": max(retrieved) if retrieved else None,
        "session": None,
        "quote_delay_seconds": None,
        "rows": rows,
        "parts": {
            name: {
                "state": part.get("state"),
                "detail": part.get("detail"),
                "retrieved_at": part.get("retrieved_at"),
            }
            for name, part in parts.items()
        },
    }


APPS = {
    "youtube": {
        "name": "YouTube",
        "package": "com.google.android.youtube",
        "web": "https://www.youtube.com/",
    },
    "facebook": {
        "name": "Facebook",
        "package": "com.facebook.katana",
        "web": "https://www.facebook.com/",
    },
    "snapchat": {
        "name": "Snapchat",
        "package": "com.snapchat.android",
        "web": "https://www.snapchat.com/",
    },
}


def integration_state(provider):
    """Account state is connected only when that provider's token is configured."""
    if provider in APPS:
        token_env = {
            "youtube": "YOUTUBE_ACCESS_TOKEN",
            "facebook": "FACEBOOK_ACCESS_TOKEN",
            "snapchat": "SNAPCHAT_ACCESS_TOKEN",
        }[provider]
        connected = bool(os.environ.get(token_env, "").strip())
        app = APPS[provider]
        return {
            "id": provider,
            "name": app["name"],
            "account": "connected" if connected else "not_connected",
            "capabilities": ["open"] + (["search"] if provider != "snapchat" else []),
            "web": app["web"],
            "android_package": app["package"],
            "detail": "Account API is configured." if connected else "No account is connected.",
        }
    if provider in ("moomoo", "webull"):
        block = _configured(provider)
        return {
            "id": provider,
            "name": "moomoo" if provider == "moomoo" else "Webull",
            "account": "connected" if block.get("state") == "ok" else "disconnected",
            "detail": block.get("detail") or "",
        }
    return None


def integrations():
    return {
        "apps": [integration_state(name) for name in ("youtube", "facebook", "snapchat")],
        "brokers": [integration_state(name) for name in ("moomoo", "webull")],
    }


def launch_target(provider):
    app = APPS.get(provider)
    if not app:
        return None
    return {"provider": provider, "web": app["web"], "android_package": app["package"]}
