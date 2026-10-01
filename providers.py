"""Live provider calls for the desk box.

Calls the xAI (Grok) Responses API and the Gemini generateContent API
directly, using API keys from the environment. Payload shapes mirror the
desk's working relay scripts (grok_ask.py / gemini_ask.py), which are
live-verified daily.

Environment:
    XAI_API_KEY       xAI API key (Grok)
    GEMINI_API_KEY    Google AI Studio API key (Gemini)
    GROK_MODEL        override, default grok-4.5
    GEMINI_MODEL      override, default gemini-3.8-flash

Set MOCK_PROVIDERS=1 to return canned replies instead of calling the
APIs (local dev/test only — never in production).
"""
import os

import httpx

XAI_URL = "https://api.x.ai/v1/responses"
GEMINI_BASE = "https://generativelanguage.googleapis.com/v1beta"

GROK_MODEL = os.environ.get("GROK_MODEL", "grok-4.5")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.8-flash")
MOCK = os.environ.get("MOCK_PROVIDERS") == "1"

DESK_HEADER = """You are one of Shavor's AI partners in his private room, alongside Ace (Muse) and \
the Night Desk crew. This room is his all-purpose command center: stock trading, research, \
learning, tracking daily life, and answering whatever questions he brings — not trading-only. \
Answer anything he asks, in plain language, with live search when facts matter.

Trading context to honor when money is on the table: he is chasing $1M trading profit by Sep \
2027 (working pace 8-10%/week). Standing rules: max 3 names, max $1,500/name, risk <=0.5%/trade, \
kill switches -$150 day / -$450 week / -$900 month. GLND stock is sold (qty 0) — the old $7 waiting-sell lock is retired. Do not brief "Sell GLND at $7" or treat GLND stock as open; confirm the live book before mentioning any GLND residue.

Room protocol — his standing rule, flat and free: no one answers to anyone; everyone talks \
freely. ALWAYS answer his messages with your own take — never stay silent waiting to be "the \
most relevant voice." Speak as yourself only; never relay, summarize, or impersonate another \
partner — no one speaks through anyone. If another partner's reply fails or is missing, cover \
for them and say so. Direct, plain, no fluff. His message:\n\n"""

CROSSTALK_HEADER = """You are on Shavor's trading desk. Below is one message from Shavor, then the \
other AI desk partner's take on it. React to their take in 3-5 sentences: where you agree, where \
you disagree, and what Shavor should actually do. Direct, plain, no fluff.\n\n"""


def _need_key(name):
    key = os.environ.get(name)
    if not key:
        raise RuntimeError(
            f"{name} is not set — add it to the server's environment and restart.")
    return key


def _mock_reply(provider, prompt):
    return (f"[mock {provider} reply — dev mode, no API call made] "
            f"Got it: {prompt[:80]}...")


def _extract_grok_text(result):
    texts = []
    for item in result.get("output", []) or []:
        if item.get("type") == "message":
            for chunk in item.get("content", []) or []:
                if chunk.get("type") == "output_text" and chunk.get("text"):
                    texts.append(chunk["text"])
    return "\n".join(texts).strip()


def _extract_gemini_text(result):
    texts = []
    for cand in result.get("candidates", []) or []:
        content = cand.get("content", {}) or {}
        for part in content.get("parts", []) or []:
            if part.get("text"):
                texts.append(part["text"])
    return "\n".join(texts).strip()


# Shared room memory, maintained by Ace and pushed from the main chat.
# app.py sets this at startup and whenever Ace posts a new digest.
# Injected into every prompt so both partners remember past conversations.
ROOM_MEMORY = ""


def _with_memory(header):
    mem = ROOM_MEMORY.strip()
    if not mem:
        return header
    lead = "His message:\n\n"
    intro = header[:-len(lead)] if header.endswith(lead) else header
    return (intro + "\nShared room memory — durable facts, decisions, and running threads "
            "from earlier conversations. Treat as true unless his new message contradicts it:\n"
            + mem + "\n\n" + lead)


async def ask_grok(prompt, max_tokens=1500, crosstalk=False, other_take="",
                 image=None):
    """Ask Grok. If crosstalk, react to the other partner's take instead.
    image is an optional (mime_type, base64) tuple — Grok sees the photo."""
    if MOCK:
        return _mock_reply("grok", other_take or prompt)
    key = _need_key("XAI_API_KEY")
    if crosstalk:
        text = (CROSSTALK_HEADER + "Shavor's message:\n" + prompt
                + "\n\nOther partner's take:\n" + other_take)
    elif image:
        mime, b64 = image
        text = [{"role": "user", "content": [
            {"type": "input_text",
             "text": _with_memory(DESK_HEADER) + prompt},
            {"type": "input_image",
             "image_url": f"data:{mime};base64,{b64}"},
        ]}]
    else:
        text = _with_memory(DESK_HEADER) + prompt
    payload = {
        "model": GROK_MODEL,
        "input": text,
        "temperature": 0.3,
        "max_output_tokens": max_tokens,
        "tools": [{"type": "web_search"}, {"type": "x_search"}],
    }
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            r = await client.post(
                XAI_URL, json=payload,
                headers={"Authorization": f"Bearer {key}"})
            r.raise_for_status()
            result = r.json()
    except httpx.HTTPError as e:
        raise RuntimeError(f"Grok API error: {e}") from e
    out = _extract_grok_text(result)
    if not out:
        raise RuntimeError("Grok returned no message text.")
    return out


async def ask_gemini(prompt, max_tokens=4000, crosstalk=False, other_take="",
                   image=None):
    """Ask Gemini. image is an optional (mime_type, base64) tuple."""
    if MOCK:
        return _mock_reply("gemini", other_take or prompt)
    key = _need_key("GEMINI_API_KEY")
    if crosstalk:
        parts = [{"text": (CROSSTALK_HEADER + "Shavor's message:\n" + prompt
                           + "\n\nOther partner's take:\n" + other_take)}]
    else:
        parts = [{"text": _with_memory(DESK_HEADER) + prompt}]
        if image:
            mime, b64 = image
            parts.append({"inline_data": {"mime_type": mime, "data": b64}})
    payload = {
        "contents": [{"role": "user", "parts": parts}],
        "tools": [{"google_search": {}}],
        "generationConfig": {
            "temperature": 0.3,
            "maxOutputTokens": max_tokens,
            "thinkingConfig": {"thinkingLevel": "low"},
        },
    }
    url = f"{GEMINI_BASE}/models/{GEMINI_MODEL}:generateContent"
    try:
        async with httpx.AsyncClient(timeout=120) as client:
            r = await client.post(
                url, json=payload,
                headers={"x-goog-api-key": key,
                         "Content-Type": "application/json"})
            r.raise_for_status()
            result = r.json()
    except httpx.HTTPError as e:
        raise RuntimeError(f"Gemini API error: {e}") from e
    out = _extract_gemini_text(result)
    if not out:
        raise RuntimeError("Gemini returned no message text.")
    return out


PROVIDERS = {
    "grok": ask_grok,
    "gemini": ask_gemini,
}
