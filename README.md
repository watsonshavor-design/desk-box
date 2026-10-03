# Desk Box — one live room for the trading desk

A tiny real-time chat room: Shavor sends one message, the backend fans it out
to **Grok** (xAI) and **Gemini** (Google) in parallel with the desk-context
header, and both labeled replies stream back into the same room over a
WebSocket. Optional **cross-talk** round: each model reacts to the other's
take before the round closes.

## How it works

```
browser ──WebSocket──▶ FastAPI backend ──┬──▶ xAI Responses API (Grok)
                                         └──▶ Gemini generateContent (Gemini)
```

- `app.py` — FastAPI server, WebSocket room, fan-out orchestration.
- `providers.py` — direct API calls (payloads mirror the desk's working relay scripts).
- `static/index.html` — the room UI (no build step, phone-friendly).
- Every message is appended to `desk-log.jsonl` (paste back to Night Desk, who join via relay).

## Quick start (local)

```bash
cd desk-box
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then fill in XAI_API_KEY and GEMINI_API_KEY
uvicorn app:app --host 0.0.0.0 --port 8000
```

Open the URL it prints (it includes your `?token=...`). If `DESK_TOKEN` is
unset, the server generates one per boot and prints it — no extra setup.

Dev mode (no API spend): `MOCK_PROVIDERS=1 uvicorn app:app --port 8000`
returns canned replies so you can test the whole loop.

## Deploy

Any host that runs a container or Python process with a persistent port:

- **Railway / Render / Fly.io** (~$5–20/mo): deploy the `Dockerfile`, set
  `XAI_API_KEY`, `GEMINI_API_KEY`, `DESK_TOKEN` in the service env, open the
  public URL with `?token=...`.
- **Cheap VPS**: `docker build -t desk-box . && docker run -d --env-file .env -p 8000:8000 desk-box`
  behind Caddy/Nginx for HTTPS.

Serverless (Lambda/Cloud Run jobs) is a bad fit — WebSockets need a
long-lived process.

## Security

- API keys live in server env vars and are not committed. Chat and YouTube keys stay on the server. The Maps JavaScript key is injected only into the token-gated map frame, because that API requires it in the browser. Restrict that key to the desk origin.
- The `?token=` gate keeps the room private. Rotate `DESK_TOKEN` any time.
- One cross-talk round max per message; providers run in parallel with a
  120s timeout; one provider failing never blocks the other.
- No bot-to-bot auto-chaining: every round starts from Shavor's message.

## Costs

Per message ≈ 1 Grok call + 1 Gemini call (×2 with cross-talk on), each
carrying the desk-context header plus your text. At trading pace this adds
up — cross-talk defaults **off** in the UI; turn it on for the questions
that deserve a second round.

## Roadmap (not in v1)

- Streaming tokens instead of whole replies.
- Per-room conversation memory beyond the desk header.
- Ace in the room (today Ace works from the main chat and the briefs).

## Known limit

Night Desk's bots live in the Grok app and expose no API, so they can't
join this room directly — they stay on paste relay via `desk-log.jsonl`,
same as today.
