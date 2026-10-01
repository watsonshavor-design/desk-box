# Desk Box — deploy in ~10 minutes (phone-friendly)

## What you pay
~$20/month all-in: ~$10 hosting + ~$10 AI calls at normal daily use.

## Steps
1. On github.com (phone browser): create a new repo called `desk-box`,
   then Add file -> Upload files and drop in every file from this zip.
2. On railway.app: sign up with GitHub, New Project -> Deploy from GitHub,
   pick `desk-box`. Railway auto-detects the Dockerfile.
3. In the Railway project go to the Variables tab and add:
   - XAI_API_KEY = your key from the xAI console (console.x.ai)
   - GEMINI_API_KEY = your key from Google AI Studio (aistudio.google.com)
   - DESK_TOKEN = any private word you make up (this is your room password)
4. Railway gives you a public URL. Open it as: <url>/?token=YOUR_DESK_TOKEN
5. Send a test message. Grok and Gemini should both answer in the room.

## Notes
- Never share your DESK_TOKEN or API keys with anyone.
- Watch spend in the xAI console once a week; Gemini is cheap.
- The room logs sessions to desk-log.jsonl for the Night Desk relay.

## Optional: Moomoo top-gainers (OpenAPI)
In Railway Variables for the desk-box service also set:
- `MOOMOO_APP_KEY` = AppKey id from https://open.moomoo.com/dashboard
- `MOOMOO_RSA_PRIVATE_KEY` = matching Ed25519/RSA private key PEM
  (use `\n` for newlines in the Railway UI). AppKey alone is not enough —
  Traditional API Key auth signs every request with the private key.
Webull gainers keep working without these. Combined merges both when Moomoo is ok.

## Optional: local Ollama LLM (fallback)
When a cloud provider (Grok/Gemini) fails, the desk can retry against a
local OpenAI-compatible endpoint (Ollama). Set in Variables / `.env`:
- `LOCAL_LLM_ENABLED` = `1` to register the local provider and enable fallback
  (default `0` — cloud-only, unchanged behavior)
- `LOCAL_LLM_URL` = OpenAI-compatible base URL
  (default `http://localhost:11434/v1`)
- `LOCAL_LLM_MODEL` = model name (default `llama3.1:8b`)
Auth uses a fixed `Authorization: Bearer ollama` header (no real API key).
Images are ignored for local calls (text-only). Successful fallbacks are
prefixed with `[local fallback] ` in the stored/broadcast reply.

**Railway note:** the default URL points at localhost on the *desk-box*
container. Railway cannot reach an Ollama process on your laptop. For
hosted fallback, point `LOCAL_LLM_URL` at a reachable Ollama/OpenAI-compatible
host (private network, tunnel, or sidecar), or keep `LOCAL_LLM_ENABLED=0`.
