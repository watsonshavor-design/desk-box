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
