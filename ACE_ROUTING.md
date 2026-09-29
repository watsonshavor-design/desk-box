# Ace routing (production behavior)

This note records what the server actually does today. It is the Phase 0
reconciliation of the bridge docs with the code. Later phases add a job
API so Ace can answer every one-answer question; they do not change the
legacy inbox filter described here.

## What the code does

`GET /api/ace-inbox` returns Shavor's messages that contain `@ace`
(case-insensitive) and are newer than `since`. Rail, Anchor, Ace, and
hidden funnel takes are not included. A message with funnel mode on is
still omitted unless the text contains `@ace`.

`GET /api/recent` returns the last N messages from every sender, including
hidden funnel takes. A bridge that wants thread context uses this, not
the inbox.

`POST /api/ace-reply` injects Ace's text into the room for any caller
that has the room token. `funnel_answer: true` marks the post as the
synthesis for `for_ts` so a bridge can avoid synthesizing twice. The
server does not enforce that mark: a second post with the same `for_ts`
is stored and broadcast again.

Funnel mode (`funnel: true` on the WebSocket user message) asks Rail and
Anchor in parallel, stores their takes with `hidden: true` and `for_ts`
set to the user message timestamp, and does not broadcast those takes.
Ace is not called by the server. Nothing in the funnel path wakes Ace.
The room stays quiet until something posts to `/api/ace-reply`.

Panel mode (funnel omitted or false) broadcasts Rail and Anchor directly.
Cross-talk is a second round between them and never calls Ace.

## Where the docs disagree

The product rule we want: Ace answers every Shavor message, and one-answer
mode is the default. The Android README says "`@ace` uses the Ace bridge,"
which matches the inbox filter. The server never fans a normal message
out to Ace. Timestamp correlation (`for_ts`) is the only link between a
question and a synthesis, and it is not unique or idempotent.

## What must not change until the job API lands

Keep the `@ace` inbox filter so the current poller does not suddenly
receive every line in the room. Do not treat `/api/recent` as a claim
queue. One-answer completion stays a bridge post until Phase 2 (message
reliability) adds leased jobs and exactly-once completion.
