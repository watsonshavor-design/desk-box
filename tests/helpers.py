def drain_until_done(ws, limit=20):
    """Read socket events until the round closes."""
    events = []
    for _ in range(limit):
        event = ws.receive_json()
        events.append(event)
        if event.get("type") == "done":
            break
    return events
