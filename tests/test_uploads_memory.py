"""Uploads, retention, and shared-memory restore."""
import os

import app as app_module

# 1x1 PNG
PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d4948445200000001000000010802000000907753de"
    "0000000c4944415408d763f8ffff3f0005fe02fea7d4b3e20000000049454e44ae426082"
)


def test_upload_and_fetch_are_token_gated(client):
    rejected = client.post(
        "/api/upload",
        files={"file": ("note.txt", b"hello", "text/plain")},
    )
    assert rejected.status_code == 403

    bad_type = client.post(
        "/api/upload",
        params={"token": "test-token"},
        files={"file": ("note.txt", b"hello", "text/plain")},
    )
    assert bad_type.status_code == 400

    empty = client.post(
        "/api/upload",
        params={"token": "test-token"},
        files={"file": ("empty.png", b"", "image/png")},
    )
    assert empty.status_code == 400

    saved = client.post(
        "/api/upload",
        params={"token": "test-token"},
        files={"file": ("dot.png", PNG, "image/png")},
    )
    assert saved.status_code == 200
    body = saved.json()
    assert body["ok"] is True
    assert body["kind"] == "image"
    assert body["url"].startswith("/uploads/")

    hidden = client.get(body["url"])
    assert hidden.status_code == 403
    shown = client.get(body["url"], params={"token": "test-token"})
    assert shown.status_code == 200
    assert shown.content == PNG

    missing = client.get("/uploads/not-a-real-file.png", params={"token": "test-token"})
    assert missing.status_code == 404


def test_oversized_image_is_rejected(client):
    blob = b"\xff\xd8\xff" + (b"a" * (app_module.MAX_IMAGE_BYTES + 1))
    result = client.post(
        "/api/upload",
        params={"token": "test-token"},
        files={"file": ("big.jpg", blob, "image/jpeg")},
    )
    assert result.status_code == 400
    assert "too large" in result.json()["error"]


def test_memory_round_trip_survives_a_reload(client):
    digest = "GLND sell stays at $7. Do not resize it."
    saved = client.post("/api/memory", json={"token": "test-token", "digest": digest})
    assert saved.status_code == 200
    current = client.get("/api/memory", params={"token": "test-token"}).json()
    assert current["digest"] == digest
    assert current["updated_ts"]
    assert digest in app_module.providers_mod.ROOM_MEMORY

    app_module.room_memory["digest"] = ""
    app_module.providers_mod.ROOM_MEMORY = ""
    raw = app_module.load_room_memory()
    assert digest in raw
    body = raw.split("\n\n", 1)
    app_module.save_room_memory(body[1] if len(body) > 1 else raw)
    assert app_module.room_memory["digest"] == digest
    assert os.path.exists(app_module.MEMORY_PATH)


def test_bad_memory_token_is_rejected(client):
    result = client.post("/api/memory", json={"token": "nope", "digest": "x"})
    assert result.status_code == 403
