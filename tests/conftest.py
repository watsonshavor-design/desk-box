"""Isolated Desk Box app for baseline smoke tests.

Environment is set before app import because the server reads DESK_TOKEN,
DESK_DATA_DIR, and MOCK_PROVIDERS at import time.
"""
import os
import shutil
import tempfile

_DATA = tempfile.mkdtemp(prefix="desk-box-test-")
os.environ["DESK_TOKEN"] = "test-token"
os.environ["DESK_DATA_DIR"] = _DATA
os.environ["MOCK_PROVIDERS"] = "1"
os.environ["ACE_AUTO"] = "0"
os.environ.setdefault("XAI_API_KEY", "")
os.environ.setdefault("GEMINI_API_KEY", "")

import pytest
from fastapi.testclient import TestClient

import app as app_module
import market


@pytest.fixture(scope="session")
def client():
    with TestClient(app_module.app) as test_client:
        yield test_client


@pytest.fixture(autouse=True)
def _reset_room(client):
    app_module.history.clear()
    app_module.store.wipe()
    app_module.thread_queues.clear()
    market.use_loader("moomoo", None)
    market.use_loader("webull", None)
    log_path = app_module.LOG_PATH
    if os.path.exists(log_path):
        os.remove(log_path)
    memory = app_module.MEMORY_PATH
    if os.path.exists(memory):
        os.remove(memory)
    app_module.room_memory["digest"] = ""
    app_module.room_memory["updated_ts"] = ""
    app_module.providers_mod.ROOM_MEMORY = ""
    upload_dir = app_module.UPLOAD_DIR
    if os.path.isdir(upload_dir):
        shutil.rmtree(upload_dir, ignore_errors=True)
    yield

