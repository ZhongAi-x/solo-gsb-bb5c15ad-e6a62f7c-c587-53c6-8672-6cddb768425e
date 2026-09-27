"""End-to-end tests for the per-target maintenance pause switch."""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import httpx

from tests.test_webhooks import _get_event, _post_event_obj
from .conftest import wait_for


def _set_pause(base_url: str, target: str, paused: bool) -> httpx.Response:
    return httpx.put(
        f"{base_url}/targets/pause",
        json={"target": target, "paused": paused},
    )


def _pauses(base_url: str, **params) -> list[dict]:
    return httpx.get(f"{base_url}/targets/pauses", params=params).json()["pauses"]


def _wait_pauses(base_url: str, target: str, paused: bool, timeout: float = 5.0) -> dict:
    def find():
        rows = _pauses(base_url, target=target)
        return rows[0] if rows and rows[0]["paused"] is paused else None

    return wait_for(find, timeout=timeout)


# -------------------------------------------------------------- set / list API

def test_put_pause_idempotent_and_listed(server):
    target = f"{server.base_url}/t/record?pause-api={uuid.uuid4().hex}"
    resp = _set_pause(server.base_url, target, True)
    assert resp.status_code == 200
    row = resp.json()
    assert set(row) == {"target", "paused", "updated_at"}
    assert row["target"] == target
    assert row["paused"] is True
    assert datetime.fromisoformat(row["updated_at"]).tzinfo is not None

    rows = _pauses(server.base_url)
    assert [r for r in rows if r["target"] == target] == [row]

    # Setting the same value again is idempotent: still one row, still paused.
    second = _set_pause(server.base_url, target, True).json()
    assert second["paused"] is True
    assert len(_pauses(server.base_url, target=target)) == 1

    # Resuming keeps the row but flips the flag.
    resumed = _set_pause(server.base_url, target, False).json()
    assert resumed["paused"] is False
    assert resumed["target"] == target
    assert [r["paused"] for r in _pauses(server.base_url, target=target)] == [False]
    assert _pauses(server.base_url, paused=True) == [
        r for r in _pauses(server.base_url) if r["paused"] is True
    ]
    assert all(r["paused"] is False for r in _pauses(server.base_url, paused=False))

    # Unknown target and pagination envelope.
    body = httpx.get(
        f"{server.base_url}/targets/pauses",
        params={"target": "http://does-not-exist.invalid/x"},
    ).json()
    assert body["pauses"] == []
    assert set(body) == {"pauses", "limit", "offset"}


def test_put_pause_rejects_bad_bodies(server):
    def status(body):
        return httpx.put(f"{server.base_url}/targets/pause", content=body).status_code

    assert status(b"not json") == 400
    assert status(b"[1,2]") == 400
    assert status(b'{"paused": true}') == 422
    assert status(b'{"target": "not-a-url", "paused": true}') == 422
    assert status(b'{"target": "ftp://example.com", "paused": true}') == 422
    assert status(b'{"target": "http://example.com", "paused": 2}') == 422
    assert status(b'{"target": "http://example.com", "paused": null}') == 422


def test_put_pause_normalises_target_url_like_event_creation(server):
    # pydantic HttpUrl canonicalisation (scheme/host lowercased) is applied
    # to both event creation and pause setting, so a pause set through an
    # equivalent spelling matches the canonical target stored on events.
    canonical = f"{server.base_url}/t/record?norm={uuid.uuid4().hex}"
    equivalent = "HTTP://" + canonical[len("http://"):]
    assert equivalent != canonical
    _post_event_obj(server.base_url, {
        "idempotency_key": uuid.uuid4().hex,
        "target": canonical,
        "payload": {},
    })
    resp = _set_pause(server.base_url, equivalent, True)
    assert resp.status_code == 200
    assert resp.json()["target"] == canonical
    assert _pauses(server.base_url, target=canonical)[0]["paused"] is True
    # Clean up so the event can deliver and no residue remains.
    _set_pause(server.base_url, canonical, False)


# ------------------------------------------------------ paused target not sent

def test_paused_target_events_are_not_claimed_but_others_deliver(server):
    paused_target = f"{server.base_url}/t/record?paused-block={uuid.uuid4().hex}"
    other_target = f"{server.base_url}/t/record?other-flows={uuid.uuid4().hex}"
    _set_pause(server.base_url, paused_target, True)

    blocked = _post_event_obj(server.base_url, {
        "idempotency_key": uuid.uuid4().hex,
        "target": paused_target,
        "payload": {},
    }).json()
    other = _post_event_obj(server.base_url, {
        "idempotency_key": uuid.uuid4().hex,
        "target": other_target,
        "payload": {},
    }).json()

    # Other endpoints keep delivering while the paused one is untouched.
    wait_for(lambda: _get_event(server.base_url, other["id"])["status"] == "delivered")
    time.sleep(1.0)
    blocked_event = _get_event(server.base_url, blocked["id"])
    assert blocked_event["status"] == "queued"
    assert blocked_event["attempts_count"] == 0

    # Resume: only the flag is cleared; the event then delivers normally,
    # and no attempt/breaker state was fabricated while paused.
    _set_pause(server.base_url, paused_target, False)
    final = wait_for(
        lambda: (lambda e: e if e["status"] == "delivered" else None)(
            _get_event(server.base_url, blocked["id"])
        )
    )
    assert final["attempts_count"] == 1


# ------------------------------------------- already-claimed attempt completes

def test_pause_does_not_interrupt_in_flight_attempt(server):
    token = f"pause-inflight-{uuid.uuid4().hex}"
    target = f"{server.base_url}/t/block?token={token}"
    event = _post_event_obj(server.base_url, {
        "idempotency_key": uuid.uuid4().hex,
        "target": target,
        "payload": {},
    }).json()
    wait_for(lambda: _get_event(server.base_url, event["id"])["status"] == "processing")

    # Pausing while the attempt is in flight must not cancel it.
    _set_pause(server.base_url, target, True)
    time.sleep(0.5)
    assert _get_event(server.base_url, event["id"])["status"] == "processing"

    # The in-flight request is allowed to finish and lands as delivered.
    httpx.post(f"{server.base_url}/t/release", params={"token": token})
    wait_for(lambda: _get_event(server.base_url, event["id"])["status"] == "delivered")
    _set_pause(server.base_url, target, False)


# ----------------------------------------------- expiry sweep keeps running --

def test_queued_events_still_expire_while_paused(server):
    target = f"{server.base_url}/t/record?paused-expiry={uuid.uuid4().hex}"
    _set_pause(server.base_url, target, True)
    now = datetime.now(timezone.utc)
    event = _post_event_obj(server.base_url, {
        "idempotency_key": uuid.uuid4().hex,
        "target": target,
        "payload": {},
        "deliver_after": (now - timedelta(seconds=1)).isoformat(),
        "expires_at": (now + timedelta(milliseconds=500)).isoformat(),
    }).json()

    expired = wait_for(
        lambda: (lambda e: e if e["status"] == "expired" else None)(
            _get_event(server.base_url, event["id"])
        ),
        timeout=8,
    )
    # Expired without a single attempt; the expired head also unblocks the
    # target ordering, but claims remain paused-gated.
    assert expired["attempts_count"] == 0

    # A later event on the same paused target is claimable by the ordering
    # rule (nothing earlier pending) yet still withheld solely by the pause.
    follower = _post_event_obj(server.base_url, {
        "idempotency_key": uuid.uuid4().hex,
        "target": target,
        "payload": {},
    }).json()
    time.sleep(0.6)
    assert _get_event(server.base_url, follower["id"])["status"] == "queued"
    assert _get_event(server.base_url, follower["id"])["attempts_count"] == 0

    _set_pause(server.base_url, target, False)
    wait_for(
        lambda: _get_event(server.base_url, follower["id"])["status"] == "delivered"
    )


# ------------------------- resume keeps breaker state and attempt scheduling --

def test_resume_preserves_attempt_counts_and_breaker_state(server):
    # The target fails the first 3 attempts (tripping the breaker open),
    # then keeps failing. Pause after the breaker opens; resume after the
    # open deadline and let the head go out as the half-open probe.
    target = f"{server.base_url}/t/probe?fail_times=10&resume-keep={uuid.uuid4().hex}"
    event = _post_event_obj(server.base_url, {
        "idempotency_key": uuid.uuid4().hex,
        "target": target,
        "payload": {},
    }).json()

    def breaker():
        rows = httpx.get(
            f"{server.base_url}/targets/breakers", params={"target": target}
        ).json()["breakers"]
        return rows[0] if rows else None

    wait_for(lambda: (lambda b: b if b and b["state"] == "open" else None)(breaker()))
    assert _get_event(server.base_url, event["id"])["attempts_count"] == 3

    _set_pause(server.base_url, target, True)
    time.sleep(2.5)  # longer than the 1s test breaker window

    # Paused: the elapsed open deadline must not produce a probe; the event
    # keeps its 3 attempts and its queued/backoff scheduling untouched.
    assert _get_event(server.base_url, event["id"])["attempts_count"] == 3
    assert _get_event(server.base_url, event["id"])["status"] == "queued"
    assert breaker()["state"] == "open"

    _set_pause(server.base_url, target, False)
    # Resume only cleared the pause: the head now becomes the half-open
    # probe exactly as it would have without the pause.
    probe = wait_for(
        lambda: (lambda b: b if b and b["state"] == "half-open" else None)(breaker()),
        timeout=8,
    )
    assert probe["probe_event_id"] == event["id"]
    # The probe fails too (fail_times=10): breaker reopens and the event
    # eventually dies after attempt 4 — counters/schedule were not reset.
    reopened = wait_for(
        lambda: (lambda b: b if b and b["state"] == "open" else None)(breaker()),
        timeout=8,
    )
    assert reopened["consecutive_failures"] == 4
    dead = wait_for(
        lambda: (lambda e: e if e["status"] == "dead" else None)(
            _get_event(server.base_url, event["id"])
        ),
        timeout=8,
    )
    assert dead["attempts_count"] == 4


# ----------------------------- persistence across restart on a legacy database

def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_health(port: int, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            httpx.get(f"http://127.0.0.1:{port}/health", timeout=1)
            return
        except httpx.HTTPError:
            time.sleep(0.2)
    raise RuntimeError(f"server on port {port} never became healthy")


def test_pause_persists_across_restart_and_legacy_db_auto_init(tmp_path):
    import sqlite3

    db_path = tmp_path / "legacy.db"
    # A database from an older release: events/attempts exist but there is
    # no breakers table, no target_pauses table and none of the newer
    # events columns. Startup must initialise everything automatically.
    legacy = sqlite3.connect(db_path)
    legacy.executescript(
        """
        CREATE TABLE events (
            id TEXT PRIMARY KEY, idempotency_key TEXT NOT NULL UNIQUE,
            target TEXT NOT NULL, payload TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued', attempts_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL, next_attempt_at REAL, delivered_at TEXT
        );
        CREATE TABLE attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL,
            attempt_no INTEGER NOT NULL, status_code INTEGER, error TEXT,
            attempted_at TEXT NOT NULL
        );
        """
    )
    legacy.commit()
    legacy.close()

    env = {
        **os.environ,
        "WEBHOOK_SECRET": "test-secret",
        "WEBHOOK_DB_PATH": str(db_path),
        "WORKER_POLL_INTERVAL": "0.5",
        "DELIVERY_TIMEOUT": "5",
    }
    port = _free_port()
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn", "app.main:app",
            "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning",
        ],
        cwd="/workspace",
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        _wait_health(port)
        target = "http://127.0.0.1:9/never-delivered-paused"
        resp = _set_pause(f"http://127.0.0.1:{port}", target, True)
        assert resp.status_code == 200
        assert resp.json()["paused"] is True
    finally:
        proc.terminate()
        proc.wait(timeout=15)

    # Restart the same database: the pause row survives and the legacy
    # schema was initialised (no migration error on either startup).
    port2 = _free_port()
    proc2 = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn", "app.main:app",
            "--host", "127.0.0.1", "--port", str(port2), "--log-level", "warning",
        ],
        cwd="/workspace",
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        _wait_health(port2)
        rows = _pauses(f"http://127.0.0.1:{port2}")
        match = [r for r in rows if r["target"] == target]
        assert len(match) == 1
        assert match[0]["paused"] is True
        # Resuming through the API works on the migrated legacy database.
        resumed = _set_pause(f"http://127.0.0.1:{port2}", target, False).json()
        assert resumed["paused"] is False
    finally:
        proc2.terminate()
        out, _ = proc2.communicate(timeout=15)
        if proc2.returncode not in (0, -15):
            raise AssertionError(out.decode())
