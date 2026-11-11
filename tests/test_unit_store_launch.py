# vim: filetype=python
"""Launches: the request, the verdicts, request ids, the states, the claim
timeout, the links, the launch mark of a heartbeat, and the migration."""

import hashlib
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from hyperspace_protocol import HeartbeatRequest, LaunchRequest
from store import (
    _MIGRATIONS,
    LAUNCH_CLAIM_TIMEOUT,
    NotFound,
    SessionKey,
    Store,
    StoreError,
    Writer,
)

ACCOUNT = "acct"
HARNESS = "claude-code"
PARENT = SessionKey(ACCOUNT, HARNESS, "parent-1")
LAPTOP = Writer(ACCOUNT, "laptop", "/home/user/project")


class Clock:
    """A clock that moves one second forward at each call."""

    def __init__(self) -> None:
        self.t = datetime(2026, 11, 11, tzinfo=UTC)

    def __call__(self) -> datetime:
        self.t += timedelta(seconds=1)
        return self.t


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def store(tmp_path: Path, clock: Clock) -> Iterator[Store]:
    s = Store(tmp_path / "data", clock=clock)
    s.register_host(ACCOUNT, "agent-1", {"kind": "runner"})
    s.register_host(ACCOUNT, "laptop", {"kind": "workstation"})
    yield s
    s.close()


def start(store: Store, key: SessionKey = PARENT) -> None:
    gen = store.create_generation(key, "transcript.jsonl", LAPTOP, "root")["id"]
    data = b"one\n"
    store.append(key, gen, LAPTOP, 0, data, hashlib.sha256(data).hexdigest(), 1)


def request(**extra: str) -> LaunchRequest:
    body: LaunchRequest = {"host": "agent-1", "prompt": "review the change"}
    body.update(extra)  # type: ignore[typeddict-item]
    return body


def launch(store: Store, **extra: str) -> str:
    return store.launch(ACCOUNT, HARNESS, "laptop", request(**extra))["launch"]


def beat(store: Store, host: str = "agent-1", **extra: object) -> None:
    body: HeartbeatRequest = {"interval": 60, "sessions": []}
    body.update(extra)  # type: ignore[typeddict-item]
    store.heartbeat(ACCOUNT, host, "all", body)


def state(store: Store, launch_id: str) -> str:
    return store.get_launch(ACCOUNT, HARNESS, launch_id)["state"]


def stored_prompt(store: Store, launch_id: str) -> str | None:
    with closing(sqlite3.connect(store.data_dir / "index.sqlite3")) as db:
        row = db.execute(
            "SELECT prompt FROM launches WHERE id = ?", (launch_id,)
        ).fetchone()
    return row[0]


def test_launch_is_recorded_pending(store: Store) -> None:
    start(store)
    response = store.launch(
        ACCOUNT,
        HARNESS,
        "laptop",
        request(
            workspace="/home/claude/workspace",
            model="claude-opus-5-5",
            effort="high",
            name="reviewer",
            parent=PARENT.session_id,
        ),
    )
    assert response["verdict"] == "queued"
    assert response["launch"].startswith("l_")
    body = store.get_launch(ACCOUNT, HARNESS, response["launch"])
    assert body == {
        "launch": response["launch"],
        "harness": HARNESS,
        "host": "agent-1",
        "state": "pending",
        "caller": "laptop",
        "created": body["created"],
        "updated": body["created"],
        "workspace": "/home/claude/workspace",
        "model": "claude-opus-5-5",
        "effort": "high",
        "name": "reviewer",
        "parent": PARENT.session_id,
    }
    # The caller's read has no prompt. The launcher's list has it.
    [pending] = store.pending_launches(ACCOUNT, HARNESS, "agent-1")
    assert pending == {**body, "prompt": "review the change"}


def test_refusals(store: Store) -> None:
    with pytest.raises(StoreError) as err:
        launch(store, parent="no-such-session")
    assert (err.value.code, err.value.detail) == (
        "bad_request",
        "parent is not a session in the account",
    )
    # The parent must be in the account, not only exist somewhere.
    start(store, SessionKey("other", HARNESS, "parent-1"))
    with pytest.raises(StoreError) as err:
        launch(store, parent="parent-1")
    assert err.value.code == "bad_request"
    with pytest.raises(NotFound) as missing:
        launch(store, host="nowhere")
    assert missing.value.detail == "no such host"
    # A host of another account is not a host of this account.
    store.register_host("other", "elsewhere", {"kind": "runner"})
    with pytest.raises(NotFound):
        launch(store, host="elsewhere")
    # A refused launch records nothing.
    assert store.pending_launches(ACCOUNT, HARNESS, "agent-1") == []
    assert store.pending_launches(ACCOUNT, HARNESS, "nowhere") == []


def test_verdict_follows_the_launch_mark(store: Store, clock: Clock) -> None:
    assert store.launch(ACCOUNT, HARNESS, "laptop", request())["verdict"] == "queued"
    # A launcher that acts on wakes only does not act on launches.
    beat(store, wakes=True)
    assert store.launch(ACCOUNT, HARNESS, "laptop", request())["verdict"] == "queued"
    beat(store, wakes=True, launches=True)
    assert store.launch(ACCOUNT, HARNESS, "laptop", request())["verdict"] == "starting"
    # The mark of another host does not count.
    other = request(host="laptop")
    assert store.launch(ACCOUNT, HARNESS, "laptop", other)["verdict"] == "queued"
    # A heartbeat of a hook (no `wakes`) keeps the mark.
    beat(store)
    assert store.launch(ACCOUNT, HARNESS, "laptop", request())["verdict"] == "starting"
    # The mark expires with its heartbeat.
    clock.t += timedelta(seconds=181)
    assert store.launch(ACCOUNT, HARNESS, "laptop", request())["verdict"] == "queued"
    beat(store, wakes=True, launches=True)
    assert store.launch(ACCOUNT, HARNESS, "laptop", request())["verdict"] == "starting"
    # A launcher heartbeat without `launches` clears it.
    beat(store, wakes=True)
    assert store.launch(ACCOUNT, HARNESS, "laptop", request())["verdict"] == "queued"
    beat(store, wakes=True, launches=True)
    # A launcher that stops clears it.
    beat(store, wakes=True, launches=True, final=True)
    assert store.launch(ACCOUNT, HARNESS, "laptop", request())["verdict"] == "queued"


def test_a_second_source_keeps_the_host_marked(store: Store) -> None:
    beat(store, source="a", wakes=True, launches=True)
    beat(store, source="b", wakes=True)
    assert store.launch(ACCOUNT, HARNESS, "laptop", request())["verdict"] == "starting"


def test_request_id(store: Store, clock: Clock) -> None:
    first = store.launch(ACCOUNT, HARNESS, "laptop", request(request="r1"))
    # The same request again gives the first response, and records nothing.
    beat(store, wakes=True, launches=True)
    again = store.launch(ACCOUNT, HARNESS, "laptop", request(request="r1"))
    assert again == first == {"launch": first["launch"], "verdict": "queued"}
    assert len(store.pending_launches(ACCOUNT, HARNESS, "agent-1")) == 1
    # Also after the prompt is gone.
    store.claim_launch(ACCOUNT, HARNESS, "agent-1", first["launch"])
    store.link_launch(ACCOUNT, HARNESS, "agent-1", first["launch"], "new-1")
    assert stored_prompt(store, first["launch"]) is None
    assert store.launch(ACCOUNT, HARNESS, "laptop", request(request="r1")) == first
    # Another field, another prompt, or another harness: refused.
    for harness, body in (
        (HARNESS, request(request="r1", name="other")),
        (HARNESS, request(request="r1", prompt="other")),
        (HARNESS, request(request="r1", host="laptop")),
        ("other-harness", request(request="r1")),
    ):
        with pytest.raises(StoreError) as err:
            store.launch(ACCOUNT, harness, "laptop", body)
        assert (err.value.code, err.value.detail) == ("request_used", "request id used")
    # The caller's host is not a field.
    assert store.launch(ACCOUNT, HARNESS, "agent-1", request(request="r1")) == first
    # Another account has its own request ids.
    store.register_host("other", "agent-1", {"kind": "runner"})
    elsewhere = store.launch("other", HARNESS, "laptop", request(request="r1"))
    assert elsewhere["launch"] != first["launch"]
    # After 24 hours, the id is free again.
    clock.t += timedelta(hours=24)
    later = store.launch(ACCOUNT, HARNESS, "laptop", request(request="r1", name="x"))
    assert later["launch"] != first["launch"]
    assert later["verdict"] == "queued"


def test_claim_link_and_first_link_wins(store: Store) -> None:
    launch_id = launch(store)
    claimed = store.claim_launch(ACCOUNT, HARNESS, "agent-1", launch_id)
    assert claimed["state"] == "claimed"
    assert claimed["prompt"] == "review the change"
    assert store.pending_launches(ACCOUNT, HARNESS, "agent-1") == []
    # A launch starts at most once.
    with pytest.raises(StoreError) as err:
        store.claim_launch(ACCOUNT, HARNESS, "agent-1", launch_id)
    assert (err.value.code, err.value.detail) == ("launch_state", "launch is claimed")
    linked = store.link_launch(ACCOUNT, HARNESS, "agent-1", launch_id, "new-1")
    assert (linked["state"], linked["session"]) == ("started", "new-1")
    assert "prompt" not in linked
    assert stored_prompt(store, launch_id) is None
    # The same session again changes nothing.
    again = store.link_launch(ACCOUNT, HARNESS, "agent-1", launch_id, "new-1")
    assert again == linked
    with pytest.raises(StoreError) as err:
        store.link_launch(ACCOUNT, HARNESS, "agent-1", launch_id, "new-2")
    assert err.value.detail == "launch is linked to another session"
    assert store.get_launch(ACCOUNT, HARNESS, launch_id)["session"] == "new-1"
    for operation in (
        lambda: store.claim_launch(ACCOUNT, HARNESS, "agent-1", launch_id),
        lambda: store.fail_launch(ACCOUNT, HARNESS, "agent-1", launch_id, "x"),
        lambda: store.cancel_launch(ACCOUNT, HARNESS, launch_id),
    ):
        with pytest.raises(StoreError) as err:
            operation()
        assert (err.value.code, err.value.detail) == (
            "launch_state",
            "launch is started",
        )


def test_link_needs_a_claim(store: Store) -> None:
    launch_id = launch(store)
    with pytest.raises(StoreError) as err:
        store.link_launch(ACCOUNT, HARNESS, "agent-1", launch_id, "new-1")
    assert err.value.detail == "launch is pending"
    with pytest.raises(StoreError) as err:
        store.fail_launch(ACCOUNT, HARNESS, "agent-1", launch_id, "x")
    assert err.value.detail == "launch is pending"


def test_fail(store: Store) -> None:
    launch_id = launch(store)
    store.claim_launch(ACCOUNT, HARNESS, "agent-1", launch_id)
    failed = store.fail_launch(
        ACCOUNT, HARNESS, "agent-1", launch_id, "claude: unknown option"
    )
    assert (failed["state"], failed["reason"]) == ("failed", "claude: unknown option")
    assert "prompt" not in failed and "session" not in failed
    assert stored_prompt(store, launch_id) is None
    # Only a failure for "no session reported" takes a late link.
    with pytest.raises(StoreError) as err:
        store.link_launch(ACCOUNT, HARNESS, "agent-1", launch_id, "new-1")
    assert err.value.detail == "launch is failed"
    with pytest.raises(StoreError):
        store.fail_launch(ACCOUNT, HARNESS, "agent-1", launch_id, "again")


def test_cancel(store: Store) -> None:
    launch_id = launch(store)
    cancelled = store.cancel_launch(ACCOUNT, HARNESS, launch_id)
    assert cancelled["state"] == "cancelled" and "prompt" not in cancelled
    assert stored_prompt(store, launch_id) is None
    assert store.pending_launches(ACCOUNT, HARNESS, "agent-1") == []
    for operation in (
        lambda: store.cancel_launch(ACCOUNT, HARNESS, launch_id),
        lambda: store.claim_launch(ACCOUNT, HARNESS, "agent-1", launch_id),
    ):
        with pytest.raises(StoreError) as err:
            operation()
        assert err.value.detail == "launch is cancelled"
    claimed = launch(store)
    store.claim_launch(ACCOUNT, HARNESS, "agent-1", claimed)
    with pytest.raises(StoreError) as err:
        store.cancel_launch(ACCOUNT, HARNESS, claimed)
    assert err.value.detail == "launch is claimed"


def test_claim_expires(store: Store, clock: Clock) -> None:
    launch_id = launch(store)
    claimed = store.claim_launch(ACCOUNT, HARNESS, "agent-1", launch_id)
    clock.t += LAUNCH_CLAIM_TIMEOUT - timedelta(seconds=5)
    assert state(store, launch_id) == "claimed"
    clock.t += timedelta(seconds=5)
    # A read shows the failure before a write records it.
    body = store.get_launch(ACCOUNT, HARNESS, launch_id)
    assert (body["state"], body["reason"]) == ("failed", "no session reported")
    assert body["updated"] > claimed["updated"]
    assert stored_prompt(store, launch_id) is not None
    # The next write of a launch records it, and deletes the prompt.
    launch(store)
    assert stored_prompt(store, launch_id) is None
    assert store.get_launch(ACCOUNT, HARNESS, launch_id) == body
    with pytest.raises(StoreError) as err:
        store.fail_launch(ACCOUNT, HARNESS, "agent-1", launch_id, "late")
    assert err.value.detail == "launch is failed"
    # The session can report late.
    linked = store.link_launch(ACCOUNT, HARNESS, "agent-1", launch_id, "new-1")
    assert (linked["state"], linked["session"]) == ("started", "new-1")
    assert "reason" not in linked


def test_late_link_before_the_failure_is_recorded(store: Store, clock: Clock) -> None:
    launch_id = launch(store)
    store.claim_launch(ACCOUNT, HARNESS, "agent-1", launch_id)
    clock.t += LAUNCH_CLAIM_TIMEOUT
    linked = store.link_launch(ACCOUNT, HARNESS, "agent-1", launch_id, "new-1")
    assert linked["state"] == "started"


def test_pending_launch_has_no_expiry(store: Store, clock: Clock) -> None:
    launch_id = launch(store)
    clock.t += timedelta(days=30)
    launch(store)
    assert state(store, launch_id) == "pending"


def test_launcher_sees_only_its_host(store: Store) -> None:
    store.register_host(ACCOUNT, "agent-2", {"kind": "runner"})
    mine = launch(store)
    theirs = launch(store, host="agent-2")
    pending = store.pending_launches(ACCOUNT, HARNESS, "agent-1")
    assert [p["launch"] for p in pending] == [mine]
    assert store.pending_launches(ACCOUNT, "other-harness", "agent-1") == []
    for operation in (
        lambda: store.claim_launch(ACCOUNT, HARNESS, "agent-1", theirs),
        lambda: store.link_launch(ACCOUNT, HARNESS, "agent-1", theirs, "s"),
        lambda: store.fail_launch(ACCOUNT, HARNESS, "agent-1", theirs, "x"),
        lambda: store.claim_launch(ACCOUNT, "other-harness", "agent-1", mine),
        lambda: store.claim_launch("other", HARNESS, "agent-1", mine),
        lambda: store.get_launch("other", HARNESS, mine),
        lambda: store.get_launch(ACCOUNT, "other-harness", mine),
        lambda: store.cancel_launch("other", HARNESS, mine),
    ):
        with pytest.raises(NotFound) as err:
            operation()
        assert err.value.detail == "launch"
    assert state(store, theirs) == "pending"


def test_pending_list_is_oldest_first_and_bounded(store: Store) -> None:
    ids = [launch(store) for _ in range(3)]
    pending = store.pending_launches(ACCOUNT, HARNESS, "agent-1")
    assert [p["launch"] for p in pending] == ids
    pending = store.pending_launches(ACCOUNT, HARNESS, "agent-1", limit=2)
    assert [p["launch"] for p in pending] == ids[:2]


def test_migration_adds_launches(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    index = next(i for i, m in enumerate(_MIGRATIONS) if "CREATE TABLE launches" in m)
    with closing(sqlite3.connect(data / "index.sqlite3")) as db:
        for migration in _MIGRATIONS[:index]:
            db.executescript(migration)
        db.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
        db.execute("INSERT INTO schema_version VALUES (?)", (index,))
        db.execute(
            "INSERT INTO host_heartbeats VALUES "
            "('acct', 'agent-1', '', '2026-11-11T00:00:00.000000Z', 60, 0, '[]', "
            "'2026-11-11T00:00:00.000000Z', 60)"
        )
        db.commit()
    with closing(Store(data, clock=Clock())) as migrated:
        migrated.register_host(ACCOUNT, "agent-1", {"kind": "runner"})
        # A wake mark from before the migration is not a launch mark.
        response = migrated.launch(ACCOUNT, HARNESS, "laptop", request())
        assert response["verdict"] == "queued"
