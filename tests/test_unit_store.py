# vim: filetype=python
"""The storage core: generations, blobs, placements, leases, and conflicts."""

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from store import Conflict, SessionKey, Store, StoreError, Writer

KEY = SessionKey("acct", "claude-code", "s1")
LAPTOP = Writer("acct", "laptop", "/home/wlritchi/hyperspace")
AGENT = Writer("acct", "agent-1", "/home/agent/workspace")
OTHER = Writer("acct", "agent-2", "/home/agent/workspace")
PATH = "transcript.jsonl"


class Clock:
    """A clock that moves one second forward at each call."""

    def __init__(self) -> None:
        self.t = datetime(2026, 10, 13, tzinfo=UTC)

    def __call__(self) -> datetime:
        self.t += timedelta(seconds=1)
        return self.t


@pytest.fixture
def store(tmp_path: Path) -> Iterator[Store]:
    s = Store(tmp_path / "data", clock=Clock())
    yield s
    s.close()


def put(
    store: Store,
    gen: str,
    base: int,
    data: bytes,
    writer: Writer = LAPTOP,
    activity: int = 0,
    key: SessionKey = KEY,
) -> int:
    info, _ = store.append(
        key,
        gen,
        writer,
        base,
        data,
        hashlib.sha256(data).hexdigest(),
        activity,
    )
    return info["length"]


def root(store: Store, writer: Writer = LAPTOP, key: SessionKey = KEY) -> str:
    return store.create_generation(key, PATH, writer, "root")["id"]


def test_append_and_read(store: Store) -> None:
    g = root(store)
    n = put(store, g, 0, b"one\n")
    n = put(store, g, n, b"two\n")
    assert n == 8
    assert store.read_log(KEY, g, 0, None) == b"one\ntwo\n"
    assert store.read_log(KEY, g, 2, 6) == b"e\ntw"


def test_retry_is_idempotent(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"one\n")
    assert put(store, g, 0, b"one\n") == 4
    assert store.read_log(KEY, g, 0, None) == b"one\n"


def test_wrong_base_length_is_a_conflict(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"one\n")
    with pytest.raises(Conflict) as err:
        put(store, g, 0, b"two\n")
    assert err.value.length == 4
    with pytest.raises(Conflict):
        put(store, g, 99, b"two\n")


def test_bad_digest(store: Store) -> None:
    g = root(store)
    with pytest.raises(StoreError) as err:
        store.append(KEY, g, LAPTOP, 0, b"x", "0" * 64, 0)
    assert err.value.code == "bad_digest"


def test_fork_reads_through_parent(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"aaaa")
    put(store, g, 4, b"bbbb")
    r = store.create_generation(KEY, PATH, LAPTOP, "rewrite", parent=g, fork_offset=4)
    put(store, r["id"], 4, b"cccc")
    assert store.read_log(KEY, r["id"], 0, None) == b"aaaacccc"
    assert store.read_log(KEY, r["id"], 2, 6) == b"aacc"
    assert store.read_log(KEY, g, 0, None) == b"aaaabbbb"


def test_fork_offset_must_be_in_range(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"aaaa")
    with pytest.raises(StoreError):
        store.create_generation(KEY, PATH, LAPTOP, "rewrite", parent=g, fork_offset=5)


def test_second_root_is_refused(store: Store) -> None:
    root(store)
    with pytest.raises(StoreError) as err:
        root(store)
    assert err.value.code == "exists"


def test_recovery_cuts_uncommitted_frames(tmp_path: Path) -> None:
    data = tmp_path / "data"
    s = Store(data, clock=Clock())
    g = root(s)
    put(s, g, 0, b"one\n")
    s.close()
    log = data / "logs" / f"{g}.zst"
    size = log.stat().st_size
    with open(log, "ab") as f:
        f.write(b"garbage from a write that did not commit")
    s = Store(data, clock=Clock())
    assert s.recover() == [g]
    assert log.stat().st_size == size
    put(s, g, 4, b"two\n")
    assert s.read_log(KEY, g, 0, None) == b"one\ntwo\n"
    s.close()


def test_blob_versions_deduplicate(store: Store) -> None:
    def blob(data: bytes) -> int:
        sha = hashlib.sha256(data).hexdigest()
        return store.put_blob(KEY, "plans/p.md", LAPTOP, data, sha)["version"]

    assert blob(b"v1") == 1
    assert blob(b"v1") == 1
    assert blob(b"v2") == 2
    assert store.read_blob(KEY, "plans/p.md", None) == b"v2"
    assert store.read_blob(KEY, "plans/p.md", 1) == b"v1"


def test_blob_and_log_kinds_do_not_mix(store: Store) -> None:
    root(store)
    with pytest.raises(StoreError) as err:
        store.put_blob(KEY, PATH, LAPTOP, b"x", hashlib.sha256(b"x").hexdigest())
    assert err.value.code == "wrong_kind"


def test_first_append_acquires_the_lease(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"one\n")
    lease = store.session_state(KEY)["lease"]
    assert lease["state"] == "held"
    assert lease["host"] == "laptop"


def test_lease_transitions(store: Store) -> None:
    g = root(store)
    n = put(store, g, 0, b"one\n", activity=1)

    with pytest.raises(StoreError) as err:
        store.surrender(KEY, "agent-1", None)
    assert err.value.code == "not_holder"

    state = store.surrender(KEY, "laptop", "agent-1")
    assert state["lease"]["state"] == "reserved"
    assert state["lease"]["host"] == "agent-1"

    with pytest.raises(StoreError) as err:
        store.teleport(KEY, OTHER, steal=False)
    assert err.value.code == "lease_held"

    event, state = store.teleport(KEY, AGENT, steal=False)
    assert not event["steal"]
    assert state["lease"] == {
        "state": "held",
        "host": "agent-1",
        "workspace": "/home/agent/workspace",
        "since": state["lease"]["since"],
    }

    # A non-holder can still append; the lease does not change.
    n = put(store, g, n, b"two\n", writer=LAPTOP)
    assert store.session_state(KEY)["lease"]["host"] == "agent-1"

    event, state = store.teleport(KEY, LAPTOP, steal=True)
    assert event["steal"]
    assert state["lease"]["host"] == "laptop"


def test_surrender_free_lets_any_host_take_it(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"one\n")
    assert store.surrender(KEY, "laptop", None)["lease"]["state"] == "free"
    _, state = store.teleport(KEY, OTHER, steal=False)
    assert state["lease"]["host"] == "agent-2"


def test_placements_record_causes(store: Store) -> None:
    g = root(store)
    n = put(store, g, 0, b"one\n", activity=1)
    store.surrender(KEY, "laptop", "agent-1")
    store.teleport(KEY, AGENT, steal=False)
    n = put(store, g, n, b"two\n", writer=AGENT, activity=1)
    # The laptop resumes a stale copy: its append conflicts, and it forks.
    with pytest.raises(Conflict):
        put(store, g, 4, b"three\n")
    d = store.create_generation(KEY, PATH, LAPTOP, "diverge", parent=g, fork_offset=4)
    put(store, d["id"], 4, b"three\n")
    causes = [(p["host"], p["cause"]) for p in store.manifest(KEY)["placements"]]
    assert causes == [
        ("laptop", "start"),
        ("agent-1", "teleport"),
        ("laptop", "diverge"),
    ]


def test_move_without_teleport(store: Store) -> None:
    g = root(store)
    n = put(store, g, 0, b"one\n")
    put(store, g, n, b"two\n", writer=Writer("acct", "laptop", "/elsewhere"))
    causes = [p["cause"] for p in store.manifest(KEY)["placements"]]
    assert causes == ["start", "move"]


def test_rewrite_never_conflicts(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"aaaa", activity=1)
    put(store, g, 4, b"bbbb", activity=1)
    r = store.create_generation(KEY, PATH, LAPTOP, "rewrite", parent=g, fork_offset=4)
    put(store, r["id"], 4, b"cccc", activity=1)
    state = store.session_state(KEY)
    assert state["state"] == "normal"
    active = next(f for f in store.manifest(KEY)["files"] if f["path"] == PATH)
    assert active["kind"] == "log" and active["active"] == r["id"]


def _diverge(store: Store, laptop_activity: int, agent_activity: int) -> str:
    g = root(store)
    n = put(store, g, 0, b"base\n", activity=1)
    put(store, g, n, b"agent\n", writer=AGENT, activity=agent_activity)
    d = store.create_generation(KEY, PATH, LAPTOP, "diverge", parent=g, fork_offset=n)
    put(store, d["id"], n, b"laptop\n", activity=laptop_activity)
    return d["id"]


def test_two_active_branches_conflict(store: Store) -> None:
    _diverge(store, laptop_activity=1, agent_activity=1)
    state = store.session_state(KEY)
    assert state["state"] == "conflicted"
    assert {b["host"] for b in state.get("branches", [])} == {"laptop", "agent-1"}


def test_fork_inside_a_segment_counts_that_segment(store: Store) -> None:
    # One append holds the shared prefix and the agent's new prompt. The
    # laptop forks at a byte inside that segment (as `_adopt_remote` does at
    # the first differing byte) and adds its own prompt. The server cannot
    # tell where in the segment the agent's activity is, so it must count
    # the full segment, and the session is conflicted.
    g = root(store, writer=AGENT)
    put(store, g, 0, b"common\nprompt A\n", writer=AGENT, activity=1)
    fork = len(b"common\n")
    d = store.create_generation(
        KEY, PATH, LAPTOP, "diverge", parent=g, fork_offset=fork
    )
    put(store, d["id"], fork, b"prompt B\n", activity=1)
    state = store.session_state(KEY)
    assert state["state"] == "conflicted"
    assert {b["host"] for b in state.get("branches", [])} == {"laptop", "agent-1"}


def test_branch_with_only_metadata_does_not_conflict(store: Store) -> None:
    _diverge(store, laptop_activity=0, agent_activity=1)
    assert store.session_state(KEY)["state"] == "normal"
    active = next(f for f in store.manifest(KEY)["files"] if f["path"] == PATH)
    assert active["kind"] == "log"
    gens = {g["id"]: g for g in active["generations"]}
    # The active branch is the one with activity: the root generation, which
    # the agent continued. The laptop's diverge branch has none.
    assert gens[active["active"]]["cause"] == "root"


def test_conflict_is_sticky(store: Store) -> None:
    d = _diverge(store, laptop_activity=1, agent_activity=1)
    put(store, d, len(b"base\nlaptop\n"), b"more\n", activity=0)
    assert store.session_state(KEY)["state"] == "conflicted"


def test_conflicted_session_refuses_teleport(store: Store) -> None:
    _diverge(store, laptop_activity=1, agent_activity=1)
    with pytest.raises(StoreError) as err:
        store.teleport(KEY, OTHER, steal=True)
    assert err.value.code == "conflicted"


def test_recovery_fork(store: Store) -> None:
    d = _diverge(store, laptop_activity=1, agent_activity=1)
    fork = store.create_fork(KEY, d, len(b"base\nlaptop\n"))
    new_key = SessionKey("acct", "claude-code", fork["session_id"])
    assert store.session_exists(new_key)
    g = store.create_generation(new_key, PATH, LAPTOP, "fork")["id"]
    put(store, g, 0, b"forked copy\n", key=new_key, activity=1)
    manifest = store.manifest(new_key)
    assert manifest["forked_from"]["session_id"] == "s1"
    assert manifest["state"]["lease"]["host"] == "laptop"


def test_read_scopes(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"one\n")
    assert store.can_read(KEY, "laptop", "own")
    assert not store.can_read(KEY, "agent-1", "own")
    assert not store.can_read(KEY, "agent-1", "granted")
    assert store.can_read(KEY, "agent-1", "all")
    store.grant(KEY, "agent-1", "laptop")
    assert store.can_read(KEY, "agent-1", "granted")
    assert not store.can_read(KEY, "agent-1", "own")
    assert not store.can_read(SessionKey("acct", "claude-code", "nope"), "x", "all")


def test_list_sessions_filters(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"one\n")
    store.add_metadata(KEY, LAPTOP, {"title": "Hyperspace design", "cwd": "/x"})
    other = SessionKey("acct", "claude-code", "s2")
    g2 = root(store, writer=AGENT, key=other)
    put(store, g2, 0, b"one\n", writer=AGENT, key=other)

    def ids(**kwargs: str) -> list[str]:
        return [
            s["session_id"]
            for s in store.list_sessions("acct", "laptop", "all", **kwargs)
        ]

    assert ids() == ["s2", "s1"]
    assert ids(title="design") == ["s1"]
    assert ids(on_host="agent-1") == ["s2"]
    assert [s["session_id"] for s in store.list_sessions("acct", "laptop", "own")] == [
        "s1"
    ]


def test_purge_session(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"secret\n")
    sha = hashlib.sha256(b"plan").hexdigest()
    store.put_blob(KEY, "plans/p.md", LAPTOP, b"plan", sha)
    store.add_metadata(KEY, LAPTOP, {"title": "has a secret"})
    result = store.purge_session(KEY, "leaked token")
    assert result["generations"] == 1 and result["objects"] == 1
    assert not store.session_exists(KEY)
    assert not (store.logs_dir / f"{g}.zst").exists()
    assert not store.objects.has(sha)
    # The id is free again.
    g2 = root(store)
    put(store, g2, 0, b"clean\n")
    assert store.read_log(KEY, g2, 0, None) == b"clean\n"
    rows = store._db.execute("SELECT target, reason FROM purges").fetchall()
    assert [tuple(r) for r in rows] == [("session acct/claude-code/s1", "leaked token")]


def test_purge_generation_takes_its_forks(store: Store) -> None:
    g = root(store)
    put(store, g, 0, b"aaaa")
    put(store, g, 4, b"bbbb")
    r = store.create_generation(KEY, PATH, LAPTOP, "rewrite", parent=g, fork_offset=4)
    put(store, r["id"], 4, b"cccc")
    other = store.create_generation(KEY, "subagents/a.jsonl", LAPTOP, "root")
    info, _ = store.append(
        KEY, other["id"], LAPTOP, 0, b"x\n", hashlib.sha256(b"x\n").hexdigest(), 0
    )
    assert info["length"] == 2
    result = store.purge_generation(KEY, g, "secret in the root")
    assert result["generations"] == 2
    assert [f["path"] for f in store.manifest(KEY)["files"]] == ["subagents/a.jsonl"]
    with pytest.raises(StoreError):
        store.read_log(KEY, r["id"], 0, None)


def test_purge_blob_version_keeps_shared_objects(store: Store) -> None:
    sha = hashlib.sha256(b"same").hexdigest()
    store.put_blob(KEY, "tool-results/a.txt", LAPTOP, b"same", sha)
    store.put_blob(KEY, "tool-results/b.txt", LAPTOP, b"same", sha)
    result = store.purge_blob_version(KEY, "tool-results/a.txt", 1, "test")
    assert result["objects"] == 0
    assert store.objects.has(sha)
    assert store.read_blob(KEY, "tool-results/b.txt", None) == b"same"
    store.purge_blob_version(KEY, "tool-results/b.txt", 1, "test")
    assert not store.objects.has(sha)
