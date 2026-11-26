"""Tests for verify-before-swap SQL replacement.

Covers:
* consistent -> swap, same params interface keeps working with PREPARE budget;
* missing / duplicate / status differences and the smallest-20 id cap;
* row cap, temp_file_limit (real PG 53400) and bad SQL all skip the swap;
* same REPEATABLE READ snapshot, idle-connection borrowing / busy 409;
* optimistic guard against modify / delete / delete-and-recreate during check;
* 100k-row deployment-scale comparison;
* path/field aliases.
"""
from __future__ import annotations

import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest

from app import state
from app.verify import compare_pairs


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
VTABLE = "verify_diff_rows"


def _dsn() -> str:
    return os.environ["DATABASE_URL"]


def _exec(sql: str, params=None):
    with psycopg.connect(_dsn(), autocommit=True) as conn:
        conn.execute(sql, params)


@pytest.fixture
def diff_table(client):
    """A small deterministic table: ids 1..30, all status 'ok'."""
    _exec(f"DROP TABLE IF EXISTS {VTABLE}")
    _exec(f"CREATE TABLE {VTABLE} (id INTEGER PRIMARY KEY, status TEXT NOT NULL)")
    _exec(
        f"INSERT INTO {VTABLE} (id, status) "
        "SELECT g, 'ok' FROM generate_series(1, 30) g"
    )
    yield VTABLE
    _exec(f"DROP TABLE IF EXISTS {VTABLE}")


@pytest.fixture
def put(client):
    def _put(session, name, sql):
        resp = client.put(
            f"/api/sessions/{session}/bindings/{name}", json={"sql": sql}
        )
        assert resp.status_code == 200, resp.text
        return resp.json()["binding"]

    return _put


def verify(client, session, name, body, path_tail="verify", method="post"):
    url = f"/api/sessions/{session}/bindings/{name}/{path_tail}"
    return getattr(client, method)(url, json=body)


def cache_statement_count(client):
    cache = client.get("/api/cache").json()
    return sum(c["prepared_count"] for c in cache["connections"])


# ---------------------------------------------------------------------------
# pure comparison semantics
# ---------------------------------------------------------------------------
def test_compare_pairs_detects_three_diff_kinds_and_sorts():
    old = [(1, "ok"), (2, "ok"), (3, "ok"), (4, "ok"), (4, "ok"), (6, "ok")]
    new = [(1, "ok"), (2, "bad"), (3, "ok"), (4, "ok"), (5, "x")]
    out = compare_pairs(old, new)
    assert out["consistent"] is False
    by_id = {d["id"]: d for d in out["differences"]}
    # id 2 status changed
    assert by_id[2]["kind"] == "status_mismatch"
    # id 4 duplicated in old
    assert by_id[4]["kind"] == "duplicate_id" and by_id[4]["duplicate"] is True
    # id 5 missing in old, id 6 missing in new
    assert by_id[5]["kind"] == "missing_in_old"
    assert by_id[6]["kind"] == "missing_in_new"
    assert out["difference_ids"] == [2, 4, 5, 6]


def test_smallest_20_difference_ids_only():
    old = [(i, "ok") for i in range(1, 41)]
    new = [(i, "changed") if i % 2 == 0 else (i, "ok") for i in range(1, 41)]
    out = compare_pairs(old, new)
    assert out["difference_count"] == 20
    assert out["difference_ids"] == list(range(2, 41, 2))[:20]
    assert len(out["differences"]) == 20

    # 25 differing ids -> only the smallest 20 are returned.
    old = [(i, "ok") for i in range(1, 31)]
    new = [(i, "changed") if i <= 25 else (i, "ok") for i in range(1, 31)]
    out = compare_pairs(old, new)
    assert out["difference_count"] == 25
    assert out["difference_ids"] == list(range(1, 21))
    assert out["ids_truncated"] is True


# ---------------------------------------------------------------------------
# docs / openapi / aliases
# ---------------------------------------------------------------------------
def test_docs_list_verify_endpoint(client):
    spec = client.get("/openapi.json").json()
    assert "/api/sessions/{session}/bindings/{name}/verify" in spec["paths"]
    assert client.get("/docs").status_code == 200


def test_path_and_field_aliases(client, put):
    put("al", "rpt", "SELECT id, status FROM deployments WHERE id = $1")
    # query-name path + alternative field names
    r = client.post(
        "/api/sessions/al/queries/rpt/check",
        json={
            "new_sql": "SELECT id, status FROM deployments WHERE id = $1",
            "parameters": [1],
            "temp_file_limit": 64,
            "row_limit": 1000,
        },
    )
    assert r.status_code == 200, r.text
    j = r.json()
    assert j["consistent"] is True
    # /check does not swap by default
    assert j["swapped"] is False

    # body-driven root alias
    r2 = client.post(
        "/verify-sql",
        json={
            "session": "al",
            "query_name": "rpt",
            "candidate": "SELECT id, status FROM deployments WHERE id = $1",
            "args": [1],
            "temp_file_limit_mib": "64MB",
            "max_rows": 1000,
        },
    )
    assert r2.status_code == 200, r2.text
    assert r2.json()["swapped"] is True


@pytest.mark.parametrize("path", [
    "/api/sessions/al/bindings/rpt/verify-sql",
    "/api/sessions/al/bindings/rpt/compare",
    "/api/sessions/al/queries/rpt/verify",
    "/api/bindings/al/rpt/verify",
    "/api/verify-sql/al/rpt",
    "/api/sql/verify",
    "/api/bindings/verify",
    "/api/verifications",
    "/verify",  # root unversioned alias
])
def test_many_path_aliases_reach_feature(client, put, path):
    put("al", "rpt", "SELECT 1::int AS id, 'ok'::text AS status")
    body = {
        "session": "al", "name": "rpt",
        "candidate_sql": "SELECT 1::int AS id, 'ok'::text AS status",
        "params": [], "max_rows": 10,
        # compare-style paths default to no swap; force swap on the rest.
        "auto_swap": True,
    }
    r = client.post(path, json=body)
    assert r.status_code == 200, f"{path} -> {r.status_code} {r.text[:200]}"
    assert r.json()["consistent"] is True


def test_nested_envelope_and_dict_candidate(client, put):
    put("ne", "rpt", "SELECT 1::int AS id, 'ok'::text AS status")
    r = client.post("/api/verify", json={
        "request": {
            "session": "ne", "query_name": "rpt",
            "candidate": {"sql": "SELECT 1::int AS id, 'ok'::text AS status"},
            "parameters": [],
            "temp_file_limit_mib": 32, "max_rows": 50,
        }
    })
    assert r.status_code == 200, r.text
    assert r.json()["swapped"] is True


def test_direct_dict_candidate_and_string_limits_not_rejected(client, put):
    put("dc", "rpt", "SELECT 1::int AS id, 'ok'::text AS status")
    r = client.post("/api/sessions/dc/bindings/rpt/verify", json={
        "candidate": {"sql": "SELECT 1::int AS id, 'ok'::text AS status",
                      "params": []},
        "max_rows": "500",
        "temp_file_limit": "32MB",
    })
    assert r.status_code == 200, r.text
    assert r.json()["consistent"] is True


def test_template_binding_extracts_id_status_by_name(client):
    # The built-in template returns 7 columns including id and status; verify
    # must locate them by name and compare just that projection.
    r = client.put(
        "/api/sessions/tpl/bindings/rpt",
        json={"template": "deployments_by_service"},
    )
    assert r.status_code == 200
    resp = client.post(
        "/api/sessions/tpl/bindings/rpt/verify",
        json={
            "candidate_sql": (
                "SELECT id, status FROM deployments "
                "WHERE service = :service ORDER BY id"
            ),
            "params": {"service": "api-gateway"},
            "temp_file_limit_mib": 64,
            "max_rows": 100000,
        },
    )
    assert resp.status_code == 200, resp.text
    j = resp.json()
    assert j["consistent"] is True and j["swapped"] is True
    assert j["old_row_count"] == j["candidate_row_count"] == 21


def test_empty_body_is_422_not_500(client, put):
    put("eb", "rpt", "SELECT 1 AS id, 'ok' AS status")
    r = client.post("/api/sessions/eb/bindings/rpt/verify")
    assert r.status_code == 422
    assert r.json()["code"] in {"missing_candidate_sql", "invalid_verify_request"}


# ---------------------------------------------------------------------------
# consistent verify -> swap -> original params interface unchanged
# ---------------------------------------------------------------------------
def test_consistent_verify_swaps_and_params_still_work(client, put):
    old_sql = "SELECT id, status FROM deployments WHERE service = $1 ORDER BY id"
    new_sql = "SELECT id, status FROM deployments WHERE service = :service ORDER BY id"
    put("sw", "rpt", old_sql)

    r = verify(
        client, "sw", "rpt",
        {"candidate_sql": new_sql, "params": {"service": "api-gateway"},
         "temp_file_limit_mib": 64, "max_rows": 100000},
    )
    assert r.status_code == 200, r.text
    j = r.json()
    assert j["consistent"] is True and j["swapped"] is True
    assert j["status"] == "swapped"
    assert j["old_row_count"] == j["candidate_row_count"] == 21
    assert j["difference_ids"] == []
    assert j["connection_slot"] in {"pg-conn-1", "pg-conn-2"}
    assert isinstance(j["backend_pid"], int)

    current = client.get("/api/sessions/sw/bindings/rpt").json()
    assert "service = $1" in current["sql"]
    assert current["version"] == 2

    # Verification must not warm/pollute the rq_ prepared-statement LRU.
    assert cache_statement_count(client) == 0

    # Original params interface (named object) works through /api/batches and
    # the new SQL is a fresh cache key -> exactly one PREPARE within budget 1.
    batch = client.post(
        "/api/batches",
        json={
            "max_prepares": 1,
            "queries": [{
                "session": "sw", "name": "rpt",
                "params": {"service": "api-gateway"},
                "arrival_tick": 0, "duration_ticks": 1, "deadline_tick": 10,
            }],
        },
    )
    assert batch.status_code == 200, batch.text
    q0 = batch.json()["queries"][0]
    assert q0["row_count"] == 21 and q0["prepare_count"] == 1

    # A follow-up batch with PREPARE budget 0 is a warm hit.
    again = client.post(
        "/api/batches",
        json={
            "max_prepares": 0,
            "queries": [{
                "session": "sw", "name": "rpt",
                "params": ["api-gateway"],
                "arrival_tick": 0, "duration_ticks": 1, "deadline_tick": 10,
            }],
        },
    )
    assert again.status_code == 200, again.text
    assert again.json()["queries"][0]["cache_hit"] is True


def test_accepted_batch_keeps_old_binding_snapshot_after_swap(client, put):
    old_sql = "SELECT id, status FROM deployments WHERE version = $1 ORDER BY id"
    new_sql = "SELECT id, status FROM deployments WHERE version = $1 AND id > 0 ORDER BY id"
    put("snap", "rpt", old_sql)

    b = client.post(
        "/api/batches",
        json={
            "max_prepares": 1,
            "queries": [{
                "session": "snap", "name": "rpt", "params": ["v2.0.1"],
                "arrival_tick": 0, "duration_ticks": 1, "deadline_tick": 10,
            }],
        },
    )
    assert b.status_code == 200
    bid = b.json()["batch_id"]
    assert "version = $1 ORDER BY id" in b.json()["snapshot"][0]["sql"]

    r = verify(client, "snap", "rpt", {
        "candidate_sql": new_sql, "params": ["v2.0.1"],
        "temp_file_limit_mib": 64, "max_rows": 100000,
    })
    assert r.json()["swapped"] is True

    stored = client.get(f"/api/batches/{bid}").json()
    assert "AND id > 0" not in stored["snapshot"][0]["sql"]


# ---------------------------------------------------------------------------
# differences over HTTP against a real table
# ---------------------------------------------------------------------------
def test_http_differences_return_smallest_20_and_counts(client, put, diff_table):
    put("df", "rpt", f"SELECT id, status FROM {VTABLE} ORDER BY id")
    candidate = (
        f"SELECT id, CASE WHEN id <= 25 THEN 'changed' ELSE status END AS status "
        f"FROM {VTABLE} ORDER BY id"
    )
    r = verify(client, "df", "rpt", {
        "candidate_sql": candidate, "params": [],
        "temp_file_limit_mib": 64, "max_rows": 1000,
    })
    assert r.status_code == 200, r.text
    j = r.json()
    assert j["consistent"] is False and j["swapped"] is False
    assert j["status"] == "differences_found"
    assert j["difference_count"] == 25
    assert j["difference_ids"] == list(range(1, 21))
    assert j["old_row_count"] == 30 and j["candidate_row_count"] == 30
    # binding untouched
    cur = client.get("/api/sessions/df/bindings/rpt").json()
    assert "CASE WHEN" not in cur["sql"] and cur["version"] == 1
    # raw package present
    assert len(j["old_rows"]) == 30 and len(j["candidate_rows"]) == 30
    assert j["raw_results"]["old"]["row_count"] == 30


def test_http_missing_and_duplicate_differences(client, put, diff_table):
    put("md", "rpt", f"SELECT id, status FROM {VTABLE} ORDER BY id")
    dup_sql = (
        f"SELECT id, status FROM {VTABLE} "
        f"UNION ALL SELECT id, status FROM {VTABLE} WHERE id = 5"
    )
    j = verify(client, "md", "rpt", {
        "candidate_sql": dup_sql, "params": [], "max_rows": 1000,
    }).json()
    assert j["swapped"] is False
    kinds = {d["id"]: d["kind"] for d in j["differences"]}
    assert kinds[5] == "duplicate_id"

    missing_sql = f"SELECT id, status FROM {VTABLE} WHERE id <= 28"
    j = verify(client, "md", "rpt", {
        "candidate_sql": missing_sql, "params": [], "max_rows": 1000,
    }).json()
    kinds = {d["id"]: d["kind"] for d in j["differences"]}
    assert kinds[29] == "missing_in_new" and kinds[30] == "missing_in_new"


# ---------------------------------------------------------------------------
# limits / errors -> skip
# ---------------------------------------------------------------------------
def test_row_cap_exceeded_skips_swap(client, put, diff_table):
    put("lim", "rpt", f"SELECT id, status FROM {VTABLE} ORDER BY id")
    r = verify(client, "lim", "rpt", {
        "candidate_sql": f"SELECT id, status FROM {VTABLE} ORDER BY id",
        "params": [], "max_rows": 10, "temp_file_limit_mib": 64,
    })
    assert r.status_code == 200
    j = r.json()
    assert j["swapped"] is False and j["skipped"] is True
    assert j["reason"] == "row_limit_exceeded"
    assert j["row_limit_exceeded"] is True
    assert j["old_row_count"] == 30
    cur = client.get("/api/sessions/lim/bindings/rpt").json()
    assert cur["version"] == 1


def test_temp_file_limit_exceeded_skips_swap(client, put):
    # Old version is a single cheap row; candidate spills (>1 MiB) under
    # work_mem=1MB and temp_file_limit=1MB -> real PG sqlstate 53400.
    put("tmp", "rpt", "SELECT 1::int AS id, 'ok'::text AS status")
    candidate = (
        "SELECT g::int AS id, repeat((g % 7)::text || '-status-', 20) AS status "
        "FROM generate_series(1, 200000) g ORDER BY status, id"
    )
    r = verify(client, "tmp", "rpt", {
        "candidate_sql": candidate, "params": [],
        "temp_file_limit_mib": 1, "max_rows": 1_000_000,
    })
    assert r.status_code == 200, r.text
    j = r.json()
    assert j["swapped"] is False and j["skipped"] is True
    assert j["reason"] == "temp_file_limit_exceeded"
    assert j["sqlstate"] == "53400"
    assert client.get("/api/sessions/tmp/bindings/rpt").json()["version"] == 1
    # Connection is still usable after the aborted transaction.
    health = client.get("/health").json()
    assert health["status"] == "ok"


def test_bad_candidate_sql_skips_swap(client, put):
    put("err", "rpt", "SELECT 1::int AS id, 'ok'::text AS status")
    r = verify(client, "err", "rpt", {
        "candidate_sql": "SELECT id, status FROM no_such_verify_table_xyz",
        "params": [], "max_rows": 100, "temp_file_limit_mib": 64,
    })
    assert r.status_code == 200
    j = r.json()
    assert j["swapped"] is False and j["reason"] == "execution_error"
    assert j["error"]["sqlstate"] == "42P01"
    assert client.get("/api/sessions/err/bindings/rpt").json()["version"] == 1


def test_missing_binding_404(client):
    r = client.post(
        "/api/sessions/nope/bindings/ghost/verify",
        json={"candidate_sql": "SELECT 1 AS id, 'x' AS status", "max_rows": 10},
    )
    assert r.status_code == 404
    assert r.json()["code"] == "binding_not_found"


def test_invalid_params_422(client, put):
    put("bp", "rpt", "SELECT id, status FROM deployments WHERE service = $1")
    r = client.post(
        "/api/sessions/bp/bindings/rpt/verify",
        json={"candidate_sql": "SELECT id, status FROM deployments WHERE service = $1",
              "params": [], "max_rows": 10},
    )
    assert r.status_code == 422
    assert r.json()["code"] == "invalid_params"


# ---------------------------------------------------------------------------
# idle connection borrowing
# ---------------------------------------------------------------------------
def test_verify_borrows_other_idle_connection(client, put):
    put("conn", "rpt", "SELECT 1::int AS id, 'ok'::text AS status")
    slot0 = state.runtime.slots[0]
    with slot0.lock:
        r = client.post(
            "/api/sessions/conn/bindings/rpt/verify",
            json={"candidate_sql": "SELECT 1::int AS id, 'ok'::text AS status",
                  "params": [], "max_rows": 10},
        )
    assert r.status_code == 200, r.text
    j = r.json()
    assert j["connection"] == 1
    assert j["connection_slot"] == "pg-conn-2"
    assert j["backend_pid"] == state.runtime.slots[1].pid


def test_no_idle_connection_409(client, put):
    put("busy", "rpt", "SELECT 1::int AS id, 'ok'::text AS status")
    locks = [s.lock for s in state.runtime.slots]
    for lk in locks:
        lk.acquire()
    try:
        r = client.post(
            "/api/sessions/busy/bindings/rpt/verify",
            json={"candidate_sql": "SELECT 1::int AS id, 'ok'::text AS status",
                  "params": [], "max_rows": 10},
        )
        assert r.status_code == 409
        assert r.json()["code"] == "no_idle_connection"
    finally:
        for lk in locks:
            lk.release()


# ---------------------------------------------------------------------------
# optimistic binding guard: modify / delete / delete+recreate
# ---------------------------------------------------------------------------
def test_compare_and_put_detects_all_concurrent_outcomes():
    store = state.bindings
    b, _ = store.put("g", "n", sql="SELECT 1 AS id, 'a' AS status")
    tok = store.identity_token("g", "n")
    store.put("g", "n", sql="SELECT 2 AS id, 'b' AS status")
    out = store.compare_and_put("g", "n", tok["generation"],
                                expected_deletes=tok["deletes"],
                                sql="SELECT 3 AS id, 'c' AS status")
    assert out["swapped"] is False and out["outcome"] == "modified"
    assert store.get("g", "n").sql == "SELECT 2 AS id, 'b' AS status"

    tok = store.identity_token("g", "n")
    store.delete("g", "n")
    out = store.compare_and_put("g", "n", tok["generation"],
                                expected_deletes=tok["deletes"],
                                sql="SELECT 3 AS id, 'c' AS status")
    assert out["outcome"] == "deleted"

    # delete then recreate under the SAME name -> version resets to 1 but the
    # generation token must still expose the change.
    store.put("g", "n", sql="SELECT 9 AS id, 'z' AS status")
    tok = store.identity_token("g", "n")
    assert tok["version"] == 1
    store.delete("g", "n")
    store.put("g", "n", sql="SELECT 10 AS id, 'z' AS status")
    out = store.compare_and_put("g", "n", tok["generation"],
                                expected_deletes=tok["deletes"],
                                sql="SELECT 11 AS id, 'candidate' AS status")
    assert out["outcome"] == "deleted_and_recreated"
    assert store.get("g", "n").sql == "SELECT 10 AS id, 'z' AS status"


def test_two_phase_apply_refuses_delete_recreate(client, put):
    put("tp", "rpt", "SELECT 1::int AS id, 'ok'::text AS status")
    r = client.post(
        "/api/sessions/tp/bindings/rpt/check",
        json={"candidate_sql": "SELECT 1::int AS id, 'ok'::text AS status",
              "params": [], "max_rows": 10},
    )
    vid = r.json()["verification_id"]
    assert r.json()["swapped"] is False

    client.delete("/api/sessions/tp/bindings/rpt")
    client.put("/api/sessions/tp/bindings/rpt", json={
        "sql": "SELECT 2::int AS id, 'new-owner'::text AS status"})

    applied = client.post(f"/api/verifications/{vid}/apply").json()
    assert applied["swapped"] is False
    assert applied["apply"]["reason"] in {
        "binding_deleted_and_recreated", "binding_changed", "binding_deleted"}

    cur = client.get("/api/sessions/tp/bindings/rpt").json()
    assert "new-owner" in cur["sql"] and cur["version"] == 1


def test_oneshot_verify_skips_when_delete_recreate_mid_check(client, put, monkeypatch):
    put("race", "rpt", "SELECT 1::int AS id, 'ok'::text AS status")

    entered = threading.Event()
    release = threading.Event()
    import app.verify as verify_mod

    original = verify_mod.run_snapshot_compare

    def gated(*a, **kw):
        entered.set()
        assert release.wait(timeout=10)
        return original(*a, **kw)

    monkeypatch.setattr(verify_mod, "run_snapshot_compare", gated)

    body = {
        "candidate_sql": "SELECT 1::int AS id, 'ok'::text AS status",
        "params": [], "max_rows": 10, "temp_file_limit_mib": 64,
    }
    with ThreadPoolExecutor(max_workers=1) as pool:
        fut = pool.submit(
            client.post, "/api/sessions/race/bindings/rpt/verify", json=body
        )
        assert entered.wait(timeout=10)
        # Concurrent same-name delete + recreate while the check is running.
        assert client.delete("/api/sessions/race/bindings/rpt").status_code == 200
        recreated = client.put(
            "/api/sessions/race/bindings/rpt",
            json={"sql": "SELECT 7::int AS id, 'recreated'::text AS status"},
        )
        assert recreated.status_code == 200
        release.set()
        resp = fut.result(timeout=30)

    assert resp.status_code == 200, resp.text
    j = resp.json()
    assert j["swapped"] is False and j["skipped"] is True
    assert j["reason"] in {
        "binding_changed", "binding_deleted_and_recreated", "binding_deleted"}
    # The actual stored binding is the recreated one, not the candidate.
    cur = client.get("/api/sessions/race/bindings/rpt").json()
    assert "recreated" in cur["sql"]
    assert cur["version"] == 1


# ---------------------------------------------------------------------------
# 100k deployment-scale rows
# ---------------------------------------------------------------------------
def test_supports_100k_rows(client, put):
    big = "verify_big_" + uuid.uuid4().hex[:8]
    try:
        _exec(f"CREATE TABLE {big} (id INTEGER PRIMARY KEY, status TEXT NOT NULL)")
        _exec(
            f"INSERT INTO {big} (id, status) "
            "SELECT g, CASE g % 4 WHEN 0 THEN 'success' WHEN 1 THEN 'failed' "
            "WHEN 2 THEN 'running' ELSE 'queued' END "
            "FROM generate_series(1, 100000) g"
        )
        put("big", "rpt", f"SELECT id, status FROM {big} ORDER BY id")
        # Candidate is logically identical but differently written.
        candidate = (
            f"SELECT id, status FROM {big} WHERE id > 0 ORDER BY id ASC"
        )
        r = client.post(
            "/api/sessions/big/bindings/rpt/verify",
            json={"candidate_sql": candidate, "params": [],
                  "temp_file_limit_mib": 256, "max_rows": 100000},
        )
        assert r.status_code == 200, r.text
        j = r.json()
        assert j["consistent"] is True, j.get("difference_ids")
        assert j["old_row_count"] == 100000
        assert j["candidate_row_count"] == 100000
        assert j["swapped"] is True
    finally:
        _exec(f"DROP TABLE IF EXISTS {big}")
