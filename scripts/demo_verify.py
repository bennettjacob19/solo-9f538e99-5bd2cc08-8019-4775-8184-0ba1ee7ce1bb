#!/usr/bin/env python3
"""Live self-test for verify-before-swap, including the tricky
"same-name binding deleted then recreated during verification" case.

Targets a running server (default http://127.0.0.1:8000) and uses only the
Python standard library:

    python scripts/serve_local.py            # terminal 1
    python scripts/demo_verify.py            # terminal 2
    BASE_URL=http://localhost:8000 python scripts/demo_verify.py
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000").rstrip("/")
FAILURES: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"  [{mark}] {label}" + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        FAILURES.append(label + (f" ({detail})" if detail else ""))


def call(method: str, path: str, body=None, timeout: float = 60.0):
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, {"raw": raw}


def wait_ready() -> None:
    for _ in range(60):
        try:
            status, body = call("GET", "/health")
            if status == 200 and body.get("status") == "ok":
                print(f"server ready at {BASE}")
                return
        except Exception:
            pass
        time.sleep(1)
    print("server not ready; start it with: python scripts/serve_local.py")
    sys.exit(2)


def put_binding(session, name, sql):
    return call("PUT", f"/api/sessions/{session}/bindings/{name}", {"sql": sql})


def verify_body(sql, params=None, **kw):
    body = {
        "candidate_sql": sql,
        "params": params if params is not None else [],
        "temp_file_limit_mib": 256,
        "max_rows": 100000,
    }
    body.update(kw)
    return body


def main() -> int:
    wait_ready()
    token = uuid.uuid4().hex[:8]
    s = f"vf_{token}"

    print("\n=== 1. consistent candidate swaps; params interface unchanged ===")
    # Unique comment keeps the physical statement key cold on repeat runs.
    old = (f"SELECT id, status FROM deployments WHERE service = $1 "
           f"/* vd-{token} */ ORDER BY id")
    new = (f"SELECT id, status FROM deployments WHERE service = :service "
           f"/* vd-{token} */ ORDER BY id")
    st, put = put_binding(s, "rpt", old)
    check("PUT old $1 binding", st == 200, str(put)[:200])

    st, ok = call("POST", f"/api/sessions/{s}/bindings/rpt/verify",
                  verify_body(new, {"service": "api-gateway"}))
    check("verify -> 200 swapped", st == 200 and ok.get("swapped") is True,
          f"st={st} body={str(ok)[:300]}")
    check("21 api-gateway rows both sides",
          ok.get("old_row_count") == 21 and ok.get("candidate_row_count") == 21)
    check("used a real backend pid", isinstance(ok.get("backend_pid"), int))
    _, cur = call("GET", f"/api/sessions/{s}/bindings/rpt")
    check("binding now v2 with canonical $1 SQL",
          cur.get("version") == 2 and "service = $1" in cur.get("sql", ""))

    st, batch = call("POST", "/api/batches", {
        "max_prepares": 1,
        "queries": [{
            "session": s, "name": "rpt",
            "params": {"service": "api-gateway"},
            "arrival_tick": 0, "duration_ticks": 1, "deadline_tick": 10,
        }],
    })
    check("batch with original named params works after swap",
          st == 200 and batch["queries"][0]["row_count"] == 21
          and batch["queries"][0]["prepare_count"] == 1,
          str(batch)[:300])

    print("\n=== 2. differences: smallest 20 ids + real row counts ===")
    st_d2, _ = put_binding(
        s, "d", "SELECT g AS id, 'ok'::text AS status FROM generate_series(1,30) g"
    )
    check("create helper binding d", st_d2 == 200)
    cand = ("SELECT g AS id, CASE WHEN g <= 25 THEN 'x' ELSE 'ok' END AS status "
            "FROM generate_series(1,30) g")
    st, diff = call("POST", f"/api/sessions/{s}/bindings/d/verify", verify_body(cand))
    check("25 differences, ids 1..20 returned",
          st == 200 and diff.get("swapped") is False
          and diff.get("difference_count") == 25
          and diff.get("difference_ids") == list(range(1, 21)),
          str(diff.get("difference_ids")))
    check("row counts reported (30/30)",
          diff.get("old_row_count") == 30 and diff.get("candidate_row_count") == 30)
    check("raw rows + detail packaged",
          len(diff.get("old_rows") or []) == 30
          and len(diff.get("differences") or []) == 20)

    print("\n=== 3. row cap exceeded -> skip swap ===")
    st, capped = call("POST", f"/api/sessions/{s}/bindings/d/verify",
                      verify_body(
                          "SELECT g AS id, 'ok'::text AS status "
                          "FROM generate_series(1,30) g", max_rows=10))
    check("over row cap -> skipped row_limit_exceeded",
          capped.get("swapped") is False
          and capped.get("reason") == "row_limit_exceeded"
          and capped.get("old_row_count") == 30,
          str(capped)[:300])

    print("\n=== 4. bad SQL -> reported error and skip ===")
    st, bad = call("POST", f"/api/sessions/{s}/bindings/rpt/verify",
                   verify_body(
                       "SELECT id, status FROM no_such_table_zzz WHERE service = $1",
                       ["api-gateway"]))
    check("missing table -> execution_error / 42P01, not swapped",
          bad.get("swapped") is False
          and bad.get("reason") == "execution_error"
          and (bad.get("error") or {}).get("sqlstate") == "42P01",
          str(bad)[:300])

    print("\n=== 5. THE scenario: same-name DELETE + RECREATE during check ===")
    # Candidate returns one identical row but takes ~4s, giving a deterministic
    # window in which to delete and recreate the same session/name.
    slow_consistent = (
        "SELECT 1::int AS id, 'ok'::text AS status "
        "WHERE (SELECT count(*) FROM ("
        "  SELECT pg_sleep(0.02) FROM generate_series(1,200)"
        ") z) = 200"
    )
    put_binding(s, "race", "SELECT 1::int AS id, 'ok'::text AS status")

    result: dict = {}

    def worker():
        st_, body_ = call(
            "POST", f"/api/sessions/{s}/bindings/race/verify",
            verify_body(slow_consistent), timeout=30,
        )
        result["status"] = st_
        result["body"] = body_

    t = threading.Thread(target=worker)
    t.start()
    time.sleep(1.0)  # let the verify enter its snapshot transaction
    st_d, _ = call("DELETE", f"/api/sessions/{s}/bindings/race")
    st_r, recreated = put_binding(
        s, "race", "SELECT 7::int AS id, 'recreated-v2'::text AS status"
    )
    check("concurrent DELETE 200 then PUT recreate 200", st_d == 200 and st_r == 200)
    t.join(timeout=30)

    body = result.get("body") or {}
    check("in-flight verify finished 200 and did NOT swap",
          result.get("status") == 200 and body.get("swapped") is False
          and body.get("skipped") is True, str(body)[:400])
    check("reason is a binding-change family code",
          body.get("reason") in {
              "binding_changed", "binding_deleted_and_recreated", "binding_deleted"},
          f"reason={body.get('reason')}")

    # ---- verify the ACTUAL stored change: recreated binding must survive ---
    st_g, cur = call("GET", f"/api/sessions/{s}/bindings/race")
    check("stored binding is the recreated SQL (candidate lost)",
          st_g == 200 and "recreated-v2" in cur.get("sql", "")
          and cur.get("version") == 1,
          str(cur)[:300])

    # A fresh verify against the recreated binding now swaps normally.
    st, swapped2 = call("POST", f"/api/sessions/{s}/bindings/race/verify",
                        verify_body(
                            "SELECT 7::int AS id, 'recreated-v2'::text AS status"))
    check("matching verify on recreated binding swaps it to v2",
          st == 200 and swapped2.get("swapped") is True
          and swapped2.get("new_binding_version") == 2, str(swapped2)[:300])

    print("\n=== 6. two-phase check -> delete/recreate -> apply refused ===")
    put_binding(s, "tp", "SELECT 1::int AS id, 'ok'::text AS status")
    st, chk = call("POST", f"/api/sessions/{s}/bindings/tp/check",
                   verify_body("SELECT 1::int AS id, 'ok'::text AS status"))
    vid = chk.get("verification_id")
    check("/check is consistent but does not swap",
          chk.get("consistent") is True and chk.get("swapped") is False and vid)
    call("DELETE", f"/api/sessions/{s}/bindings/tp")
    call("PUT", f"/api/sessions/{s}/bindings/tp",
         {"sql": "SELECT 9::int AS id, 'someone-else'::text AS status"})
    st, applied = call("POST", f"/api/verifications/{vid}/apply")
    check("apply after delete/recreate is refused",
          applied.get("swapped") is False
          and (applied.get("apply") or {}).get("applied") is not True,
          str(applied)[:300])
    _, cur = call("GET", f"/api/sessions/{s}/bindings/tp")
    check("someone-else recreated binding intact",
          "someone-else" in cur.get("sql", ""))

    print("\n" + "=" * 64)
    if FAILURES:
        print(f"DEMO_VERIFY FAILED: {len(FAILURES)} check(s):")
        for f in FAILURES:
            print(" -", f)
        return 1
    print("DEMO_VERIFY: ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
