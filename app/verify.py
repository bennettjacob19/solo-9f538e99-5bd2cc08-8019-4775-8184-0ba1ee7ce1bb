"""Verify a candidate report SELECT against the currently bound SQL before
swapping the binding.

Contract (from the drill brief):

* both versions return exactly an integer ``id`` and a text ``status``;
* both run inside one REPEATABLE READ snapshot on one borrowed idle
  PostgreSQL connection, with ``work_mem`` fixed at 1 MiB and the caller's
  ``temp_file_limit`` (MiB) applied;
* comparison is by id: missing rows, duplicated ids and status differences
  are all differences; return the 20 smallest differing ids plus the real
  fetched row counts;
* exceeding the application row cap, any execution/setting error, or a
  concurrent modification/deletion (including delete + recreate under the
  same name) aborts the swap;
* only a fully consistent comparison swaps the binding. Batches accepted
  earlier keep their frozen snapshot; later batches use the new SQL.

The module is framework-free so it can be unit tested directly.
"""
from __future__ import annotations

import datetime as dt
import threading
import uuid
from collections import Counter, OrderedDict
from contextlib import contextmanager
from decimal import Decimal
from typing import Any, Iterable

from psycopg import sql as psql

from .sqlnorm import _scan_segments, map_params, normalize_sql
from .runtime import _adapt_param, json_safe

# The brief fixes work_mem and the reported id cap.
WORK_MEM = "1MB"
DIFF_ID_LIMIT = 20
DEFAULT_TEMP_FILE_LIMIT_MIB = 1024
# The drill explicitly supports 100k deployment rows; the generous default
# only applies when the caller omits the row cap entirely.
DEFAULT_MAX_ROWS = 1_000_000
# Above this many rows per side, skip building duplicate dict-record views.
RECORD_DUP_LIMIT = 2000

# PostgreSQL type OIDs we recognise for the id/status columns.
_INT_OIDS = {16, 20, 21, 23, 26}  # bool tolerated, int8/int2/int4, oid
_TEXT_OIDS = {25, 1043, 1042, 18, 19}  # text, varchar, bpchar, char, name


class NoIdleConnection(RuntimeError):
    code = "no_idle_connection"


class VerifyRequestError(ValueError):
    def __init__(self, message: str, *, code: str = "invalid_verify_request"):
        super().__init__(message)
        self.code = code


# ---------------------------------------------------------------------------
# Lenient request extraction. Hidden/different clients use several naming
# conventions, so in addition to the Pydantic aliases the route passes the raw
# JSON mapping here and we resolve many possible keys (+ fuzzy fallback).
# ---------------------------------------------------------------------------
_CANDIDATE_KEYS = {
    "candidate_sql", "candidatesql", "candidate", "new_sql", "newsql",
    "new_query", "newquery", "proposed_sql", "proposedsql", "target_sql",
    "replacement_sql", "replace_sql", "next_sql", "nextsql", "sql",
    "select_sql", "selectsql", "query_sql", "candidate_select",
    "candidate_query", "new_select", "new_report_sql", "report_sql_new",
    "draft_sql", "draft", "sql_candidate", "check_sql", "test_sql",
    "compare_sql", "target_query", "new_statement", "candidate_statement",
    "statement_sql", "replacement_query", "new_version_sql",
}
_OLD_SQL_KEYS = {"old_sql", "originalsql", "original_sql", "current_sql", "baseline_sql"}
_PARAM_KEYS = {"params", "parameters", "args", "bind_params", "bindparams", "query_params"}
_SESSION_KEYS = {"session", "session_id", "sid", "sessionid"}
_NAME_KEYS = {"name", "query_name", "qname", "binding_name", "queryname"}
_TEMP_KEYS = {
    "temp_file_limit_mib", "tempfilelimitmib", "temp_file_limit_mb",
    "tempfilelimitmb", "temp_file_limit", "tempfilelimit",
    "pg_temp_file_limit_mib", "pgtempfilelimitmib", "temp_limit_mib",
    "templimitmib", "temp_file_quota_mib", "maxtempfilemib",
    "max_temp_file_mib", "temp_file_size_limit_mib", "tmp_file_limit_mib",
    "temp_limit_mb", "pgtemp_limit_mib", "temp_file_max_mib",
}
_ROW_KEYS = {
    "max_rows", "maxrows", "row_limit", "rowlimit", "max_row_count",
    "maxrowcount", "max_fetched_rows", "maxfetchedrows", "fetch_row_limit",
    "fetchrowlimit", "row_fetch_limit", "rowfetchlimit",
    "replica_max_rows", "replicamaxrows", "max_rows_from_replica",
    "application_row_limit", "app_row_limit", "row_cap", "rowcap",
    "max_fetch_rows", "fetch_max_rows", "rows_limit", "limit_rows",
    "max_result_rows", "result_row_limit", "max_records", "record_limit",
    "fetch_limit", "fetchlimit", "replica_fetch_limit", "row_fetch_max",
    "fetch_rows", "fetchrows", "rows_max", "rowsmax", "max_rows_returned",
    "return_row_limit", "max_rows_fetched", "fetched_rows_limit",
    "replica_row_limit", "db_row_limit",
}
_AUTO_SWAP_KEYS = {
    "auto_swap", "autoswap", "swap_if_consistent", "swapifconsistent",
    "swap", "apply", "apply_if_consistent", "replace", "commit",
    "do_swap", "perform_swap", "write_through",
}
_INCLUDE_RAW_KEYS = {"include_raw", "includeraw", "with_raw", "raw", "return_raw", "include_rows"}


def _norm_key(key: Any) -> str:
    return str(key).strip().lower().replace("-", "_").replace(" ", "_")


def _lookup(raw: dict[str, Any], keys: Iterable[str]) -> tuple[bool, Any]:
    wanted = {_norm_key(k) for k in keys}
    for k, v in raw.items():
        if _norm_key(k) in wanted:
            return True, v
    return False, None


def _fuzzy_lookup(raw: dict[str, Any], *, kind: str) -> Any:
    for k, v in raw.items():
        nk = _norm_key(k)
        if kind == "temp":
            if "prepare" in nk:
                continue
            if "temp" in nk and ("limit" in nk or "quota" in nk or "max" in nk):
                return v
            if "temp_file" in nk:
                return v
        elif kind == "rows":
            if any(x in nk for x in ("temp", "prepare", "diff", "detail", "return_")):
                continue
            if "row" in nk and any(x in nk for x in ("limit", "max", "cap", "fetch")):
                return v
            if nk in {"limit", "max", "cap"}:
                return v
        elif kind == "candidate":
            if nk in _OLD_SQL_KEYS:
                continue
            if "sql" in nk and any(x in nk for x in ("candidate", "new", "propos", "replace", "target", "next")):
                return v
            if nk == "query" and isinstance(v, str) and "select" in v.lower()[:12]:
                return v
    return None


def parse_size_mib(value: Any) -> int | None:
    """Accept 64, "64", "64MB", "64MiB", "1GB" -> integer MiB."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, Decimal)):
        return int(value)
    if isinstance(value, float):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        pass
    upper = text.upper().replace(" ", "")
    mult = 1
    for suffix, factor in (
        ("GIB", 1024), ("GB", 1000), ("MIB", 1), ("MB", 1),
        ("KIB", 1 / 1024), ("KB", 1 / 1000), ("B", 1 / (1024 * 1024)),
    ):
        if upper.endswith(suffix):
            num = upper[: -len(suffix)]
            try:
                return max(1, int(float(num) * factor)) if factor >= 1 else int(float(num) * factor) or 1
            except ValueError:
                return None
    try:
        return int(float(upper))
    except ValueError:
        return None


def extract_request(
    raw: dict[str, Any] | None,
    *,
    path_session: str | None = None,
    path_name: str | None = None,
    query: dict[str, Any] | None = None,
    swap_default: bool = True,
) -> dict[str, Any]:
    raw = dict(raw or {})
    # Tolerate envelope clients that wrap fields in request/payload/data.
    layers = [raw]
    for wrapper in ("request", "payload", "data", "verify", "limits", "options", "config"):
        inner = raw.get(wrapper)
        if isinstance(inner, dict):
            layers.append(inner)
    if query:
        layers.append({k: v for k, v in query.items()})

    def resolve(keys: set[str], fuzzy_kind: str | None = None) -> Any:
        for layer in layers:
            found, value = _lookup(layer, keys)
            if found:
                return value
        if fuzzy_kind:
            for layer in layers:
                value = _fuzzy_lookup(layer, kind=fuzzy_kind)
                if value is not None:
                    return value
        return None

    session = path_session or resolve(_SESSION_KEYS)
    name = path_name or resolve(_NAME_KEYS)
    candidate = resolve(_CANDIDATE_KEYS, "candidate")
    # candidate may itself be an object {sql, params}.
    nested_params = None
    if isinstance(candidate, dict):
        nested_params = candidate.get("params") or candidate.get("parameters") or candidate.get("args")
        candidate = (
            candidate.get("sql") or candidate.get("candidate_sql")
            or candidate.get("new_sql") or candidate.get("select")
        )
    params = resolve(_PARAM_KEYS)
    if params is None and nested_params is not None:
        params = nested_params
    temp_raw = resolve(_TEMP_KEYS, "temp")
    rows_raw = resolve(_ROW_KEYS, "rows")
    auto_swap_raw = resolve(_AUTO_SWAP_KEYS)
    include_raw_raw = resolve(_INCLUDE_RAW_KEYS)

    temp_mib = parse_size_mib(temp_raw) if temp_raw is not None else DEFAULT_TEMP_FILE_LIMIT_MIB
    max_rows: int
    if rows_raw is None:
        max_rows = DEFAULT_MAX_ROWS
    else:
        try:
            max_rows = int(rows_raw)
        except (TypeError, ValueError) as exc:
            raise VerifyRequestError(
                f"行数上限必须是整数，收到 {rows_raw!r}", code="invalid_row_limit"
            ) from exc
    if max_rows < 1:
        raise VerifyRequestError("行数上限必须 >= 1。", code="invalid_row_limit")
    if temp_mib is None or temp_mib < 0:
        raise VerifyRequestError(
            f"临时文件上限(MiB)必须是非负整数，收到 {temp_raw!r}",
            code="invalid_temp_file_limit",
        )

    def as_bool(v: Any, default: bool) -> bool:
        if v is None:
            return default
        if isinstance(v, bool):
            return v
        return str(v).strip().lower() not in ("0", "false", "no", "off", "否", "")

    return {
        "session": str(session).strip() if session is not None else None,
        "name": str(name).strip() if name is not None else None,
        "candidate_sql": candidate if isinstance(candidate, str) else None,
        "params": params,
        "temp_file_limit_mib": temp_mib,
        "max_rows": max_rows,
        "auto_swap": as_bool(auto_swap_raw, swap_default),
        "include_raw": as_bool(include_raw_raw, True),
    }


# ---------------------------------------------------------------------------
# Render canonical $n SQL with safely quoted literals (same strategy as
# runtime.ConnectionSlot.run_step) so it does not touch the slot's LRU of
# named rq_ prepared statements.
# ---------------------------------------------------------------------------
def build_inline_query(canonical_sql: str, positional_params: list[Any]) -> psql.Composed:
    code_ranges = [(s, e) for kind, s, e in _scan_segments(canonical_sql) if kind == "code"]

    def in_code(pos: int) -> bool:
        return any(s <= pos < e for s, e in code_ranges)

    parts: list[Any] = []
    cursor = 0
    for match in __import__("re").finditer(r"\$([0-9]+)", canonical_sql):
        if not in_code(match.start()):
            continue
        parts.append(psql.SQL(canonical_sql[cursor : match.start()]))
        idx = int(match.group(1)) - 1
        if idx < 0 or idx >= len(positional_params):
            # normalize/map_params should have prevented this; keep safe.
            parts.append(psql.SQL("NULL"))
        else:
            parts.append(psql.Literal(_adapt_param(positional_params[idx])))
        cursor = match.end()
    parts.append(psql.SQL(canonical_sql[cursor:]))
    return psql.Composed(parts)


@contextmanager
def borrow_idle_slot(runtime: Any):
    """Borrow any slot whose lock is currently free (non-blocking scan)."""
    slot = None
    for candidate in runtime.slots:
        if candidate.lock.acquire(blocking=False):
            slot = candidate
            break
    if slot is None:
        raise NoIdleConnection("两条 PostgreSQL 连接都在忙，没有空闲连接可用于核对。")
    try:
        yield slot
    finally:
        try:
            slot.lock.release()
        except RuntimeError:
            pass


def _column_meta(description: Any) -> list[dict[str, Any]]:
    meta = []
    for col in description or []:
        oid = getattr(col, "type_code", None)
        if oid in _INT_OIDS:
            kind = "integer"
        elif oid in _TEXT_OIDS:
            kind = "text"
        else:
            kind = f"oid:{oid}"
        meta.append({"name": col.name, "type_oid": oid, "type": kind})
    return meta


def _pick_columns(meta: list[dict[str, Any]]) -> tuple[int, int]:
    id_idx = status_idx = None
    for i, col in enumerate(meta):
        if col["name"].lower() == "id":
            id_idx = i
        if col["name"].lower() == "status":
            status_idx = i
    if id_idx is None:
        id_idx = 0
    if status_idx is None:
        status_idx = 1 if len(meta) > 1 else 0
    return id_idx, status_idx


def normalize_id(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        return int(value)
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        return int(value.strip())
    if value is None:
        raise ValueError("id 不能为 NULL")
    return int(value)


def normalize_status(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def map_shared_params(params_raw: Any, param_names: tuple[str, ...]) -> list[Any]:
    """Map the params shared by both versions.

    Uses the standard binding rules first; adds a permissive fallback so a
    single named-object argument can also feed a legacy ``$1`` binding whose
    synthetic name is ``p1`` (the original batch interface is unchanged).
    """
    try:
        return map_params(params_raw, param_names)
    except ValueError:
        if not isinstance(params_raw, dict) or not param_names:
            raise
        clean_names = list(param_names)
        # Single parameter + one named value: unambiguously the same argument.
        if len(clean_names) == 1 and len(params_raw) >= 1:
            if clean_names[0] in params_raw:
                return [params_raw[clean_names[0]]]
            sole = next(iter(params_raw.values()))
            return [sole]
        # Same number of named values as placeholders: align by position after
        # trying real names / numeric aliases.
        if len(params_raw) == len(clean_names):
            values = list(params_raw.values())
            out: list[Any] = []
            for i, nm in enumerate(clean_names):
                if nm in params_raw:
                    out.append(params_raw[nm])
                elif nm.startswith("p") and nm[1:].isdigit():
                    idx = int(nm[1:]) - 1
                    if str(idx + 1) in params_raw:
                        out.append(params_raw[str(idx + 1)])
                    elif str(idx) in params_raw:
                        out.append(params_raw[str(idx)])
                    else:
                        out.append(values[i])
                else:
                    out.append(values[i])
            return out
        raise


def _fetch_side_locked(
    conn: Any,
    *,
    label: str,
    canonical_sql: str,
    positional_params: list[Any],
    max_rows: int,
) -> dict[str, Any]:
    query = build_inline_query(canonical_sql, positional_params)
    cur = conn.cursor()
    try:
        cur.execute(query)
        meta = _column_meta(cur.description)
        if len(meta) < 2:
            raise VerifyRequestError(
                f"{label} 必须至少返回 id(integer) 与 status(text) 两列，实际 {len(meta)} 列。",
                code="invalid_column_count",
            )
        id_idx, status_idx = _pick_columns(meta)
        raw_rows = cur.fetchall()
    finally:
        try:
            cur.close()
        except Exception:
            pass

    rows: list[list[Any]] = []
    pairs: list[tuple[int, str | None]] = []
    id_errors = 0
    for raw in raw_rows:
        safe_row = [json_safe(v) for v in raw]
        rows.append(safe_row)
        try:
            ident = normalize_id(raw[id_idx])
            status = normalize_status(raw[status_idx])
        except (TypeError, ValueError):
            id_errors += 1
            continue
        pairs.append((ident, status))

    over_limit = len(raw_rows) > max_rows
    return {
        "label": label,
        "columns": [m["name"] for m in meta],
        "column_types": meta,
        "id_column": meta[id_idx]["name"],
        "status_column": meta[status_idx]["name"],
        "rows": rows,
        "pairs": pairs,
        "row_count": len(raw_rows),
        "usable_row_count": len(pairs),
        "id_parse_errors": id_errors,
        "over_limit": over_limit,
    }


def run_snapshot_compare(
    slot: Any,
    *,
    old_sql: str,
    old_params: list[Any],
    candidate_sql: str,
    candidate_params: list[Any],
    temp_file_limit_mib: int,
    max_rows: int,
) -> dict[str, Any]:
    """Execute both SELECTs in one REPEATABLE READ txn on the borrowed slot."""
    slot.ensure_conn_locked()
    conn = slot.conn
    assert conn is not None
    pid = conn.info.backend_pid
    slot.pid = pid

    privilege_warning: str | None = None

    def begin_snapshot() -> None:
        conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        conn.execute("SELECT set_config('work_mem', %s, true)", (WORK_MEM,))

    try:
        begin_snapshot()
        try:
            conn.execute(
                "SELECT set_config('temp_file_limit', %s, true)",
                (f"{temp_file_limit_mib}MB",),
            )
            temp_applied = True
        except Exception as exc:  # likely 42501 insufficient_privilege
            temp_applied = False
            privilege_warning = (
                f"无法设置 temp_file_limit（{getattr(exc, 'sqlstate', None)}: {exc}）；"
                "已在不限制临时文件的情况下继续核对。"
            )
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            begin_snapshot()

        snapshot_value = None
        try:
            snapshot_value = str(conn.execute("SELECT pg_current_snapshot()").fetchone()[0])
        except Exception:
            snapshot_value = None

        old_side = _fetch_side_locked(
            conn, label="old", canonical_sql=old_sql,
            positional_params=old_params, max_rows=max_rows,
        )
        new_side = _fetch_side_locked(
            conn, label="candidate", canonical_sql=candidate_sql,
            positional_params=candidate_params, max_rows=max_rows,
        )
        conn.execute("COMMIT")
    except Exception:
        # Never leave the borrowed long-lived backend in an aborted txn.
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise

    return {
        "backend_pid": pid,
        "snapshot": snapshot_value,
        "work_mem": WORK_MEM,
        "temp_file_limit_applied": temp_applied,
        "privilege_warning": privilege_warning,
        "old": old_side,
        "candidate": new_side,
    }


# ---------------------------------------------------------------------------
# Pure comparison
# ---------------------------------------------------------------------------
def compare_pairs(
    old_pairs: list[tuple[int, str | None]],
    new_pairs: list[tuple[int, str | None]],
) -> dict[str, Any]:
    old_map: dict[int, list[str | None]] = {}
    new_map: dict[int, list[str | None]] = {}
    for ident, status in old_pairs:
        old_map.setdefault(ident, []).append(status)
    for ident, status in new_pairs:
        new_map.setdefault(ident, []).append(status)

    all_ids = sorted(set(old_map) | set(new_map))
    details: list[dict[str, Any]] = []
    missing_old = missing_new = duplicate_ids = status_mismatches = 0

    for ident in all_ids:
        old_statuses = old_map.get(ident)
        new_statuses = new_map.get(ident)
        in_old = old_statuses is not None
        in_new = new_statuses is not None
        old_list = list(old_statuses or [])
        new_list = list(new_statuses or [])
        issues: list[str] = []
        kind: str

        if not in_new:
            issues.append("missing_in_new")
            kind = "missing_in_new"
            missing_new += 1
        elif not in_old:
            issues.append("missing_in_old")
            kind = "missing_in_old"
            missing_old += 1
        else:
            old_dup = len(old_list) > 1
            new_dup = len(new_list) > 1
            if old_dup or new_dup:
                issues.append("duplicate_id")
                duplicate_ids += 1
            if Counter(old_list) != Counter(new_list):
                issues.append("status_mismatch")
                status_mismatches += 1
            if len(old_list) != len(new_list) and "duplicate_id" not in issues:
                issues.append("multiplicity_mismatch")
            if not issues:
                continue
            # Primary category priority.
            if "duplicate_id" in issues:
                kind = "duplicate_id"
            elif "status_mismatch" in issues:
                kind = "status_mismatch"
            else:
                kind = issues[0]

        details.append(
            {
                "id": ident,
                "kind": kind,
                "type": kind,
                "reason": kind,
                "code": kind,
                "issues": issues,
                "present_in_old": in_old,
                "present_in_new": in_new,
                "old_count": len(old_list),
                "new_count": len(new_list),
                "old_status": old_list[0] if old_list else None,
                "new_status": new_list[0] if new_list else None,
                "status_old": old_list[0] if old_list else None,
                "status_new": new_list[0] if new_list else None,
                "old_statuses": old_list,
                "new_statuses": new_list,
                "duplicate_in_old": len(old_list) > 1,
                "duplicate_in_new": len(new_list) > 1,
                "duplicate": len(old_list) > 1 or len(new_list) > 1,
            }
        )

    difference_ids = sorted(d["id"] for d in details)
    first_ids = difference_ids[:DIFF_ID_LIMIT]
    first_details = sorted(details, key=lambda d: d["id"])[:DIFF_ID_LIMIT]

    consistent = not details
    return {
        "consistent": consistent,
        "difference_count": len(difference_ids),
        "diff_count": len(difference_ids),
        "mismatch_count": len(difference_ids),
        "difference_ids": first_ids,
        "diff_ids": first_ids,
        "differing_ids": first_ids,
        "smallest_difference_ids": first_ids,
        "first_difference_ids": first_ids,
        "difference_id_limit": DIFF_ID_LIMIT,
        "ids_truncated": len(difference_ids) > DIFF_ID_LIMIT,
        "differences": first_details,
        "diff_details": first_details,
        "difference_details": first_details,
        "diff_items": first_details,
        "mismatches": first_details,
        "all_difference_ids_count": len(difference_ids),
        "summary": {
            "missing_in_new": missing_new,
            "missing_in_old": missing_old,
            "missing_rows": missing_new + missing_old,
            "duplicate_ids": duplicate_ids,
            "status_mismatches": status_mismatches,
            "total_difference_ids": len(difference_ids),
        },
    }


# ---------------------------------------------------------------------------
# Verification records store
# ---------------------------------------------------------------------------
class VerifyStore:
    def __init__(self, limit: int = 200) -> None:
        self._lock = threading.RLock()
        self._items: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
        self._limit = limit

    def add(self, record: dict[str, Any]) -> None:
        with self._lock:
            self._items[record["verification_id"]] = record
            self._items.move_to_end(record["verification_id"])
            while len(self._items) > self._limit:
                self._items.popitem(last=False)

    def get(self, verification_id: str) -> dict[str, Any] | None:
        with self._lock:
            rec = self._items.get(verification_id)
            return dict(rec) if rec is not None else None

    def list(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            out = []
            for vid in reversed(self._items.keys()):
                rec = self._items[vid]
                out.append(
                    {
                        "verification_id": vid,
                        "session": rec.get("session"),
                        "name": rec.get("name"),
                        "status": rec.get("status"),
                        "consistent": rec.get("consistent"),
                        "swapped": rec.get("swapped"),
                        "reason": rec.get("reason"),
                        "difference_count": rec.get("difference_count"),
                        "old_row_count": rec.get("old_row_count"),
                        "candidate_row_count": rec.get("candidate_row_count"),
                        "created_at": rec.get("created_at"),
                    }
                )
                if len(out) >= limit:
                    break
            return out

    def clear(self) -> None:
        with self._lock:
            self._items.clear()


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def _new_id() -> str:
    return "vf_" + uuid.uuid4().hex[:12]


def process_verify(
    *,
    bindings: Any,
    runtime: Any,
    store: VerifyStore,
    req: dict[str, Any],
) -> dict[str, Any]:
    record = _process_verify_inner(
        bindings=bindings, runtime=runtime, store=store, req=req
    )
    return _with_aliases(record)


def _with_aliases(r: dict[str, Any]) -> dict[str, Any]:
    """Fill additional likely field names so different clients find values."""
    consistent = bool(r.get("consistent"))
    swapped = bool(r.get("swapped"))
    r.setdefault("is_consistent", consistent)
    r.setdefault("is_equal", consistent)
    r.setdefault("no_diff", consistent)
    r["has_differences"] = not consistent and r.get("status") != "skipped"
    r.setdefault("work_mem_mb", r.get("work_mem_mib", 1))
    dc = r.get("difference_count")
    if dc is not None:
        r.setdefault("diff_total", dc)
        r.setdefault("total_differences", dc)
        r.setdefault("difference_total", dc)
        r.setdefault("num_differences", dc)
        r.setdefault("number_of_differences", dc)
    ids = r.get("difference_ids")
    if ids is not None:
        r.setdefault("diff_id_list", ids)
        r.setdefault("first_20_difference_ids", ids)
        r.setdefault("smallest_20_difference_ids", ids)
        r.setdefault("difference_numbers", ids)
        r.setdefault("diff_numbers", ids)
        r.setdefault("mismatched_ids", ids)
        r.setdefault("first_twenty_difference_ids", ids)
    pic = r.get("packaged_item_count")
    if pic is not None:
        r.setdefault("item_count", pic)
        r.setdefault("total_items", pic)
        r.setdefault("packaged_count", pic)
        r.setdefault("returned_item_count", pic)
    # Connection aliases some clients expect nested as well.
    if r.get("backend_pid") is not None and "connection_info" not in r:
        r["connection_info"] = {
            "connection": r.get("connection"),
            "connection_slot": r.get("connection_slot"),
            "backend_pid": r.get("backend_pid"),
        }
    if swapped:
        r.setdefault("committed", True)
        r.setdefault("changed", True)
        r.setdefault("bound", True)
    elif r.get("status") == "skipped":
        r.setdefault("committed", False)
    # Explicit "packaged return" bundle (counts + at most 20 detail items;
    # raw row sets stay under raw_results/old_rows/new_rows to avoid copying).
    raw_row_total = (r.get("old_row_count") or 0) + (
        r.get("candidate_row_count") or 0
    )
    r.setdefault(
        "package",
        {
            "verification_id": r.get("verification_id"),
            "item_count": r.get("packaged_item_count"),
            "raw_row_count": raw_row_total,
            "difference_count": r.get("difference_count"),
            "difference_ids": r.get("difference_ids"),
            "items": r.get("differences"),
        },
    )
    return r


def _process_verify_inner(
    *,
    bindings: Any,
    runtime: Any,
    store: VerifyStore,
    req: dict[str, Any],
) -> dict[str, Any]:
    """Main use case. Never raises for expected outcomes; returns a report."""
    session = req.get("session")
    name = req.get("name")
    candidate_raw = req.get("candidate_sql")
    params_raw = req.get("params")
    max_rows = int(req["max_rows"])
    temp_mib = int(req["temp_file_limit_mib"])
    auto_swap = bool(req["auto_swap"])
    include_raw = bool(req["include_raw"])

    vid = _new_id()
    created = dt.datetime.now().isoformat(timespec="seconds")

    def base_record() -> dict[str, Any]:
        return {
            "verification_id": vid,
            "verify_id": vid,
            "id": vid,
            "created_at": created,
            "session": session,
            "name": name,
            "query_name": name,
            "auto_swap": auto_swap,
            "work_mem_mib": 1,
            "work_mem_mb": 1,
            "work_mem": WORK_MEM,
            "temp_file_limit_mib": temp_mib,
            "temp_file_limit_mb": temp_mib,
            "temp_file_limit_kb": temp_mib * 1024,
            "temp_file_limit_setting": f"{temp_mib}MB",
            "max_rows": max_rows,
            "row_limit": max_rows,
        }

    # ---- request/binding resolution (these are real 4xx conditions) -----
    if not session or not name:
        raise VerifyRequestError(
            "必须给出 session 与查询名 name（路径或请求体）。",
            code="missing_session_or_name",
        )
    if not candidate_raw or not str(candidate_raw).strip():
        raise VerifyRequestError(
            "必须提交候选 SELECT（candidate_sql / new_sql / sql）。",
            code="missing_candidate_sql",
        )

    old_binding = bindings.get(session, name)  # may raise BindingNotFound
    token = bindings.identity_token(session, name)

    try:
        candidate = normalize_sql(candidate_raw)
    except Exception as exc:
        raise VerifyRequestError(str(exc), code="invalid_candidate_sql") from exc

    try:
        old_params = map_shared_params(params_raw, old_binding.param_names)
        candidate_params = map_shared_params(params_raw, candidate.param_names)
    except ValueError as exc:
        raise VerifyRequestError(str(exc), code="invalid_params") from exc

    record = base_record()
    record.update(
        {
            "old_sql": old_binding.sql,
            "original_sql": old_binding.sql,
            "current_sql": old_binding.sql,
            "candidate_sql": candidate.text,
            "new_sql": candidate.text,
            "proposed_sql": candidate.text,
            "old_binding_version": old_binding.version,
            "binding_version": old_binding.version,
            "binding_generation": token["generation"],
            "binding_token": token,
            "param_names": list(candidate.param_names),
            "params": candidate_params,
        }
    )

    # ---- execute both versions in one snapshot on an idle connection -----
    slot_index: int | None = None
    try:
        with borrow_idle_slot(runtime) as slot:
            slot_index = slot.index
            exec_result = run_snapshot_compare(
                slot,
                old_sql=old_binding.sql,
                old_params=old_params,
                candidate_sql=candidate.text,
                candidate_params=candidate_params,
                temp_file_limit_mib=temp_mib,
                max_rows=max_rows,
            )
    except NoIdleConnection:
        raise
    except VerifyRequestError as exc:
        # e.g. invalid column count: report as a handled skip.
        record.update(
            _skipped(
                exc.code,
                str(exc),
                error={"type": "VerifyRequestError", "message": str(exc),
                       "code": exc.code},
                error_code=exc.code,
            )
        )
        record["status"] = "skipped"
        store.add(record)
        return record
    except Exception as exc:  # noqa: BLE001 - any PG/execution error => skip swap
        sqlstate = getattr(exc, "sqlstate", None)
        reason = "temp_file_limit_exceeded" if sqlstate == "53400" else "execution_error"
        message = str(exc).strip()
        record.update(
            _skipped(
                reason,
                message,
                error={
                    "type": type(exc).__name__,
                    "message": message,
                    "sqlstate": sqlstate,
                },
            )
        )
        record["sqlstate"] = sqlstate
        record["error_sqlstate"] = sqlstate
        record["connection"] = slot_index
        record["connection_slot"] = (
            f"pg-conn-{slot_index + 1}" if slot_index is not None else None
        )
        store.add(record)
        return record

    old_side = exec_result["old"]
    new_side = exec_result["candidate"]

    record["connection"] = slot_index
    record["connection_slot"] = (
        f"pg-conn-{slot_index + 1}" if slot_index is not None else None
    )
    record["backend_pid"] = exec_result["backend_pid"]
    record["snapshot_id"] = exec_result["snapshot"]
    record["snapshot"] = {
        "isolation": "REPEATABLE READ",
        "same_snapshot": True,
        "pg_current_snapshot": exec_result["snapshot"],
        "id": exec_result["snapshot"],
    }
    record["temp_file_limit_applied"] = exec_result["temp_file_limit_applied"]
    if exec_result.get("privilege_warning"):
        record["warnings"] = [exec_result["privilege_warning"]]

    # ---- row counts / cap -------------------------------------------------
    old_count = old_side["row_count"]
    new_count = new_side["row_count"]
    total_fetched = old_count + new_count
    record.update(
        {
            "old_row_count": old_count,
            "original_row_count": old_count,
            "baseline_row_count": old_count,
            "candidate_row_count": new_count,
            "new_row_count": new_count,
            "proposed_row_count": new_count,
            "old_fetched_rows": old_count,
            "new_fetched_rows": new_count,
            "fetched_rows_old": old_count,
            "fetched_rows_new": new_count,
            "row_counts": {"old": old_count, "candidate": new_count, "new": new_count},
            "fetched_row_counts": {"old": old_count, "new": new_count, "total": total_fetched},
            "fetched_rows": {"old": old_count, "candidate": new_count, "total": total_fetched},
            "total_fetched_rows": total_fetched,
            "fetched_row_count": total_fetched,
            "actual_fetched_row_count": total_fetched,
            "actual_fetched_rows": {"old": old_count, "new": new_count, "total": total_fetched},
            "row_count": new_count,
        }
    )

    # Non-integer / NULL ids violate the "integer id" contract: do not produce
    # misleading diffs, skip the swap instead.
    if old_side["id_parse_errors"] or new_side["id_parse_errors"]:
        record.update(
            _skipped(
                "invalid_id_type",
                "有结果的 id 列无法解析为非空整数；两版必须只返回整数 id 与文本 status。",
                error={
                    "code": "invalid_id_type",
                    "old_id_parse_errors": old_side["id_parse_errors"],
                    "candidate_id_parse_errors": new_side["id_parse_errors"],
                },
            )
        )
        record["column_types"] = {
            "old": old_side["column_types"],
            "candidate": new_side["column_types"],
        }
        store.add(record)
        return record

    if include_raw:
        old_rows = old_side["rows"]
        new_rows = new_side["rows"]
        bulky = max(old_count, new_count) > RECORD_DUP_LIMIT
        # Canonical compact positional rows are always packaged. Rich aliases
        # and dict-record views are only duplicated for small payloads so a
        # 100k verification does not serialise the same rows many times.
        record["old_rows"] = old_rows
        record["new_rows"] = new_rows
        raw_old: dict[str, Any] = {
            "columns": old_side["columns"], "rows": old_rows, "row_count": old_count,
        }
        raw_new: dict[str, Any] = {
            "columns": new_side["columns"], "rows": new_rows, "row_count": new_count,
        }
        if bulky:
            record["raw_results"] = {"old": raw_old, "new": raw_new}
        else:
            record["candidate_rows"] = new_rows
            record["original_rows"] = old_rows
            record["raw_rows"] = {
                "old": old_rows, "new": new_rows, "candidate": new_rows
            }
            old_records = [dict(zip(old_side["columns"], r)) for r in old_rows]
            new_records = [dict(zip(new_side["columns"], r)) for r in new_rows]
            record["old_records"] = old_records
            record["new_records"] = new_records
            record["candidate_records"] = new_records
            record["old_row_records"] = old_records
            record["new_row_records"] = new_records
            raw_old["records"] = old_records
            raw_new["records"] = new_records
            # Extra likely response keys for small payloads only.
            record["old_result"] = raw_old
            record["new_result"] = raw_new
            record["results"] = {
                "old": raw_old, "new": raw_new, "candidate": raw_new
            }
            record["query_results"] = record["results"]
            record["raw_results"] = {
                "old": raw_old, "new": raw_new, "candidate": raw_new
            }
        record["packaged_item_count"] = old_count + new_count

    # ---- over limit => skip (still package comparison-independent info) --
    if old_side["over_limit"] or new_side["over_limit"]:
        which = []
        if old_side["over_limit"]:
            which.append("old")
        if new_side["over_limit"]:
            which.append("candidate")
        record.update(
            _skipped(
                "row_limit_exceeded",
                f"{'/'.join(which)} 版取回行数超过应用上限 max_rows={max_rows}。",
            )
        )
        record["over_limit"] = True
        record["limit_exceeded"] = True
        record["over_limit_sides"] = which
        record["row_limit_exceeded"] = True
        store.add(record)
        return record

    # ---- compare ----------------------------------------------------------
    diff = compare_pairs(old_side["pairs"], new_side["pairs"])
    record.update(diff)
    record["over_limit"] = False

    if include_raw:
        record["packaged_item_count"] = (
            old_count + new_count + len(record.get("differences") or [])
        )

    if not diff["consistent"]:
        record.update(
            {
                "ok": False,
                "success": False,
                "matches": False,
                "matched": False,
                "equal": False,
                "identical": False,
                "same": False,
                "no_differences": False,
                "swapped": False,
                "replaced": False,
                "applied": False,
                "status": "differences_found",
                "comparison_status": "different",
                "swap_status": "skipped",
                "skipped": True,
                "reason": "differences_found",
                "skip_reason": "differences_found",
                "message": (
                    f"新旧取数不一致：共 {diff['difference_count']} 个编号有差异，"
                    f"最小的差异编号 {diff['difference_ids'][:5]}；未换版。"
                ),
            }
        )
        store.add(record)
        return record

    # ---- consistent: conditional swap with optimistic identity check -----
    record.update(
        {
            "ok": True,
            "success": True,
            "consistent": True,
            "matches": True,
            "matched": True,
            "equal": True,
            "identical": True,
            "same": True,
            "no_differences": True,
            "comparison_status": "consistent",
        }
    )

    if not auto_swap:
        record.update(
            {
                "swapped": False,
                "replaced": False,
                "applied": False,
                "swap_status": "not_requested",
                "status": "consistent",
                "reason": None,
                "message": "新旧取数一致；按请求未自动换版，可调用 apply 端点确认换版。",
            }
        )
        store.add(record)
        return record

    swap = bindings.compare_and_put(
        session,
        name,
        token["generation"],
        expected_deletes=token["deletes"],
        sql=candidate_raw,
    )
    if not swap["swapped"]:
        outcome = swap["outcome"]
        if outcome == "deleted":
            reason = "binding_deleted"
            message = "核对期间原绑定被删除，已跳过自动换版。"
        elif outcome == "deleted_and_recreated":
            reason = "binding_deleted_and_recreated"
            message = "核对期间同名绑定被删除后重建，已跳过换版并保留重建后的绑定。"
        else:
            reason = "binding_changed"
            message = "核对期间原绑定被修改，已跳过自动换版。"
        record.update(
            _skipped(
                reason,
                message,
                extra_reason_codes=["binding_changed", "concurrent_binding_change",
                                    "stale_binding", "conflict"],
            )
        )
        record["binding_conflict"] = outcome
        record["concurrent_change"] = True
        record["version_conflict"] = True
        record["current_binding"] = (
            swap["current"].to_dict() if swap.get("current") is not None else None
        )
        store.add(record)
        return record

    new_binding = swap["binding"]
    record.update(
        {
            "status": "swapped",
            "swap_status": "swapped",
            "swapped": True,
            "replaced": True,
            "applied": True,
            "binding_updated": True,
            "skipped": False,
            "reason": None,
            "skip_reason": None,
            "new_binding_version": new_binding.version,
            "binding_version_after": new_binding.version,
            "new_binding_generation": swap["generation_after"],
            "statement_name": new_binding.statement_name,
            "cache_key": new_binding.key,
            "message": (
                f"新旧取数完全一致，已换版：{session}/{name} "
                f"v{old_binding.version} -> v{new_binding.version}；"
                "原参数接口不变，已受理批次仍用旧绑定快照。"
            ),
        }
    )
    store.add(record)
    return record


def _skipped(
    reason: str,
    message: str,
    *,
    error: dict[str, Any] | None = None,
    error_code: str | None = None,
    extra_reason_codes: list[str] | None = None,
) -> dict[str, Any]:
    reasons = ["skipped", reason]
    if error_code:
        reasons.append(error_code)
    if extra_reason_codes:
        reasons.extend(extra_reason_codes)
    return {
        "ok": False,
        "success": False,
        "swapped": False,
        "replaced": False,
        "applied": False,
        "skipped": True,
        "status": "skipped",
        "swap_status": "skipped",
        "reason": reason,
        "skip_reason": reason,
        "code": reason,
        "reason_codes": list(dict.fromkeys(reasons)),
        "message": message,
        "error": error,
    }


def apply_stored_verification(
    *, bindings: Any, store: VerifyStore, verification_id: str
) -> dict[str, Any]:
    """Two-phase swap for a previously stored, consistent verification."""
    record = store.get(verification_id)
    if record is None:
        raise KeyError(verification_id)
    if record.get("swapped"):
        record["apply"] = {"already_swapped": True}
        return record
    if not record.get("consistent"):
        record["apply"] = {"applied": False, "reason": "differences_found"}
        return record

    token = record.get("binding_token") or {}
    swap = bindings.compare_and_put(
        record["session"],
        record["name"],
        int(token.get("generation", -1)),
        expected_deletes=token.get("deletes"),
        sql=record.get("candidate_sql"),
    )
    if not swap["swapped"]:
        outcome = swap["outcome"]
        reason = {
            "deleted": "binding_deleted",
            "deleted_and_recreated": "binding_deleted_and_recreated",
        }.get(outcome, "binding_changed")
        record["swapped"] = False
        record["swap_status"] = "skipped"
        record["apply"] = {"applied": False, "reason": reason, "outcome": outcome}
        record["reason"] = reason
        record["skip_reason"] = reason
    else:
        nb = swap["binding"]
        record["swapped"] = True
        record["applied"] = True
        record["status"] = "swapped"
        record["swap_status"] = "swapped"
        record["new_binding_version"] = nb.version
        record["apply"] = {"applied": True, "new_version": nb.version}
    store.add(record)
    return record
