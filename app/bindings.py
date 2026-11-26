"""Per-session named query bindings.

Names are scoped by session: the same query name in two sessions identifies
two independent bindings. The store is thread-safe; batches receive an
immutable snapshot at acceptance time.
"""
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any

from .sqlnorm import CanonicalSql, SqlValidationError, normalize_sql
from .templates import TEMPLATES


class BindingError(KeyError):
    code = "binding_error"


class BindingNotFound(BindingError):
    code = "binding_not_found"


class BindingValidationError(ValueError):
    code = "binding_validation_error"


@dataclass(frozen=True)
class Binding:
    session: str
    name: str
    sql: str                       # canonical $n SQL
    key: str
    statement_name: str
    param_names: tuple[str, ...]
    template: str | None
    title: str | None
    description: str | None
    version: int
    created_at: float
    updated_at: float
    rebind_count: int

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["param_names"] = list(self.param_names)
        return d


@dataclass
class _Versioned:
    binding: Binding


class BindingStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        # session -> name -> Binding
        self._sessions: dict[str, dict[str, Binding]] = {}
        # (session, name) -> mutation counters. A monotonic generation token is
        # essential for detecting "delete then recreate under the same name":
        # such a binding is version=1 again, so comparing versions alone is not
        # enough. Every put *and* every delete bumps the generation.
        self._revisions: dict[tuple[str, str], dict[str, int]] = {}
        # Coarse process-wide mutation counter (any binding write/delete).
        self.global_revision: int = 0

    def _rev_state_locked(self, session: str, name: str) -> dict[str, int]:
        key = (session, name)
        state = self._revisions.get(key)
        if state is None:
            state = {"generation": 0, "puts": 0, "deletes": 0}
            self._revisions[key] = state
        return state

    def identity_token(self, session: str, name: str) -> dict[str, Any]:
        """Opaque optimistic-concurrency token for one (session, name)."""
        session = _clean(session, "session")
        name = _clean(name, "name")
        with self._lock:
            st = self._rev_state_locked(session, name)
            bucket = self._sessions.get(session)
            current = bucket.get(name) if bucket else None
            return {
                "session": session,
                "name": name,
                "generation": st["generation"],
                "puts": st["puts"],
                "deletes": st["deletes"],
                "global_revision": self.global_revision,
                "version": current.version if current else None,
                "exists": current is not None,
                "updated_at": current.updated_at if current else None,
                "created_at": current.created_at if current else None,
                "key": current.key if current else None,
            }

    def compare_and_put(
        self,
        session: str,
        name: str,
        expected_generation: int,
        *,
        expected_deletes: int | None = None,
        sql: str | None = None,
        template: str | None = None,
        title: str | None = None,
        description: str | None = None,
    ) -> dict[str, Any]:
        """Optimistic swap: put only if the binding was not touched since capture.

        Returns a dict with ``outcome`` in ``swapped`` / ``modified`` /
        ``deleted`` / ``deleted_and_recreated``. When the outcome is not
        ``swapped`` the current binding (possibly the recreated one) is left
        exactly as-is and returned under ``current``.
        """
        session = _clean(session, "session")
        name = _clean(name, "name")
        with self._lock:
            st = self._rev_state_locked(session, name)
            generation_now = st["generation"]
            deletes_now = st["deletes"]
            bucket = self._sessions.get(session)
            current = bucket.get(name) if bucket else None

            unchanged = generation_now == expected_generation
            if expected_deletes is not None and deletes_now != expected_deletes:
                unchanged = False

            if not unchanged:
                if current is None:
                    outcome = "deleted"
                elif expected_deletes is not None and deletes_now > expected_deletes:
                    outcome = "deleted_and_recreated"
                else:
                    outcome = "modified"
                return {
                    "outcome": outcome,
                    "swapped": False,
                    "current": current,
                    "generation_before": expected_generation,
                    "generation_after": generation_now,
                    "deletes_before": expected_deletes,
                    "deletes_after": deletes_now,
                }

            new_binding, old_binding = self.put(
                session,
                name,
                sql=sql,
                template=template,
                title=title,
                description=description,
            )
            st = self._rev_state_locked(session, name)
            return {
                "outcome": "swapped",
                "swapped": True,
                "binding": new_binding,
                "old": old_binding,
                "generation_after": st["generation"],
            }

    # ---- writes -------------------------------------------------------
    def put(
        self,
        session: str,
        name: str,
        *,
        sql: str | None = None,
        template: str | None = None,
        title: str | None = None,
        description: str | None = None,
    ) -> tuple[Binding, Binding | None]:
        """Create or rebind. Returns (new_binding, old_binding_or_None)."""
        session = _clean(session, "session")
        name = _clean(name, "name")

        canonical: CanonicalSql
        tpl_id: str | None = None
        tpl_title: str | None = None
        tpl_desc: str | None = None
        template_param_names: tuple[str, ...] | None = None
        sql_explicit = sql is not None and bool(str(sql).strip())

        if template:
            if template not in TEMPLATES:
                raise BindingValidationError(
                    f"未知模板 {template!r}；可用模板: {sorted(TEMPLATES)}"
                )
            tpl = TEMPLATES[template]
            tpl_id = template
            tpl_title = tpl["title"]
            tpl_desc = tpl["description"]
            template_param_names = tuple(tpl.get("param_names", ()))
            # Template provides the default SQL; an explicit sql may override.
            if sql is None:
                sql = tpl["sql"]

        if sql is None or not str(sql).strip():
            raise BindingValidationError(
                "必须提供 sql，或通过 template 选择内置参数化 SQL。"
            )

        try:
            canonical = normalize_sql(sql)
        except SqlValidationError as exc:
            raise BindingValidationError(str(exc)) from exc

        # Template SQL carries meaningful parameter names; otherwise native
        # $n placeholders get synthetic p1..pN names (lists still work).
        param_names = canonical.param_names
        if (
            not sql_explicit
            and template_param_names
            and len(template_param_names) == canonical.param_count
        ):
            param_names = template_param_names

        now = time.time()
        with self._lock:
            bucket = self._sessions.setdefault(session, {})
            old = bucket.get(name)
            binding = Binding(
                session=session,
                name=name,
                sql=canonical.text,
                key=canonical.key,
                statement_name=canonical.statement_name,
                param_names=param_names,
                template=tpl_id or old.template if old else tpl_id,
                title=title or tpl_title or (old.title if old else None),
                description=description
                or tpl_desc
                or (old.description if old else None),
                version=(old.version + 1) if old else 1,
                created_at=old.created_at if old else now,
                updated_at=now,
                rebind_count=(old.rebind_count + 1) if old else 0,
            )
            bucket[name] = binding
            st = self._rev_state_locked(session, name)
            st["generation"] += 1
            st["puts"] += 1
            self.global_revision += 1
            return binding, old

    def delete(self, session: str, name: str) -> Binding:
        session = _clean(session, "session")
        name = _clean(name, "name")
        with self._lock:
            bucket = self._sessions.get(session)
            if not bucket or name not in bucket:
                raise BindingNotFound(f"会话 {session!r} 中没有查询名 {name!r}。")
            removed = bucket.pop(name)
            st = self._rev_state_locked(session, name)
            st["generation"] += 1
            st["deletes"] += 1
            self.global_revision += 1
            return removed

    # ---- reads --------------------------------------------------------
    def get(self, session: str, name: str) -> Binding:
        with self._lock:
            bucket = self._sessions.get(session)
            if not bucket or name not in bucket:
                known = sorted(bucket) if bucket else []
                raise BindingNotFound(
                    f"会话 {session!r} 中没有查询名 {name!r}。"
                    + (f" 该会话已有: {known}" if known else " 该会话还没有任何绑定。")
                )
            return bucket[name]

    def get_many(self, refs: list[tuple[str, str]]) -> list[Binding]:
        """Resolve a whole batch against one consistent binding snapshot."""
        with self._lock:
            return [self.get(session, name) for session, name in refs]

    def find_many(self, refs: list[tuple[str, str]]) -> list["Binding | None"]:
        """Like :meth:`get_many` but preserves positions with None for missing."""
        with self._lock:
            out: list[Binding | None] = []
            for session, name in refs:
                bucket = self._sessions.get(session)
                out.append(bucket.get(name) if bucket else None)
            return out

    def list_session(self, session: str) -> list[Binding]:
        with self._lock:
            return list(self._sessions.get(_clean(session, "session"), {}).values())

    def list_all(self) -> dict[str, list[Binding]]:
        with self._lock:
            return {s: list(names.values()) for s, names in sorted(self._sessions.items())}

    def sessions(self) -> list[dict[str, Any]]:
        with self._lock:
            out = []
            for session, names in sorted(self._sessions.items()):
                out.append(
                    {
                        "session": session,
                        "query_names": sorted(names),
                        "binding_count": len(names),
                    }
                )
            return out

    def resolve_snapshot_item(self, session: str, name: str) -> Binding:
        return self.get(session, name)


def _clean(value: Any, field_name: str) -> str:
    if value is None:
        raise BindingValidationError(f"{field_name} 不能为空。")
    text = str(value).strip()
    if not text:
        raise BindingValidationError(f"{field_name} 不能为空字符串。")
    if len(text) > 128:
        raise BindingValidationError(f"{field_name} 最长 128 字符。")
    return text
