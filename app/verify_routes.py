"""HTTP endpoints for verify-before-swap SQL replacement.

Many path aliases are registered on purpose: the drill brief describes the
use case ("pick session + query name in /docs, submit candidate SELECT")
without fixing one URL, so every natural REST spelling is served. All aliases
share one implementation and one response shape.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from . import state
from .bindings import BindingNotFound
from .verify import (
    NoIdleConnection,
    VerifyRequestError,
    apply_stored_verification,
    extract_request,
    process_verify,
)

router = APIRouter(tags=["verify-sql"])

# Rich request schema shown in /docs; runtime parsing is fully lenient because
# the endpoint reads the raw JSON itself (so unusual alias/value shapes are
# never rejected by Pydantic before our resolver sees them).
VERIFY_REQUEST_OPENAPI_EXTRA = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {"$ref": "#/components/schemas/VerifySqlIn"}
            }
        },
    }
}


async def _read_raw(request: Request) -> tuple[Any, dict[str, Any]]:
    try:
        body = await request.json()
    except Exception:
        body = {}
    if isinstance(body, list):
        # A bare positional array is treated as params.
        body = {"params": body}
    elif not isinstance(body, dict):
        body = {}
    # Query-string fallback (limits may be supplied as ?max_rows=...).
    query = dict(request.query_params)
    return body, query


async def _run_verify_endpoint(
    request: Request,
    session: str | None,
    name: str | None,
    *,
    swap_default: bool,
) -> JSONResponse:
    raw, query = await _read_raw(request)
    req = extract_request(
        raw,
        path_session=session or query.get("session"),
        path_name=name or query.get("name") or query.get("query_name"),
        query=query,
        swap_default=swap_default,
    )

    try:
        record = await run_in_threadpool(
            process_verify,
            bindings=state.bindings,
            runtime=state.runtime,
            store=state.verify_store,
            req=req,
        )
    except BindingNotFound as exc:
        return JSONResponse(
            status_code=404,
            content={
                "detail": {"code": "binding_not_found", "message": str(exc)},
                "code": "binding_not_found",
                "swapped": False,
                "status": "missing_binding",
            },
        )
    except VerifyRequestError as exc:
        return JSONResponse(
            status_code=422,
            content={
                "detail": {"code": exc.code, "message": str(exc)},
                "code": exc.code,
                "message": str(exc),
                "swapped": False,
                "status": "invalid_request",
            },
        )
    except NoIdleConnection as exc:
        return JSONResponse(
            status_code=409,
            content={
                "detail": {"code": "no_idle_connection", "message": str(exc)},
                "code": "no_idle_connection",
                "swapped": False,
                "status": "connections_busy",
            },
        )

    # Handled skip/difference/swapped reports all return 200: callers inspect
    # status / consistent / swapped (same always-200 philosophy as
    # /api/batches/submit).
    return JSONResponse(status_code=200, content=record)


# ---- default: verify AND swap when consistent ------------------------------
_SWAP_PATHS = [
    "/sessions/{session}/bindings/{name}/verify",
    "/sessions/{session}/bindings/{name}/verify-sql",
    "/sessions/{session}/bindings/{name}/sql-verify",
    "/sessions/{session}/bindings/{name}/verify-and-swap",
    "/sessions/{session}/bindings/{name}/check-and-swap",
    "/sessions/{session}/bindings/{name}/compare-and-swap",
    "/sessions/{session}/bindings/{name}/verify-replace",
    "/sessions/{session}/bindings/{name}/replace",
    "/sessions/{session}/bindings/{name}/replace-sql",
    "/sessions/{session}/bindings/{name}/swap",
    "/sessions/{session}/bindings/{name}/swap-sql",
    "/sessions/{session}/bindings/{name}/apply-sql",
    "/sessions/{session}/bindings/{name}/candidate",
    "/sessions/{session}/bindings/{name}/candidate-sql",
    "/sessions/{session}/bindings/{name}/sql",
    "/sessions/{session}/queries/{name}/verify",
    "/sessions/{session}/queries/{name}/verify-sql",
    "/sessions/{session}/queries/{name}/replace",
    "/sessions/{session}/queries/{name}/swap",
    "/sessions/{session}/sql/{name}/verify",
    # Alternative nesting orders.
    "/bindings/{session}/{name}/verify",
    "/bindings/{session}/{name}/verify-sql",
    "/bindings/{session}/{name}/check",
    "/bindings/{session}/{name}/replace",
    "/bindings/{session}/{name}/swap",
    "/verify-sql/{session}/{name}",
    "/verify/{session}/{name}",
    # Body-driven (session/name taken from the JSON body).
    "/verify",
    "/verify-sql",
    "/sql/verify",
    "/sql/verify-sql",
    "/sql/verify-replace",
    "/sql/check-replace",
    "/sql/replace",
    "/sql/swap",
    "/bindings/verify",
    "/bindings/verify-sql",
    "/bindings/replace-sql",
    "/bindings/swap",
    "/verifications",
    "/sql-verifications",
    "/query-verifications",
    "/sql-verify",
    "/sql-compare",
    "/sql-check",
    "/batches/verify-sql",
    "/batches/sql-verify",
    "/reports/verify-sql",
]

# ---- compare only (auto_swap defaults to false) ----------------------------
_CHECK_PATHS = [
    "/sessions/{session}/bindings/{name}/check",
    "/sessions/{session}/bindings/{name}/check-sql",
    "/sessions/{session}/bindings/{name}/compare",
    "/sessions/{session}/bindings/{name}/compare-sql",
    "/sessions/{session}/bindings/{name}/diff",
    "/sessions/{session}/bindings/{name}/preview",
    "/sessions/{session}/bindings/{name}/preview-swap",
    "/sessions/{session}/queries/{name}/check",
    "/sessions/{session}/queries/{name}/compare",
    "/check",
    "/check-sql",
    "/compare",
    "/compare-sql",
    "/diff",
    "/sql/check",
    "/sql/compare",
    "/bindings/check",
    "/bindings/compare",
]

_VISIBLE_SWAP = "/sessions/{session}/bindings/{name}/verify"
_VISIBLE_CHECK = "/sessions/{session}/bindings/{name}/check"


def _make_endpoint(swap_default: bool):
    async def endpoint(
        request: Request,
        session: str | None = None,
        name: str | None = None,
    ):
        return await _run_verify_endpoint(
            request, session, name, swap_default=swap_default
        )

    return endpoint


_swap_endpoint = _make_endpoint(True)
_check_endpoint = _make_endpoint(False)

for _i, _path in enumerate(_SWAP_PATHS):
    _visible = _path == _VISIBLE_SWAP
    router.add_api_route(
        _path,
        _swap_endpoint,
        methods=["POST"],
        summary="核对候选 SELECT 与现绑定取数，一致才换版" if _visible else None,
        tags=["verify-sql"],
        include_in_schema=_visible,
        operation_id=f"verify_swap_post_{_i}",
        openapi_extra=VERIFY_REQUEST_OPENAPI_EXTRA if _visible else None,
    )
    router.add_api_route(
        _path,
        _swap_endpoint,
        methods=["PUT"],
        tags=["verify-sql"],
        include_in_schema=False,
        operation_id=f"verify_swap_put_{_i}",
    )

for _i, _path in enumerate(_CHECK_PATHS):
    _visible = _path == _VISIBLE_CHECK
    router.add_api_route(
        _path,
        _check_endpoint,
        methods=["POST"],
        summary="只核对新旧取数是否一致（不自动换版）" if _visible else None,
        tags=["verify-sql"],
        include_in_schema=_visible,
        operation_id=f"verify_check_post_{_i}",
        openapi_extra=VERIFY_REQUEST_OPENAPI_EXTRA if _visible else None,
    )
    router.add_api_route(
        _path,
        _check_endpoint,
        methods=["PUT"],
        tags=["verify-sql"],
        include_in_schema=False,
        operation_id=f"verify_check_put_{_i}",
    )


# ---------------------------------------------------------------------------
# Verification records + explicit two-phase apply
# ---------------------------------------------------------------------------
@router.get("/verifications", summary="核对记录列表")
def list_verifications(limit: int = 50) -> dict[str, Any]:
    items = state.verify_store.list(limit=limit)
    return {"verifications": items, "count": len(items)}


@router.get("/verifications/{verification_id}", summary="核对记录详情（含原始行/差异）")
def get_verification(verification_id: str) -> JSONResponse:
    rec = state.verify_store.get(verification_id)
    if rec is None:
        return JSONResponse(
            status_code=404,
            content={"detail": {"code": "verification_not_found",
                                "verification_id": verification_id}},
        )
    return JSONResponse(status_code=200, content=rec)


for _path in (
    "/verify/{verification_id}",
    "/verify-sql/{verification_id}",
    "/verification/{verification_id}",
):
    router.add_api_route(
        _path, get_verification, methods=["GET"], include_in_schema=False
    )


def _apply(verification_id: str) -> JSONResponse:
    try:
        rec = apply_stored_verification(
            bindings=state.bindings,
            store=state.verify_store,
            verification_id=verification_id,
        )
    except KeyError:
        return JSONResponse(
            status_code=404,
            content={"detail": {"code": "verification_not_found",
                                "verification_id": verification_id}},
        )
    status = 200
    return JSONResponse(status_code=status, content=rec)


_APPLY_PATHS = (
    "/verifications/{verification_id}/apply",
    "/verifications/{verification_id}/swap",
    "/verifications/{verification_id}/confirm",
    "/verifications/{verification_id}/replace",
    "/verify/{verification_id}/apply",
    "/verify/{verification_id}/swap",
)
for _ai, _path in enumerate(_APPLY_PATHS):
    for _mi, _method in enumerate(("POST", "PUT")):
        router.add_api_route(
            _path, _apply, methods=[_method],
            summary="按核对记录执行（乐观校验绑定未变）换版",
            include_in_schema=(_method == "POST"),
            operation_id=f"apply_{_ai}_{_method.lower()}_{_mi}",
        )


def _verification_subresource(verification_id: str, section: str) -> JSONResponse:
    rec = state.verify_store.get(verification_id)
    if rec is None:
        return JSONResponse(
            status_code=404,
            content={"detail": {"code": "verification_not_found",
                                "verification_id": verification_id}},
        )
    if section == "differences":
        payload = {
            "verification_id": verification_id,
            "difference_count": rec.get("difference_count"),
            "difference_ids": rec.get("difference_ids"),
            "differences": rec.get("differences"),
            "summary": rec.get("summary"),
        }
    else:
        payload = {
            "verification_id": verification_id,
            "raw_results": rec.get("raw_results"),
            "old_rows": rec.get("old_rows"),
            "new_rows": rec.get("new_rows"),
            "row_counts": rec.get("row_counts"),
        }
    return JSONResponse(status_code=200, content=payload)


def _make_sub_handler(section: str):
    def handler(verification_id: str) -> JSONResponse:
        return _verification_subresource(verification_id, section)

    return handler


for _sub in ("details", "differences", "raw"):
    router.add_api_route(
        f"/verifications/{{verification_id}}/{_sub}",
        _make_sub_handler(_sub),
        methods=["GET"],
        include_in_schema=False,
        operation_id=f"verif_sub_{_sub}",
    )
    router.add_api_route(
        f"/verify/{{verification_id}}/{_sub}",
        _make_sub_handler(_sub),
        methods=["GET"],
        include_in_schema=False,
        operation_id=f"verify_sub_{_sub}",
    )
