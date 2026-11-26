"""FastAPI entrypoint.

Run locally:
    DATABASE_URL=postgresql://... uvicorn app.main:app --reload
Or:
    docker compose up --build
"""
from __future__ import annotations

import contextlib

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from . import __version__, state
from .routes import router
from .verify_routes import router as verify_router

TAGS_METADATA = [
    {"name": "drill", "description": "绑定管理、双连接排程、预备语句缓存与批次取数"},
    {"name": "verify-sql", "description": "换 SQL 前在同一快照核对新旧 id/status，一致才换版"},
]


@contextlib.asynccontextmanager
async def lifespan(_app: FastAPI):
    inserted = state.runtime.startup(
        seed=state.settings.seed_deployments,
        timeout_seconds=state.settings.db_connect_timeout_seconds,
    )
    _app.state.seed_inserted = inserted
    yield
    state.runtime.shutdown()


app = FastAPI(
    title="报表高峰 · 双 PostgreSQL 连接取数演练",
    version=__version__,
    lifespan=lifespan,
    description=(
        "只有两条 PostgreSQL 连接可用；每条连接最多保留两条真实 PREPARE 语句。\n\n"
        "1. 在 **Swagger `/docs`** 中按会话新增/改绑查询名，SQL 可选内置的"
        "按服务 / 版本 / 日期筛选模板，也可写自己的参数化 SQL。\n"
        "2. `POST /api/batches` 提交至多 8 个查询（会话、查询名、参数、到达拍、"
        "占用拍数、最迟完成拍）和全批 PREPARE 上限。\n"
        "3. 排程器精确枚举双连接分配与顺序：限额内可准时完成才受理，否则拒绝且"
        "缓存照旧。\n"
        "4. 受理后按排程在真实 PostgreSQL 上 PREPARE/EXECUTE，返回后端 PID、"
        "起止拍、实际 PREPARE 次数与结果。\n\n"
        "同名查询在不同会话互不影响；每批冻结受理时的绑定；预备语句跨批保留。"
    ),
    openapi_tags=TAGS_METADATA,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    from fastapi.openapi.utils import get_openapi

    from .models import VerifySqlIn

    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
        tags=TAGS_METADATA,
    )
    components = schema.setdefault("components", {}).setdefault("schemas", {})
    components["VerifySqlIn"] = VerifySqlIn.model_json_schema(
        ref_template="#/components/schemas/{model}"
    )
    app.openapi_schema = schema
    return schema


app.openapi = _custom_openapi

# Canonical JSON API under /api (shown in /docs) ...
app.include_router(router, prefix="/api")
app.include_router(verify_router, prefix="/api")
# ... plus root-level aliases (/batches, /sessions, /cache ...) for clients
# that prefer unversioned paths. Kept out of the schema to avoid duplicate docs.
app.include_router(router, include_in_schema=False)
app.include_router(verify_router, include_in_schema=False)


@app.get("/", tags=["meta"], summary="服务入口与导览")
def root() -> dict:
    return {
        "service": "pg-two-connection-report-drill",
        "version": __version__,
        "docs": "/docs",
        "openapi": "/openapi.json",
        "start_here": {
            "templates": "GET /api/templates",
            "add_or_rebind": "PUT /api/sessions/{session}/bindings/{name}",
            "verify_sql_before_swap": "POST /api/sessions/{session}/bindings/{name}/verify",
            "submit_batch": "POST /api/batches",
            "plan_only": "POST /api/batches/plan",
            "cache": "GET /api/cache",
            "connections": "GET /api/connections",
            "deployments": "GET /api/deployments",
            "batches": "GET /api/batches",
        },
        "constraints": {
            "postgresql_connections": state.settings.connection_count,
            "prepared_statements_per_connection": state.settings.prepared_per_connection,
            "max_queries_per_batch": state.settings.max_batch_queries,
            "one_batch_at_a_time": True,
        },
    }


@app.get("/health", tags=["meta"], summary="健康检查（含两条连接的 PID）")
def health() -> dict:
    conns = state.runtime.connections_state()
    ok = all(c.get("connected") for c in conns)
    return {
        "status": "ok" if ok else "degraded",
        "seed_inserted": getattr(app.state, "seed_inserted", None),
        "connections": [
            {
                "slot": c["connection_slot"],
                "connected": c["connected"],
                "backend_pid": c["backend_pid"],
                "prepared_count": c["prepared_count"],
            }
            for c in conns
        ],
    }
