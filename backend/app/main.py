"""模拟写入后端。

每个后端实例：
  * 把收到的写入落到自己的 SQLite（/data/writes.db），持久化保存；
  * 支持慢回执：POST /writes?delay=秒，先落库再延迟回执，模拟"已送达、未回执"；
  * 支持健康故障注入：POST /control/fault on/off，故障期间 /healthz 503、写入 500；
  * /writes/<id> 供代理在切流后仍向"原送达节点"查询回执；
  * /records 列出本节点全部写入记录。
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

import aiosqlite
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

NODE_NAME = os.environ.get("NODE_NAME", "backend")
DB_PATH = os.environ.get("BACKEND_DB", "/data/writes.db")

# 故障开关是进程内瞬态，不需要落库。
_fault_on = False
# 正在慢回执中的请求 id -> asyncio.Event，用于 /writes/{id} 等待回执完成。
_pending: dict[str, asyncio.Event] = {}


async def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS writes (
                id           TEXT PRIMARY KEY,
                payload      TEXT NOT NULL,
                client_ip    TEXT,
                received_at  REAL NOT NULL,
                acked_at     REAL,
                delay        REAL NOT NULL DEFAULT 0
            )
            """
        )
        await db.commit()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(title=f"mock-write-backend-{NODE_NAME}", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> JSONResponse:
    if _fault_on:
        return JSONResponse(status_code=503, content={"node": NODE_NAME, "healthy": False})
    return JSONResponse(status_code=200, content={"node": NODE_NAME, "healthy": True})


@app.post("/control/fault")
async def control_fault(request: Request) -> dict[str, Any]:
    """注入/解除健康故障。body: {"on": true|false}"""
    global _fault_on
    body = await request.json()
    _fault_on = bool(body.get("on", True))
    return {"node": NODE_NAME, "fault": _fault_on}


@app.post("/writes")
async def create_write(request: Request) -> JSONResponse:
    if _fault_on:
        # 故障期间拒绝写入（代理探测会先失败，正常不会走到这里）。
        return JSONResponse(
            status_code=500,
            content={"node": NODE_NAME, "error": "node in fault state, write rejected"},
        )

    delay = float(request.query_params.get("delay", "0"))
    try:
        body = await request.json()
    except Exception:
        body = {}
    write_id = str(body.get("id") or uuid.uuid4())
    payload = str(body.get("payload", ""))
    now = time.time()

    # 先落库，再慢回执：即使回执期间连接被切，写入也已经在本节点持久化。
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO writes (id, payload, client_ip, received_at, delay) "
            "VALUES (?, ?, ?, ?, ?)",
            (write_id, payload, request.client.host if request.client else None, now, delay),
        )
        await db.commit()

    if delay > 0:
        done = asyncio.Event()
        _pending[write_id] = done
        try:
            await asyncio.sleep(delay)
        finally:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE writes SET acked_at = ? WHERE id = ?", (time.time(), write_id)
                )
                await db.commit()
            _pending.pop(write_id, None)
            done.set()
    else:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE writes SET acked_at = ? WHERE id = ?", (time.time(), write_id)
            )
            await db.commit()

    return JSONResponse(
        status_code=201,
        content={
            "id": write_id,
            "node": NODE_NAME,
            "received_at": now,
            "delay": delay,
            "status": "acked",
        },
    )


@app.get("/writes/{write_id}")
async def get_write(write_id: str, wait: bool = False) -> JSONResponse:
    """查询单条写入的回执状态。

    wait=true 时，如果该写入仍在慢回执中，最多等待 60 秒，供代理向原送达节点取回执。
    """
    if wait and write_id in _pending:
        try:
            await asyncio.wait_for(_pending[write_id].wait(), timeout=60)
        except asyncio.TimeoutError:
            pass

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM writes WHERE id = ?", (write_id,))
        row = await cur.fetchone()
    if row is None:
        return JSONResponse(
            status_code=404, content={"node": NODE_NAME, "id": write_id, "found": False}
        )
    data = dict(row)
    data["node"] = NODE_NAME
    data["found"] = True
    data["status"] = "acked" if data["acked_at"] is not None else "pending"
    return JSONResponse(status_code=200, content=data)


@app.get("/records")
async def records() -> dict[str, Any]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT id, payload, client_ip, received_at, acked_at, delay "
            "FROM writes ORDER BY received_at"
        )
        rows = await cur.fetchall()
    return {"node": NODE_NAME, "count": len(rows), "records": [dict(r) for r in rows]}
