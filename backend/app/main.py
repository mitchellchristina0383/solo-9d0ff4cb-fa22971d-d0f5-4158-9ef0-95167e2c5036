"""模拟写入后端。

每个后端实例：
  * 把收到的写入落到自己的 SQLite（/data/writes.db），持久化保存；
  * 支持慢回执：POST /writes?delay=秒，先落库再延迟回执，模拟"已送达、未回执"；
  * 支持健康故障注入：POST /control/fault on/off，故障期间 /healthz 503、写入 500；
  * 支持崩溃演练：POST /control/crash，立即（或延迟若干秒）退出进程，
    配合容器 restart 策略与同卷重启，演练"回执前节点退出"；
  * /writes/<id> 供代理在切流后仍向"原送达节点"查询回执；
  * /records 列出本节点全部写入记录。

崩溃恢复语义（关键）：
  慢写入在延迟前就已把行落库（acked_at 为 NULL）。若进程在回执前退出，
  重启后内存中的慢回执任务必然不存在；启动时把所有 acked_at IS NULL 的行
  依据持久记录确认（置 acked_at/recovered_at）。此时服务尚未开始受理请求，
  不存在"仍在运行的慢回执"，因此查询不会把活的慢回执提前确认。
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


async def _migrate(db: aiosqlite.Connection) -> None:
    cur = await db.execute("PRAGMA table_info(writes)")
    columns = {row[1] for row in await cur.fetchall()}
    if "recovered_at" not in columns:
        await db.execute("ALTER TABLE writes ADD COLUMN recovered_at REAL")


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
                delay        REAL NOT NULL DEFAULT 0,
                recovered_at REAL
            )
            """
        )
        await _migrate(db)
        await db.commit()


async def recover_after_restart() -> int:
    """重启恢复：依据持久记录确认崩溃前未回执的写入。

    此刻进程刚启动、还未对外服务，_pending 必为空——凡是 acked_at IS NULL 的行，
    都是上一进程在回执前退出留下的（活的慢回执只可能存在于服务运行期）。
    把它们就地标记为已确认，并记录 recovered_at 供控制台/代理区分"恢复确认"。
    返回恢复的行数。
    """
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id FROM writes WHERE acked_at IS NULL ORDER BY received_at"
        )
        orphan_ids = [row[0] for row in await cur.fetchall()]
        now = time.time()
        for write_id in orphan_ids:
            await db.execute(
                "UPDATE writes SET acked_at = ?, recovered_at = ? WHERE id = ?",
                (now, now, write_id),
            )
        await db.commit()
    return len(orphan_ids)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    recovered = await recover_after_restart()
    if recovered:
        # 事件只进日志/标准输出；持久事实在 writes 表的 recovered_at 列。
        print(f"[recover] node {NODE_NAME} confirmed {recovered} write(s) "
              "left unacked by the previous process", flush=True)
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


@app.post("/control/crash")
async def control_crash(request: Request) -> JSONResponse:
    """模拟节点崩溃：先返回本响应，再（可选延迟后）立即退出进程。

    body: {"delay": 秒}，默认 0。数据已在写入处理中 commit，
    崩溃不影响已落库内容；配合同卷重启可演练回执恢复链。
    """
    try:
        body = await request.json()
    except Exception:
        body = {}
    delay = float(body.get("delay", 0) or 0)

    async def _kill() -> None:
        if delay > 0:
            await asyncio.sleep(delay)
        # 硬退出：跳过 atexit/缓冲刷新，模拟真实崩溃（已 commit 的 SQLite 行不丢）。
        os._exit(1)

    asyncio.create_task(_kill())
    return JSONResponse(status_code=202,
                        content={"node": NODE_NAME, "crashing": True, "delay": delay})


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

    # 先落库，再慢回执：即使回执期间连接被切/进程崩溃，写入也已经在本节点持久化。
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
                    "UPDATE writes SET acked_at = ? WHERE id = ? AND acked_at IS NULL",
                    (time.time(), write_id),
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
    注意：仍在运行的慢回执（_pending 中）在其完成前一律报告 pending，
    查询本身绝不会提前确认；只有进程重启后的崩溃遗留行才会在启动时被恢复确认。
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
    data["recovered"] = data.get("recovered_at") is not None
    data["status"] = "acked" if data["acked_at"] is not None else "pending"
    return JSONResponse(status_code=200, content=data)


@app.get("/records")
async def records() -> dict[str, Any]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT id, payload, client_ip, received_at, acked_at, recovered_at, delay "
            "FROM writes ORDER BY received_at"
        )
        rows = await cur.fetchall()
    return {"node": NODE_NAME, "count": len(rows), "records": [dict(r) for r in rows]}
