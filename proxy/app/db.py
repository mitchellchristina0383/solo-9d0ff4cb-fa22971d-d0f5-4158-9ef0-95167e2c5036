"""代理侧 SQLite 持久层：节点配置、当前路由、写入记录、切流事件。

写入记录一旦落库即代表"已送达某节点"，回执状态只能就地向原节点查询更新，
任何状态机代码都不会把同一条写入再投到另一个节点。
"""
from __future__ import annotations

import os
import time
from typing import Any

import aiosqlite

DB_PATH = os.environ.get("PROXY_DB", "/data/proxy.db")

# 写入状态机
PENDING = "PENDING"        # 已送达/投递中，尚未拿到回执
ACKED = "ACKED"            # 已拿到原节点成功回执
TIMEOUT_UNCERTAIN = "TIMEOUT_UNCERTAIN"  # 写入已落库但回执超时，结果不确定
FAILED = "FAILED"          # 节点明确拒绝（连接前失败/5xx），未送达

# 单连接 + 单写锁，SQLite 写入串行化，避免 lock 竞争。
_conn: aiosqlite.Connection | None = None
_write_lock: Any = None


async def init_db() -> None:
    global _conn, _write_lock
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    _conn = aiosqlite.connect(DB_PATH)
    await _conn
    _write_lock = asyncio_lock()
    _conn.row_factory = aiosqlite.Row
    await _conn.execute("PRAGMA journal_mode=WAL")
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nodes (
            name        TEXT PRIMARY KEY,
            base_url    TEXT NOT NULL,
            created_at  REAL NOT NULL
        )
        """
    )
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS route (
            id          INTEGER PRIMARY KEY CHECK (id = 1),
            active      TEXT NOT NULL,
            updated_at  REAL NOT NULL
        )
        """
    )
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS writes (
            id           TEXT PRIMARY KEY,
            target_node  TEXT NOT NULL,
            payload      TEXT NOT NULL,
            delay_hint   REAL NOT NULL DEFAULT 0,
            status       TEXT NOT NULL,
            status_detail TEXT NOT NULL DEFAULT '',
            created_at   REAL NOT NULL,
            updated_at   REAL NOT NULL,
            acked_at     REAL
        )
        """
    )
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          REAL NOT NULL,
            kind        TEXT NOT NULL,
            message     TEXT NOT NULL
        )
        """
    )
    await _conn.commit()


def asyncio_lock() -> Any:
    import asyncio

    return asyncio.Lock()


async def register_node(name: str, base_url: str) -> None:
    async with _write_lock:
        await _conn.execute(
            "INSERT OR IGNORE INTO nodes (name, base_url, created_at) VALUES (?, ?, ?)",
            (name, base_url, time.time()),
        )
        # 已存在则更新地址（compose 重建容器场景）。
        await _conn.execute(
            "UPDATE nodes SET base_url = ? WHERE name = ?", (base_url, name)
        )
        cur = await _conn.execute("SELECT COUNT(*) AS c FROM route")
        row = await cur.fetchone()
        if row["c"] == 0:
            await _conn.execute(
                "INSERT INTO route (id, active, updated_at) VALUES (1, ?, ?)",
                (name, time.time()),
            )
        await _conn.commit()


async def get_active() -> str:
    cur = await _conn.execute("SELECT active FROM route WHERE id = 1")
    row = await cur.fetchone()
    return row["active"]


async def set_active(name: str) -> None:
    async with _write_lock:
        await _conn.execute(
            "UPDATE route SET active = ?, updated_at = ? WHERE id = 1",
            (name, time.time()),
        )
        await _conn.commit()


async def list_nodes() -> list[dict[str, Any]]:
    cur = await _conn.execute("SELECT name, base_url FROM nodes ORDER BY name")
    rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def node_url(name: str) -> str | None:
    cur = await _conn.execute("SELECT base_url FROM nodes WHERE name = ?", (name,))
    row = await cur.fetchone()
    return row["base_url"] if row else None


async def insert_write(
    write_id: str, target_node: str, payload: str, delay_hint: float
) -> None:
    async with _write_lock:
        await _conn.execute(
            "INSERT INTO writes (id, target_node, payload, delay_hint, status, "
            "status_detail, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                write_id,
                target_node,
                payload,
                delay_hint,
                PENDING,
                "delivered, awaiting ack",
                time.time(),
                time.time(),
            ),
        )
        await _conn.commit()


async def update_write_status(
    write_id: str, status: str, detail: str, acked: bool = False
) -> None:
    async with _write_lock:
        acked_at = time.time() if acked else None
        if acked:
            await _conn.execute(
                "UPDATE writes SET status = ?, status_detail = ?, updated_at = ?, "
                "acked_at = ? WHERE id = ?",
                (status, detail, time.time(), acked_at, write_id),
            )
        else:
            await _conn.execute(
                "UPDATE writes SET status = ?, status_detail = ?, updated_at = ? "
                "WHERE id = ?",
                (status, detail, time.time(), write_id),
            )
        await _conn.commit()


async def get_write(write_id: str) -> dict[str, Any] | None:
    cur = await _conn.execute("SELECT * FROM writes WHERE id = ?", (write_id,))
    row = await cur.fetchone()
    return dict(row) if row else None


async def list_writes() -> list[dict[str, Any]]:
    cur = await _conn.execute("SELECT * FROM writes ORDER BY created_at DESC LIMIT 200")
    rows = await cur.fetchall()
    return [dict(r) for r in rows]


async def add_event(kind: str, message: str) -> None:
    async with _write_lock:
        await _conn.execute(
            "INSERT INTO events (ts, kind, message) VALUES (?, ?, ?)",
            (time.time(), kind, message),
        )
        await _conn.commit()


async def list_events(limit: int = 30) -> list[dict[str, Any]]:
    cur = await _conn.execute(
        "SELECT ts, kind, message FROM events ORDER BY id DESC LIMIT ?", (limit,)
    )
    rows = await cur.fetchall()
    return [dict(r) for r in rows]
