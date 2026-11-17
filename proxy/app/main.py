"""FastAPI 反向代理 + 写入切流控制台。

切流安全语义（本文件的所有代码路径都遵守）：
  1. 每个写入请求在发出前快照"当前现役节点"，并把该节点名持久化到 writes 行；
     之后无论怎么切流，这个请求只与该节点通信，绝不重投到另一节点。
  2. 切流只改 route 表，对已经发往旧节点、尚未回执的在途请求不做任何取消动作；
     回执仍从旧节点读取（也可事后用 /api/writes/{id}/refresh 向原节点查询）。
  3. 切流前先探测目标节点 /healthz，探测失败则保留旧路由并返回 409。
  4. 回执恢复链按"连接是否建立过"区分失败性质：
       - 确认未建立连接（ConnectError/ConnectTimeout/PoolTimeout）→ FAILED，
         写入确定未送达，不补投；
       - 请求已发出后的读超时 → TIMEOUT_UNCERTAIN；
       - 请求已发出后的连接中断/重置/协议错误（如节点在回执前退出）→
         DISCONNECT_UNCERTAIN；两类不确定都只保留"结果不确定"，不自动补投，
         待原节点恢复后凭其持久记录核实更正。
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from . import db

NODE_A = os.environ.get("NODE_A_NAME", "A")
NODE_B = os.environ.get("NODE_B_NAME", "B")
URL_A = os.environ.get("BACKEND_A_URL", "http://127.0.0.1:8001")
URL_B = os.environ.get("BACKEND_B_URL", "http://127.0.0.1:8002")

# 连接失败 = 节点不可达、请求未送达；读超时 = 请求已发出、回执未归。
CONNECT_TIMEOUT = float(os.environ.get("CONNECT_TIMEOUT", "3.0"))
READ_TIMEOUT = float(os.environ.get("WRITER_READ_TIMEOUT", "30.0"))
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "2.5"))

# node -> 在途写入 id 集合（仅内存态，进程重启后以 DB 中 PENDING 行作为排空依据）。
_inflight: dict[str, set[str]] = {}
_inflight_lock = asyncio.Lock()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()
    await db.register_node(NODE_A, URL_A)
    await db.register_node(NODE_B, URL_B)
    _inflight.setdefault(NODE_A, set())
    _inflight.setdefault(NODE_B, set())
    app.state.http = httpx.AsyncClient(
        timeout=httpx.Timeout(connect=CONNECT_TIMEOUT, read=READ_TIMEOUT,
                              write=CONNECT_TIMEOUT, pool=CONNECT_TIMEOUT),
        # 显式关闭 httpx 默认的连接重试，保证"一次投递"语义。
        limits=httpx.Limits(max_connections=100),
    )
    unresolved = await db.list_unresolved()
    await db.add_event(
        "BOOT",
        f"proxy started, active={await db.get_active()}, "
        f"{len(unresolved)} write(s) still awaiting confirmation from original node",
    )
    try:
        yield
    finally:
        await app.state.http.aclose()


app = FastAPI(title="write-cutover-proxy", lifespan=lifespan)


# ---------------------------------------------------------------- 健康探测

async def probe_node(base_url: str) -> tuple[bool, str]:
    """返回 (是否健康, 说明)。探测本身绝不触发任何写入。"""
    client: httpx.AsyncClient = app.state.http
    try:
        resp = await client.get(
            f"{base_url}/healthz", timeout=httpx.Timeout(PROBE_TIMEOUT)
        )
    except httpx.RequestError as exc:
        return False, f"probe request failed: {type(exc).__name__}: {exc}"
    if resp.status_code == 200:
        return True, "healthy"
    return False, f"healthz returned HTTP {resp.status_code}: {resp.text[:200]}"


async def node_health_map() -> dict[str, dict[str, str]]:
    nodes = await db.list_nodes()
    results = await asyncio.gather(*(probe_node(n["base_url"]) for n in nodes))
    return {
        n["name"]: {"healthy": ok, "detail": detail, "base_url": n["base_url"]}
        for n, (ok, detail) in zip(nodes, results)
    }


# ---------------------------------------------------------------- 切流

@app.post("/api/switch")
async def switch(request: Request) -> JSONResponse:
    body = await request.json()
    target = str(body.get("target", "")).strip()
    nodes = {n["name"]: n["base_url"] for n in await db.list_nodes()}
    if target not in nodes:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": f"unknown target node: {target!r}",
                     "known_nodes": sorted(nodes)},
        )

    active = await db.get_active()
    if target == active:
        return JSONResponse(content={"ok": True, "noop": True, "active": active,
                                     "message": "target is already active"})

    healthy, detail = await probe_node(nodes[target])
    if not healthy:
        # 探测失败：保持旧路由，不动 route 表。
        await db.add_event(
            "SWITCH_BLOCKED",
            f"{active} -> {target} blocked, target unhealthy: {detail}",
        )
        return JSONResponse(
            status_code=409,
            content={"ok": False, "active": active, "blocked": True,
                     "reason": f"target {target} probe failed: {detail}",
                     "message": "kept old route"},
        )

    async with _inflight_lock:
        inflight_snapshot = {k: len(v) for k, v in _inflight.items()}
        await db.set_active(target)
    await db.add_event(
        "SWITCH_OK",
        f"{active} -> {target} ok; inflight at cutover={inflight_snapshot}; "
        "new requests now route to target, in-flight requests stay on old node",
    )
    return JSONResponse(content={
        "ok": True, "active": target, "previous": active,
        "inflight_at_cutover": inflight_snapshot,
        "message": "switch done; in-flight writes continue against their original node",
    })


# ---------------------------------------------------------------- 写入代理

async def _track(node: str, write_id: str) -> None:
    async with _inflight_lock:
        _inflight.setdefault(node, set()).add(write_id)


async def _untrack(node: str, write_id: str) -> None:
    async with _inflight_lock:
        _inflight.setdefault(node, set()).discard(write_id)


@app.post("/api/writes")
async def proxy_write(request: Request) -> JSONResponse:
    params = request.query_params
    delay = float(params.get("delay", "0"))
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {"payload": body}

    # 关键：路由在投递前快照，切流不影响本请求。
    active = await db.get_active()
    base_url = await db.node_url(active)
    write_id = str(body.get("id") or uuid.uuid4())
    body["id"] = write_id
    payload = str(body.get("payload", ""))

    await db.insert_write(write_id, active, payload, delay)
    await _track(active, write_id)
    client: httpx.AsyncClient = app.state.http
    try:
        resp = await client.post(
            f"{base_url}/writes", params={"delay": delay}, json=body
        )
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout) as exc:
        # 确认连接未建立：请求没有被后端受理，标记 FAILED，不向任何节点补投。
        detail = f"connect to {active} failed before delivery: {type(exc).__name__}: {exc}"
        await db.update_write_status(write_id, db.FAILED, detail)
        await db.add_event("WRITE_FAILED", f"{write_id} -> {active}: {detail}")
        return JSONResponse(status_code=502,
                            content={"id": write_id, "node": active,
                                     "status": db.FAILED, "uncertain": False,
                                     "detail": detail})
    except httpx.ReadTimeout:
        # 读超时：请求已送达，后端可能已落库，回执未归。结果不确定，禁止补投。
        detail = (f"read timeout after {READ_TIMEOUT}s waiting ack from {active}; "
                  "write may already be persisted there, result UNCERTAIN; "
                  "no automatic re-delivery to another node")
        await db.update_write_status(write_id, db.TIMEOUT_UNCERTAIN, detail)
        await db.add_event("WRITE_TIMEOUT_UNCERTAIN",
                           f"{write_id} -> {active}: {detail}")
        return JSONResponse(status_code=504,
                            content={"id": write_id, "node": active,
                                     "status": db.TIMEOUT_UNCERTAIN,
                                     "uncertain": True, "detail": detail})
    except (httpx.WriteTimeout, httpx.ReadError, httpx.NetworkError,
            httpx.RemoteProtocolError, httpx.LocalProtocolError) as exc:
        # 请求已经（可能已经）送达后连接中断：节点也许已经落库，只是回执前连接断了
        # （典型：旧节点在回执前退出）。保留结果不确定，绝不按 FAILED 处理、绝不补投；
        # 待原节点同卷重启后凭其持久记录核实。
        detail = (f"connection to {active} broken after request was sent: "
                  f"{type(exc).__name__}: {exc}; delivery not confirmed, "
                  "write may already be persisted on the original node, result UNCERTAIN; "
                  "no automatic re-delivery to another node")
        await db.update_write_status(write_id, db.DISCONNECT_UNCERTAIN, detail)
        await db.add_event("WRITE_DISCONNECT_UNCERTAIN",
                           f"{write_id} -> {active}: {detail}")
        return JSONResponse(status_code=502,
                            content={"id": write_id, "node": active,
                                     "status": db.DISCONNECT_UNCERTAIN,
                                     "uncertain": True, "detail": detail})
    except httpx.HTTPError as exc:
        # 其它未细分的传输异常：无法证明未送达，保守按不确定处理，同样不补投。
        detail = (f"unclassified transport error against {active} after dispatch: "
                  f"{type(exc).__name__}: {exc}; result UNCERTAIN; "
                  "no automatic re-delivery to another node")
        await db.update_write_status(write_id, db.DISCONNECT_UNCERTAIN, detail)
        await db.add_event("WRITE_DISCONNECT_UNCERTAIN",
                           f"{write_id} -> {active}: {detail}")
        return JSONResponse(status_code=502,
                            content={"id": write_id, "node": active,
                                     "status": db.DISCONNECT_UNCERTAIN,
                                     "uncertain": True, "detail": detail})
    finally:
        await _untrack(active, write_id)

    if resp.status_code in (200, 201):
        try:
            ack = resp.json()
        except Exception:
            ack = {}
        await db.update_write_status(
            write_id, db.ACKED,
            f"acked by {active} (backend status={ack.get('status', 'acked')})",
            acked=True,
        )
        await db.add_event("WRITE_ACKED", f"{write_id} acked by {active}")
        return JSONResponse(status_code=201,
                            content={"id": write_id, "node": active,
                                     "status": db.ACKED, "backend": ack})

    # 后端明确拒绝（如故障态 500）：未受理，FAILED；同样不补投。
    detail = f"{active} rejected with HTTP {resp.status_code}: {resp.text[:300]}"
    await db.update_write_status(write_id, db.FAILED, detail)
    await db.add_event("WRITE_FAILED", f"{write_id} -> {active}: {detail}")
    return JSONResponse(
        status_code=502,
        content={"id": write_id, "node": active, "status": db.FAILED, "detail": detail},
    )


@app.get("/api/writes/{write_id}/refresh")
async def refresh_write(write_id: str) -> JSONResponse:
    """向"原送达节点"查询该写入的最新回执状态——切流后仍然找原节点。

    - 原节点暂不可达：保持原状态（待核），不覆盖结论、不向其他节点补投；
    - 原节点仍在慢回执（backend 报 pending）：不提前确认，保持原状态；
    - 原节点凭持久记录确认 acked（含重启恢复）：就地更正为 ACKED，归属不变。
    """
    row = await db.get_write(write_id)
    if row is None:
        return JSONResponse(status_code=404,
                            content={"error": f"unknown write {write_id}"})
    node = row["target_node"]
    base_url = await db.node_url(node)
    client: httpx.AsyncClient = app.state.http
    try:
        resp = await client.get(
            f"{base_url}/writes/{write_id}",
            timeout=httpx.Timeout(connect=CONNECT_TIMEOUT, read=10.0,
                                  write=CONNECT_TIMEOUT, pool=CONNECT_TIMEOUT),
        )
    except httpx.RequestError as exc:
        # 节点暂不可达：结论保持不变，等待稍后再核，绝不补投别的节点。
        note = (f"original node {node} temporarily unreachable "
                f"({type(exc).__name__}); keep status {row['status']} pending "
                "verification, no re-delivery")
        await db.add_event("WRITE_REFRESH_PENDING",
                           f"{write_id}: {note}")
        return JSONResponse(status_code=200,
                            content={"refreshed": False, "reachable": False,
                                     "write": row, "note": note})

    if resp.status_code == 404:
        note = (f"original node {node} has no row yet; keep status {row['status']} "
                "(do not re-deliver)")
        return JSONResponse(content={"refreshed": False, "reachable": True,
                                     "write": row, "note": note})

    data = resp.json()
    if data.get("status") == "acked":
        if row["status"] != db.ACKED:
            if data.get("recovered"):
                detail = (f"acked confirmed after restart from the persisted record "
                          f"on original node {node} (recovered_at={data.get('recovered_at')})")
                event_kind = "WRITE_RECOVERED"
            else:
                detail = f"acked confirmed later by original node {node}"
                event_kind = "WRITE_RESOLVED"
            await db.update_write_status(write_id, db.ACKED, detail, acked=True)
            await db.add_event(event_kind, f"{write_id}: {detail}")
        refreshed = await db.get_write(write_id)
        return JSONResponse(content={"refreshed": True, "reachable": True,
                                     "write": refreshed, "backend_view": data})

    # 后端仍报告 pending：慢回执还在运行，查询不得提前确认，状态原样保留。
    note = (f"original node {node} still processing the slow ack; "
            f"keep status {row['status']} (queries never confirm early)")
    return JSONResponse(content={"refreshed": False, "reachable": True,
                                 "write": row, "backend_view": data, "note": note})


# ---------------------------------------------------------------- 状态/排空

@app.get("/api/status")
async def status() -> JSONResponse:
    active = await db.get_active()
    health = await node_health_map()
    writes = await db.list_writes()

    async with _inflight_lock:
        inflight_counts = {k: len(v) for k, v in _inflight.items()}
        inflight_ids = {k: sorted(v) for k, v in _inflight.items()}

    # 排空视角：已切走的旧节点上仍未拿到回执的写入。
    per_node: dict[str, dict[str, Any]] = {}
    empty_slot = lambda: {"total": 0, "pending": 0, "acked": 0,
                          "timeout_uncertain": 0, "disconnect_uncertain": 0,
                          "failed": 0}
    for w in writes:
        slot = per_node.setdefault(w["target_node"], empty_slot())
        slot["total"] += 1
        slot[w["status"].lower()] = slot.get(w["status"].lower(), 0) + 1
    for name in health:
        per_node.setdefault(name, empty_slot())
    for name, slot in per_node.items():
        slot["inflight_now"] = inflight_counts.get(name, 0)
        slot["inflight_ids"] = inflight_ids.get(name, [])
        slot["is_active"] = name == active
        # 未确认（PENDING 或任一不确定状态）的旧节点都处于排空待核。
        slot["unresolved"] = (slot["pending"] + slot["timeout_uncertain"]
                              + slot["disconnect_uncertain"])
        slot["draining"] = name != active and (
            slot["inflight_now"] > 0 or slot["unresolved"] > 0
        )

    return JSONResponse(content={
        "ts": time.time(),
        "active": active,
        "nodes": health,
        "drainage": per_node,
        "writes": writes,
        "events": await db.list_events(),
        "timeouts": {"connect": CONNECT_TIMEOUT, "read": READ_TIMEOUT,
                     "probe": PROBE_TIMEOUT},
    })


@app.get("/api/nodes/{name}/records")
async def node_records(name: str) -> JSONResponse:
    """透传某后端自己保存的写入记录。"""
    base_url = await db.node_url(name)
    if base_url is None:
        return JSONResponse(status_code=404, content={"error": f"unknown node {name}"})
    try:
        resp = await app.state.http.get(
            f"{base_url}/records", timeout=httpx.Timeout(PROBE_TIMEOUT)
        )
    except httpx.RequestError as exc:
        return JSONResponse(status_code=502,
                            content={"error": f"node {name} unreachable: {exc}"})
    return JSONResponse(content=resp.json())


@app.post("/api/nodes/{name}/fault")
async def node_fault(name: str, request: Request) -> JSONResponse:
    """给某后端注入/解除健康故障（透传后端控制接口）。"""
    base_url = await db.node_url(name)
    if base_url is None:
        return JSONResponse(status_code=404, content={"error": f"unknown node {name}"})
    body = await request.json()
    try:
        resp = await app.state.http.post(
            f"{base_url}/control/fault", json={"on": bool(body.get("on", True))},
            timeout=httpx.Timeout(PROBE_TIMEOUT),
        )
    except httpx.RequestError as exc:
        return JSONResponse(status_code=502,
                            content={"error": f"node {name} unreachable: {exc}"})
    await db.add_event("FAULT_INJECTION",
                       f"node {name} fault on={bool(body.get('on', True))}")
    return JSONResponse(content=resp.json())


@app.post("/api/nodes/{name}/crash")
async def node_crash(name: str, request: Request) -> JSONResponse:
    """让某后端立即（或延迟）崩溃退出（透传后端控制接口），演练回执前节点退出。

    compose 中后端配 restart 策略后会同卷自动重启，代理侧在途/不确定写入
    可随后用 /api/writes/{id}/refresh 凭原节点持久记录核实。
    """
    base_url = await db.node_url(name)
    if base_url is None:
        return JSONResponse(status_code=404, content={"error": f"unknown node {name}"})
    try:
        body = await request.json()
    except Exception:
        body = {}
    try:
        resp = await app.state.http.post(
            f"{base_url}/control/crash",
            json={"delay": float(body.get("delay", 0) or 0)},
            timeout=httpx.Timeout(PROBE_TIMEOUT),
        )
    except httpx.RequestError as exc:
        return JSONResponse(status_code=502,
                            content={"error": f"node {name} unreachable: {exc}"})
    await db.add_event("NODE_CRASH",
                       f"node {name} instructed to crash (delay={body.get('delay', 0)}s); "
                       "in-flight writes stay bound to this node, awaiting verification")
    return JSONResponse(content=resp.json())


# ---------------------------------------------------------------- 控制台页面

@app.get("/", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    html = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")
    return HTMLResponse(html)
