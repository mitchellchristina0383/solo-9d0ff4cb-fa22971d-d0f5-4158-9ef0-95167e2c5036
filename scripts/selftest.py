#!/usr/bin/env python3
"""端到端自测（仅依赖标准库）：用临时目录拉起两个后端 + 一个代理并逐项核验。

覆盖：
  1. 普通写入落现役节点，备用节点无该写入（无重复投递）；
  2. 慢回执写入在途时切流：切流立即生效，新请求走新节点，
     在途写入仍由旧节点回执，且只存在于旧节点；
  3. 目标节点健康故障时切流被拦截（409），旧路由保持；
  4. 连续两次切流都不打断各自的在途写入（再次切流安全）；
  5. 回执超时：返回 504 + TIMEOUT_UNCERTAIN，不自动补投；
     事后向"原送达节点"查询可确认其实已落库并回执；
  6. 全量核对：两侧后端写入 ID 集合不相交，每条写入恰好一次；
  7. 回执前节点崩溃：代理记 DISCONNECT_UNCERTAIN（不记 FAILED、不补投），
     原节点同卷重启后凭持久记录恢复确认，refresh 后转 ACKED；
  8. 确认未建立连接（节点未运行）的写入仍为 FAILED；
  9. 仍在运行的慢回执不会被查询提前确认，自然结束后才 ACKED；
 10. 代理自身重启：路由与写入归属不变，待核写入仍绑定原节点；
 11. 原节点暂不可达时 refresh 保持待核，结论不变。

用法:
  python3 -I scripts/selftest.py [--python /path/to/venv/python]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PASS, FAIL = 0, 0


def check(cond: bool, name: str, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  \033[32mPASS\033[0m  {name} {detail}")
    else:
        FAIL += 1
        print(f"  \033[31mFAIL\033[0m  {name} {detail}")


def http(method: str, url: str, body: dict | None = None, timeout: float = 30):
    """返回 (status, json)。连接层失败返回 (-1, {"error": ...})。"""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())
    except (urllib.error.URLError, ConnectionError, OSError) as exc:
        return -1, {"error": f"{type(exc).__name__}: {exc}"}


def wait_ready(url: str, timeout: float = 20) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            time.sleep(0.3)
    return False


def wait_dead(url: str, timeout: float = 10) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=1)
        except Exception:
            return True
        time.sleep(0.2)
    return False


def wait_status(proxy: str, write_id: str, expect: str, timeout: float = 15) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        _, s = http("GET", f"{proxy}/api/status")
        for w in s["writes"]:
            if w["id"] == write_id and w["status"] == expect:
                return w
        time.sleep(0.15)
    raise AssertionError(f"write {write_id} never reached status {expect}")


def get_write_row(proxy: str, write_id: str) -> dict:
    _, s = http("GET", f"{proxy}/api/status")
    return next(w for w in s["writes"] if w["id"] == write_id)


class SlowPost(threading.Thread):
    def __init__(self, proxy: str, delay: float, payload: str):
        super().__init__(daemon=True)
        self.proxy, self.delay, self.payload = proxy, delay, payload
        self.result: tuple[int, dict] | None = None
        self.write_id: str | None = None

    def run(self):
        self.result = http("POST", f"{self.proxy}/api/writes?delay={self.delay}",
                           {"payload": self.payload}, timeout=30)
        if self.result:
            self.write_id = self.result[1].get("id")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--python", default=sys.executable)
    args = ap.parse_args()

    work = Path(tempfile.mkdtemp(prefix="cutover-selftest-"))
    logs = work / "logs"
    logs.mkdir()
    procs: dict[str, subprocess.Popen] = {}
    ports = {"proxy": 18000, "A": 18001, "B": 18002}
    proxy = f"http://127.0.0.1:{ports['proxy']}"
    db_paths = {"A": work / "a.db", "B": work / "b.db"}

    def spawn(kind: str, app_dir: Path, env: dict, port: int) -> None:
        log = open(logs / f"{kind}.log", "wb")
        p = subprocess.Popen(
            [args.python, "-I", "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(port), "--app-dir", str(app_dir)],
            env={**os.environ, **env}, stdout=log, stderr=subprocess.STDOUT,
        )
        procs[kind] = p

    def stop(kind: str) -> None:
        p = procs.pop(kind, None)
        if p is None:
            return
        p.terminate()
        try:
            p.wait(timeout=8)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait(timeout=5)

    def spawn_backend(node: str) -> None:
        spawn(f"backend-{node}", ROOT / "backend",
              {"NODE_NAME": node, "BACKEND_DB": str(db_paths[node])}, ports[node])
        assert wait_ready(f"http://127.0.0.1:{ports[node]}/healthz")

    try:
        print("== 启动两个后端 + 代理（读回执超时 2s，便于演练超时场景） ==")
        spawn_backend("A")
        spawn_backend("B")
        check(wait_ready(f"http://127.0.0.1:{ports['A']}/healthz")
              and wait_ready(f"http://127.0.0.1:{ports['B']}/healthz"),
              "两个后端启动并健康")

        proxy_env = {
            "NODE_A_NAME": "A", "NODE_B_NAME": "B",
            "BACKEND_A_URL": f"http://127.0.0.1:{ports['A']}",
            "BACKEND_B_URL": f"http://127.0.0.1:{ports['B']}",
            "PROXY_DB": str(work / "proxy.db"),
            "WRITER_READ_TIMEOUT": "2", "CONNECT_TIMEOUT": "2", "PROBE_TIMEOUT": "2",
        }
        spawn("proxy", ROOT / "proxy", proxy_env, ports["proxy"])
        check(wait_ready(proxy + "/"), "代理启动并提供控制台页面")

        _, st = http("GET", f"{proxy}/api/status")
        check(st["active"] == "A", "初始现役节点为 A", f"active={st['active']}")

        print("\n== 场景 1：普通写入只落现役节点 A ==")
        code, w0 = http("POST", f"{proxy}/api/writes?delay=0", {"payload": "base-write"})
        check(code == 201 and w0["status"] == "ACKED", "普通写入成功回执",
              f"id={w0.get('id','')[:8]} node={w0.get('node')}")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        check(any(r["id"] == w0["id"] for r in rec_a["records"]), "写入存在于 A 的 SQLite")
        check(not any(r["id"] == w0["id"] for r in rec_b["records"]), "B 上没有该写入（未重复投递）")

        print("\n== 场景 2：慢回执在途切流 A -> B，不中断、不重复 ==")
        slow1 = SlowPost(proxy, 1.2, "slow-during-cutover-1")
        slow1.start()
        deadline = time.time() + 5
        pending_id = None
        while time.time() < deadline:
            _, s = http("GET", f"{proxy}/api/status")
            for row in s["writes"]:
                if row["status"] == "PENDING":
                    pending_id = row["id"]
                    inflight = s["drainage"]["A"]["inflight_now"]
                    check(inflight >= 1, "切流前 A 显示在途写入", f"inflight_now={inflight}")
                    break
            if pending_id:
                break
            time.sleep(0.1)
        check(bool(pending_id), "慢写入已进入 PENDING（已送达、待回执）")

        code, sw = http("POST", f"{proxy}/api/switch", {"target": "B"})
        check(code == 200 and sw["ok"] and sw["active"] == "B", "在途切流 A -> B 成功")
        _, s2 = http("GET", f"{proxy}/api/status")
        check(s2["drainage"]["A"]["draining"] is True, "旧节点 A 进入排空调（DRAINING）")

        slow1.join(timeout=10)
        check(not slow1.is_alive() and slow1.result and slow1.result[0] == 201,
              "在途写入未被切流打断，仍正常拿到回执")
        check(slow1.write_id == pending_id and slow1.result[1]["node"] == "A",
              "回执仍来自原送达节点 A",
              f"node={slow1.result[1].get('node') if slow1.result else '?'}")
        row = wait_status(proxy, pending_id, "ACKED")
        check(row["target_node"] == "A", "代理记录中该写入始终绑定 A")

        code, w_new = http("POST", f"{proxy}/api/writes?delay=0", {"payload": "after-switch"})
        check(code == 201 and w_new["node"] == "B", "切流后新请求立即走新节点 B",
              f"node={w_new.get('node')}")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        ids_a, ids_b = {r["id"] for r in rec_a["records"]}, {r["id"] for r in rec_b["records"]}
        check(pending_id in ids_a and pending_id not in ids_b, "慢写入只在 A，未被投到 B")
        check(w_new["id"] in ids_b and w_new["id"] not in ids_a, "新写入只在 B")

        print("\n== 场景 3：目标节点探测失败 -> 保持旧路由 ==")
        http("POST", f"{proxy}/api/nodes/A/fault", {"on": True})
        code, blocked = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 409 and blocked.get("blocked") and blocked.get("active") == "B",
              "A 故障时切向 A 被 409 拦截，现役仍为 B", blocked.get("reason", "")[:90])
        http("POST", f"{proxy}/api/nodes/A/fault", {"on": False})
        code, sw2 = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 200 and sw2["active"] == "A", "故障解除后切流成功，现役回到 A")

        print("\n== 场景 4：连续再次切流，各自在途写入都绑定原节点 ==")
        t1 = SlowPost(proxy, 1.2, "roundtrip-a")
        t1.start()
        time.sleep(0.35)
        code, _ = http("POST", f"{proxy}/api/switch", {"target": "B"})
        check(code == 200, "第二次切流 A -> B 成功且不打断在途写入")
        t1.join(timeout=10)
        check(t1.result and t1.result[1]["node"] == "A", "切走前送达 A 的写入仍由 A 回执")

        t2 = SlowPost(proxy, 1.2, "roundtrip-b")
        t2.start()
        time.sleep(0.35)
        code, _ = http("POST", f"{proxy}/api/switch", {"target": "A"})
        check(code == 200, "第三次切流 B -> A 成功")
        t2.join(timeout=10)
        check(t2.result and t2.result[1]["node"] == "B",
              "切走前送达 B 的写入仍由 B 回执（再次切流不打断它）")

        print("\n== 场景 5：回执超时 -> 结果不确定，不补投，可事后向原节点核实 ==")
        active_before = http("GET", f"{proxy}/api/status")[1]["active"]
        t0 = time.time()
        code, tout = http("POST", f"{proxy}/api/writes?delay=5",
                          {"payload": "timeout-case"}, timeout=15)
        elapsed = time.time() - t0
        check(code == 504 and tout.get("uncertain") and
              tout["status"] == "TIMEOUT_UNCERTAIN",
              f"读超时返回 504 + TIMEOUT_UNCERTAIN（{elapsed:.1f}s 触发）")
        timeout_id = tout["id"]
        timeout_node = tout["node"]
        row = get_write_row(proxy, timeout_id)
        check(row["status"] == "TIMEOUT_UNCERTAIN", "超时状态已落库，页面可见结果不确定")
        _, rec_other = http("GET", f"{proxy}/api/nodes/"
                            f"{'B' if timeout_node == 'A' else 'A'}/records")
        check(not any(r["id"] == timeout_id for r in rec_other["records"]),
              "超时后代理没有向另一节点补投")

        print("   等待后端慢回执结束后，向原送达节点查询核实……")
        time.sleep(4.2)
        code, ref = http("GET", f"{proxy}/api/writes/{timeout_id}/refresh")
        check(code == 200 and ref["write"]["status"] == "ACKED"
              and ref["write"]["target_node"] == timeout_node,
              "向原节点查询后确认：写入其实已落库并回执，状态就地更正，未换节点")

        print("\n== 场景 6：全量核对，两侧后端写入不相交、每条恰好一次 ==")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        ids_a, ids_b = {r["id"] for r in rec_a["records"]}, {r["id"] for r in rec_b["records"]}
        check(ids_a.isdisjoint(ids_b), "A/B 后端写入 ID 集合不相交（无任何重复投递）")
        _, s_all = http("GET", f"{proxy}/api/status")
        proxy_ids = {w["id"] for w in s_all["writes"]}
        check(proxy_ids == ids_a | ids_b,
              f"代理侧 {len(proxy_ids)} 条写入与后端记录一一对应",
              f"A={len(ids_a)} B={len(ids_b)}")

        print("\n== 场景 7：回执前节点崩溃 -> DISCONNECT_UNCERTAIN，同卷重启后恢复确认 ==")
        # 先确保现役是 A，向 A 投一笔 8s 慢写入。
        if http("GET", f"{proxy}/api/status")[1]["active"] != "A":
            code, _ = http("POST", f"{proxy}/api/switch", {"target": "A"})
            assert code == 200
        crash_post = SlowPost(proxy, 8, "crash-before-ack")
        crash_post.start()
        crash_id = None
        deadline = time.time() + 5
        while time.time() < deadline:
            _, s = http("GET", f"{proxy}/api/status")
            for row in s["writes"]:
                if row["payload"] == "crash-before-ack" and row["status"] == "PENDING":
                    crash_id = row["id"]
            if crash_id:
                break
            time.sleep(0.1)
        check(bool(crash_id), "慢写入已送达 A 并落库（PENDING）")
        # 立刻让 A 崩溃：写入已落库、回执前节点退出。
        code, cr = http("POST", f"{proxy}/api/nodes/A/crash", {"delay": 0})
        # 代理透传 JSON（状态码归一为 200，crashing 标志保留）；
        # 若响应未及送出进程已退出，也可能得到 502/-1，均说明崩溃指令生效。
        check((code == 200 and cr.get("crashing") is True) or code in (502, -1),
              "崩溃指令已下发到节点 A", f"http={code} body={str(cr)[:80]}")
        check(wait_dead(f"http://127.0.0.1:{ports['A']}/healthz"), "节点 A 已退出")
        crash_post.join(timeout=15)
        check(crash_post.result is not None, "崩溃后代理对客户端的写入请求已返回")
        cstatus, cbody = crash_post.result
        check(cstatus == 502 and cbody.get("uncertain") is True
              and cbody.get("status") == "DISCONNECT_UNCERTAIN",
              "回执前断线保留结果不确定（不记 FAILED）",
              f"http={cstatus} status={cbody.get('status')}")
        row = get_write_row(proxy, crash_id)
        check(row["status"] == "DISCONNECT_UNCERTAIN" and row["target_node"] == "A",
              "代理持久记录：DISCONNECT_UNCERTAIN 且归属仍为 A")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        check(not any(r["id"] == crash_id for r in rec_b["records"]),
              "断线后未向 B 补投")

        # 节点暂不可达时 refresh：保持待核，结论不变。
        code, ref_dead = http("GET", f"{proxy}/api/writes/{crash_id}/refresh")
        check(code == 200 and ref_dead.get("reachable") is False
              and ref_dead["write"]["status"] == "DISCONNECT_UNCERTAIN",
              "原节点不可达时 refresh 保持待核，不覆盖结论")

        # 同卷重启 A：启动恢复应把崩溃遗留行就地确认。
        spawn_backend("A")
        con = sqlite3.connect(db_paths["A"])
        rec = con.execute(
            "SELECT acked_at, recovered_at FROM writes WHERE id = ?", (crash_id,)
        ).fetchone()
        con.close()
        check(rec is not None and rec[0] is not None and rec[1] is not None,
              "A 同卷重启后凭持久记录恢复确认该写入（acked_at/recovered_at 均写入）")
        code, ref_rec = http("GET", f"{proxy}/api/writes/{crash_id}/refresh")
        check(code == 200 and ref_rec["refreshed"] is True
              and ref_rec["write"]["status"] == "ACKED"
              and ref_rec["write"]["target_node"] == "A"
              and ref_rec["backend_view"].get("recovered") is True,
              "控制台按原写入 ID 核实后转 ACKED，归属不变（重启恢复确认）")
        check(get_write_row(proxy, crash_id)["status"] == "ACKED",
              "代理库结论已持久更正为 ACKED")

        print("\n== 场景 8：确认未建立连接（节点未运行）仍为 FAILED ==")
        # 切到 B 作为现役，然后停掉 B，使新写入在连接阶段失败。
        code, _ = http("POST", f"{proxy}/api/switch", {"target": "B"})
        check(code == 200, "切流到 B 成功")
        stop("backend-B")
        check(wait_dead(f"http://127.0.0.1:{ports['B']}/healthz"), "节点 B 已停止")
        t0 = time.time()
        code, fail_body = http("POST", f"{proxy}/api/writes?delay=0",
                               {"payload": "connect-refused"}, timeout=15)
        elapsed = time.time() - t0
        check(code == 502 and fail_body.get("status") == "FAILED"
              and fail_body.get("uncertain") is False,
              f"连接未建立记 FAILED（{elapsed:.1f}s）", fail_body.get("detail", "")[:80])
        fail_id = fail_body.get("id")
        check(get_write_row(proxy, fail_id)["status"] == "FAILED", "FAILED 已持久化")
        spawn_backend("B")  # 恢复 B 供后续场景

        print("\n== 场景 9：仍在运行的慢回执不会被查询提前确认 ==")
        code, _ = http("POST", f"{proxy}/api/switch", {"target": "A"})
        assert code == 200
        # 延迟 1.2s 短于代理读超时 2s：请求保持在途，不会先被超时改写。
        live = SlowPost(proxy, 1.2, "live-slow-ack")
        live.start()
        live_id = None
        deadline = time.time() + 5
        while time.time() < deadline:
            _, s = http("GET", f"{proxy}/api/status")
            for row in s["writes"]:
                if row["payload"] == "live-slow-ack":
                    live_id = row["id"]
            if live_id:
                break
            time.sleep(0.1)
        check(bool(live_id), "新的慢写入已进入 PENDING")
        time.sleep(1.0)
        code, early = http("GET", f"{proxy}/api/writes/{live_id}/refresh")
        check(code == 200 and early.get("refreshed") is False
              and early["write"]["status"] == "PENDING",
              "慢回执仍在运行时查询不提前确认，保持 PENDING")
        # 直接问后端，也应仍是 pending（未被恢复逻辑误伤）。
        code, backend_view = http(
            "GET", f"http://127.0.0.1:{ports['A']}/writes/{live_id}")
        check(code == 200 and backend_view["status"] == "pending"
              and backend_view.get("recovered") is False,
              "后端视角同样仍是 pending（活的慢回执未被启动恢复标记）")
        live.join(timeout=15)
        row = wait_status(proxy, live_id, "ACKED")
        check(row["status"] == "ACKED", "慢回执自然结束后才转 ACKED")

        print("\n== 场景 10：代理自身重启，路由与写入归属不变 ==")
        # 先制造一笔不确定写入（超时），记录其归属，再重启代理。
        active_now = http("GET", f"{proxy}/api/status")[1]["active"]
        code, uncert = http("POST", f"{proxy}/api/writes?delay=5",
                            {"payload": "across-proxy-restart"}, timeout=15)
        check(code == 504 and uncert["status"] == "TIMEOUT_UNCERTAIN",
              "制造一笔待核写入用于代理重启验证")
        uncert_id, uncert_node = uncert["id"], uncert["node"]
        stop("proxy")
        spawn("proxy", ROOT / "proxy", proxy_env, ports["proxy"])
        check(wait_ready(proxy + "/"), "代理重启完成")
        _, s3 = http("GET", f"{proxy}/api/status")
        check(s3["active"] == active_now, "重启后现役路由保持不变",
              f"active={s3['active']}")
        row = next(w for w in s3["writes"] if w["id"] == uncert_id)
        check(row["target_node"] == uncert_node
              and row["status"] == "TIMEOUT_UNCERTAIN",
              "待核写入重启后仍绑定原节点、状态不变")
        boot_events = [e for e in s3["events"] if e["kind"] == "BOOT"]
        check(any("awaiting confirmation" in e["message"] for e in boot_events),
              "重启 BOOT 事件提示有待核写入")
        time.sleep(4.5)
        code, ref_after = http("GET", f"{proxy}/api/writes/{uncert_id}/refresh")
        check(ref_after["write"]["status"] == "ACKED"
              and ref_after["write"]["target_node"] == uncert_node,
              "重启后仍可向原节点核实并更正，归属不变")

        print("\n== 场景 11：最终持久化核对 ==")
        for node, db_path in (("A", db_paths["A"]), ("B", db_paths["B"])):
            con = sqlite3.connect(db_path)
            n = con.execute("SELECT COUNT(*) FROM writes").fetchone()[0]
            con.close()
            _, rec = http("GET", f"{proxy}/api/nodes/{node}/records")
            check(n == len(rec["records"]),
                  f"后端 {node} 的 SQLite 持久化写入数 = {n}")
        _, rec_a = http("GET", f"{proxy}/api/nodes/A/records")
        _, rec_b = http("GET", f"{proxy}/api/nodes/B/records")
        ids_a, ids_b = {r["id"] for r in rec_a["records"]}, {r["id"] for r in rec_b["records"]}
        check(ids_a.isdisjoint(ids_b), "最终 A/B 写入 ID 集合仍不相交")
        # 崩溃那笔必须只在 A，且带恢复标记。
        crash_rows = [r for r in rec_a["records"] if r["id"] == crash_id]
        check(len(crash_rows) == 1 and crash_rows[0].get("recovered_at") is not None,
              "崩溃写入仅存在于 A 且带 recovered_at 标记")
        # 连接失败那笔两个后端都不该有。
        check(fail_id not in ids_a and fail_id not in ids_b,
              "FAILED（未送达）写入未出现在任何后端")

    finally:
        print("\n== 清理进程 ==")
        for kind in list(procs):
            stop(kind)
        if FAIL:
            print(f"\n日志与临时数据保留在: {work}")
        else:
            shutil.rmtree(work, ignore_errors=True)

    print(f"\n================ 自测结果: {PASS} 通过 / {FAIL} 失败 ================")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
