# 写入切流反向代理（FastAPI + SQLite）

值班工程师更新服务节点时，用本系统把写入流量从旧节点切到备用节点。核心保证：

- **不中断已送达的写入**：每个写入在投递前快照现役节点并持久化绑定，切流只改路由表，
  不取消在途请求；回执仍从**原送达节点**读取，连续多次切流也不改变归属、不打断它，
  更不会向其他节点补投。
- **不重复投递**：代理对任何写入只发一次。按"连接是否建立过"区分失败性质：
  - 确认未建立连接（`ConnectError`/`ConnectTimeout`/`PoolTimeout`）或后端明确拒绝 →
    `FAILED`，写入确定未送达；
  - 请求已发出后的读超时 → `TIMEOUT_UNCERTAIN`（504）；
  - 请求已发出后的连接中断/重置/协议错误（典型：节点在回执前退出）→
    `DISCONNECT_UNCERTAIN`（502）。两类"不确定"都只保留结果不确定，**不自动补投**，
    后端两侧写入 ID 集合永不相交。
- **崩溃回执恢复链**：后端"先落库再慢回执"。若节点在回执前退出，同卷重启时启动恢复会
  依据 SQLite 持久记录把崩溃遗留的未回执写入就地确认（写 `acked_at`/`recovered_at`）；
  恢复发生在服务受理请求之前，因此**仍在运行的慢回执绝不会被查询提前确认**。
- **探测失败保持旧路由**：切流前先探目标节点 `/healthz`，失败返回 409，路由表不变。
- **控制台可按原写入 ID 核实**：不确定写入可点"向原节点查询"或输入原写入 ID 核实；
  原节点暂不可达时保持待核、结论不变；节点恢复后凭持久记录确认并就地更正为 `ACKED`。

## 目录结构

```
backend/            模拟写入后端（两个实例，各自 SQLite 保存收到的写入）
  app/main.py       慢回执 delay、健康故障注入 /control/fault、崩溃注入 /control/crash、
                    启动恢复 recover_after_restart、/healthz、/records、/writes/{id}
  Dockerfile
proxy/              FastAPI 反向代理 + 切流控制台
  app/main.py       代理、切流、探测、在途/排空跟踪、故障/崩溃透传、回执核实
  app/db.py         SQLite 持久层（nodes / route / writes / events）
  app/dashboard.html 切流网页（现役节点、排空状态、写入记录、按 ID 核实、事件日志）
  Dockerfile
docker-compose.yml  一键启动 proxy + backend-a + backend-b（含健康检查、数据卷与重启策略）
scripts/selftest.py 纯标准库端到端自测（58 项断言，11 个场景）
```

## 用 Docker Compose 启动

```bash
docker compose up --build
# 控制台： http://localhost:8080/
```

- 后端各自把写入保存在卷 `backend-a-data` / `backend-b-data` 的 `/data/writes.db`；
  代理状态在卷 `proxy-data` 的 `/data/proxy.db`。
- 后端配置 `restart: unless-stopped`：在网页上点"模拟节点崩溃"后会自动同卷重启，
  可完整演练"回执前退出 → 断线不确定 → 同卷恢复确认 → 控制台核实转 ACKED"。
- 想演练回执超时，可把 compose 里 `WRITER_READ_TIMEOUT` 调小（如 `2`）后重建。

## 不用 Docker 的本地运行（开发/自测）

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r proxy/requirements.txt        # 后端依赖相同
mkdir -p .run/data
NODE_NAME=A BACKEND_DB=$PWD/.run/data/a.db \
  python -m uvicorn app.main:app --app-dir backend --port 8001 &
NODE_NAME=B BACKEND_DB=$PWD/.run/data/b.db \
  python -m uvicorn app.main:app --app-dir backend --port 8002 &
NODE_A_NAME=A NODE_B_NAME=B \
BACKEND_A_URL=http://127.0.0.1:8001 BACKEND_B_URL=http://127.0.0.1:8002 \
PROXY_DB=$PWD/.run/data/proxy.db \
  python -m uvicorn app.main:app --app-dir proxy --port 8080
```

## 网页操作演练

1. 慢回执延迟填 `10` 秒，点"投递写入"，在请求返回前立刻点另一节点的"切换流量"；
2. 观察：现役节点立即变更；旧节点卡片显示 **DRAINING 排空中** 与在途写入 ID；
   慢请求结束后仍由旧节点回执，写入只存在于旧节点；
3. 点某节点"注入健康故障"后再向它切流：收到 409 拦截提示，现役节点不变；
4. 投递一笔长延迟写入后点该节点"模拟节点崩溃"：写入显示红色
   **DISCONNECT_UNCERTAIN**；节点自动重启后点"向原节点查询"（或输入原写入 ID），
   状态就地更正为 **ACKED**，归属节点不变，另一节点无此写入；
5. "拉取 A/B 的记录"可对比两个后端各自 SQLite 中保存的写入（含 `recovered_at` 标记）。

## HTTP 接口（代理 8080）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/writes?delay=秒` | 投递写入（代理只投一次，绑定当前现役节点） |
| POST | `/api/switch` | body `{"target":"B"}`；探测失败 409 并保持旧路由 |
| GET  | `/api/status` | 现役节点、健康、排空/在途、全部写入与事件 |
| GET  | `/api/writes/{id}/refresh` | 向**原送达节点**查询回执并就地更新状态；节点不可达保持待核 |
| GET  | `/api/nodes/{name}/records` | 透传该后端自己保存的写入记录 |
| POST | `/api/nodes/{name}/fault` | 注入/解除健康故障 `{"on": true|false}` |
| POST | `/api/nodes/{name}/crash` | 模拟节点崩溃 `{"delay": 秒}`，配合 restart 策略同卷重启 |

## 自测

```bash
python -I scripts/selftest.py            # 自动拉起 3 个进程，跑完即清理
```

覆盖：普通写入无重复、慢回执在途切流、探测失败保旧路由、连续再次切流不打断在途写入、
读超时 504 不确定且不补投（事后向原节点核实可转 ACKED）、回执前节点崩溃记
DISCONNECT_UNCERTAIN 且同卷重启后凭持久记录恢复确认、确认未连接的失败仍为 FAILED、
活的慢回执不被查询提前确认、代理重启后路由与归属不变、不可达时 refresh 保持待核、
两侧后端记录不相交且与代理一一对应。

## 写入状态机

| 状态 | 含义 | 后续动作 |
| --- | --- | --- |
| `PENDING` | 已投递、等待回执（含在途慢回执） | 等回执或向原节点查询，查询不提前确认 |
| `ACKED` | 原节点成功回执，或事后凭其持久记录确认（含重启恢复） | 终态 |
| `TIMEOUT_UNCERTAIN` | 读超时，请求已发出、回执未归，可能已落库 | 只向原节点核实，不补投 |
| `DISCONNECT_UNCERTAIN` | 发出后连接中断/节点退出，送达与否不确定 | 只向原节点核实，不补投；节点不可达时保持待核 |
| `FAILED` | 确认连接未建立或后端明确拒绝，未送达 | 终态，不补投 |

## 关键环境变量

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `WRITER_READ_TIMEOUT` | 30 | 等回执读超时秒数，超时即结果不确定 |
| `CONNECT_TIMEOUT` | 3 | 连接超时，确认连接未建立的失败视为未送达 `FAILED` |
| `PROBE_TIMEOUT` | 2.5 | 切流健康探测超时 |
| `BACKEND_A_URL` / `BACKEND_B_URL` | 本地 8001/8002 | 两个后端地址 |
