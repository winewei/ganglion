# Ganglion 0.2 设计说明

Ganglion 0.2 是一个**小型分布式 Job/Worker 系统**：控制面接收工程 Job，把它安全地交给某个 Worker；Worker 准备环境、拉起 Agent Spine、持有并续租执行权、上报结果。它不是 Agent 框架，不含任何软件工程智能。

协议的权威定义是 API schema（`openapi.json`，由 `ganglion/control/api.py` 生成，测试保证两者一致）。本文件只讲架构、状态机和取舍，不重复字段级定义。

## 一、边界

| 归 Agent Spine | 归 Ganglion |
|---|---|
| 规划、OpenSpec change、change 间 DAG | Job 接收（REST、Git inbox） |
| implement / 独立 review / fix loop / 测试 | Worker 注册与能力 |
| worktree 隔离（Spine 自己的）、集成 | 原子 claim、lease、epoch fencing |
| 最终工程结论 | 心跳、崩溃对账、重试 / 重新分配 |
| | 仓库 / 环境准备、拉起 Spine、结果转发、通知 |

控制面**从不**运行 Claude Code、Codex、测试或构建，也不读 Spine 的内部文件。只有 Worker 上的 Spine adapter 知道 Spine 如何启动、如何判定完成。

## 二、架构

```
                 Git inbox 仓库 ──(inbox adapter, 可在任意一台机器)──┐
                 ganglion job submit / REST ─────────────────────────┤
                                                                     ▼
                     ┌──────────── 控制面 (ganglion server) ────────────┐
                     │ FastAPI  ── service.py (全部规则) ── store.py      │
                     │ jobs / attempts / workers / events  (PostgreSQL)   │
                     └──────────────┬───────────────────────────────────┘
                              HTTP + Bearer token
              ┌─────────────────────┼─────────────────────┐
              ▼                     ▼                     ▼
        Worker (macOS)        Worker (Linux)        Worker (VM)
   register/claim/heartbeat  ... 同左 ...
   repo.py: worktree / clone / gh repo create
   launchers/claude.py: tmux + 交互式 claude
   spine.py: /spine-run --auto；结构化结果 or DELIVERY.md
```

单机模式（`control_plane = "local"`，0.1.x 配置的默认映射）：Worker 进程内嵌同一个 `ControlPlane`，存储用 SQLite。**同一套 Job/Attempt/fencing 代码**，没有第二条路径；只是少了 HTTP 和 PostgreSQL。

### 模块

| 模块 | 职责 |
|---|---|
| `control/models.py` | 状态常量、错误类型（`stale_attempt`、`conflict`、`unavailable`…） |
| `control/store.py` | 事务、SQL（SQLite / PostgreSQL）。业务层只用 `get / insert / cas / 命名查询` |
| `control/service.py` | 控制面全部规则：准入、深度、claim、lease、fencing、幂等、过期、事件、指标、生命周期通知 |
| `control/api.py` | FastAPI + pydantic schema（= 协议），bearer 认证，后台 sweep |
| `control/client.py` | `HttpClient`（纯标准库）与 `LocalClient`（进程内），接口相同 |
| `worker/runtime.py` | Worker 生命周期与每个 attempt 的本地状态机 |
| `worker/repo.py` | 工作区准备：worktree / clone 缓存 / `gh repo create`，需求投递，结果分支推送 |
| `worker/state.py` | Worker 本地 `worker.json`（flock）、`events.jsonl`、单实例锁 |
| `launchers/claude.py` | tmux 会话、trust 预写与兜底、精确会话识别 |
| `spine.py` | Agent Spine adapter：prompt、环境变量契约、结果识别 |
| `inbox.py` | Git inbox → Job（幂等键 `inbox:<id>`） |
| `legacy.py` | 0.1.x `state.json` 的兼容读取、0.1.x 会话的 Stop hook |
| `notify.py` | ntfy / file / null / 自定义后端（与 0.1.x 相同） |

Worker 与单机模式只依赖标准库；`fastapi / uvicorn / pydantic / psycopg` 只有控制面服务器需要（`pip install .[server]`）。

### 旧 → 新

| 0.1.x (`ganglion.py`) | 2.0 |
|---|---|
| `State` 里的 requests 状态机、去重、`retry` | 控制面 Job/Attempt（`service.py`）；`retry` = `POST /v1/jobs/{id}/retry` |
| `compute_depth`（扫 inbox 算深度） | 控制面准入：`parent_job_id` + `depth` + `max_depth`，parent 必须已存在 |
| `scan_inbox` / `parse_frontmatter` / `poll_once` | `inbox.py`：Job source 之一 |
| `resolve_repo` / `create_repo` / `deliver_requirement` | `worker/repo.py`（全局仓库名 → Worker 本地路径） |
| `Tmux` / `prewrite_trust` / `trust_fallback` | `launchers/claude.py`（`ClaudeCodeLauncher`） |
| Stop hook 判定 DELIVERY.md 并通知 | Worker 的 `SpineAdapter.collect()` 判定；hook 只负责唤醒 Worker |
| `reconcile`（launching ↔ tmux） | `Worker.reconcile`：本地进程 × 控制面 attempt 两层对账 |
| `daemon_lock`、`state.lock` | `worker/state.py`（同样的 flock 方案） |
| 通知全部在本机 | 生命周期通知在控制面，操作性通知在 Worker（见第七节） |

## 三、状态机

### Job

```
            create ──► queued ◄──────────── release / retryable fail / lease 过期（预算内）
              │           │ claim（epoch+1，新 attempt）
              │           ▼
              │        running ──complete──► completed
              │           │
              │           └──不可重试 fail / 预算耗尽──► failed ──(手动 retry)──► queued
              ├──准入失败（深度、parent 缺失 / 被拒、非法名）──► rejected（终态，不可 retry）
              └──没有任何 Worker 映射该仓库 / new 缺 owner──► failed
```

- `running` 时 `current_attempt_id`、`epoch`、`lease_until` 共同标识唯一的权威 attempt。
- `failures` 统计过期与失败（release 不算），达到 `max_attempts` 即 `failed`。
- `target: manual` 且暂无 Worker 映射：仍是 `queued`，记 `job.waiting` 事件并通知一次；Worker 在 `[repos]` 补上映射后重新注册即可 claim。

### Attempt（从不复用、从不覆盖）

```
active ──complete──► completed
   ├────fail───────► failed
   ├────release────► released
   └────lease 过期──► expired   （由 claim、fenced 调用或 sweep 惰性触发）
```

### Worker 本地记录（`worker.json`，非权威）

```
preparing ─► launching ─► running ─► reporting ─► done
      任一步：控制面说不再是 owner ─► stale（停手，但不杀进程）
      claim 重放得到已结束的 attempt ─► dropped
```

每个副作用之前先落盘：claim 前先存 request id；起 tmux 前先记 `launching`；上报前先存报告和它的 request id。每个副作用都幂等，所以崩溃后"再做一遍"就是恢复。

## 四、Claim / Lease / Fencing / 幂等

**不变量**：一个 Job 可以有很多 attempt，但任一时刻至多一个权威 attempt；过期的 attempt 永远不能改动 Job 的权威状态。

1. **Claim 是一次条件更新。** 候选按 `created_at` FIFO 取出、在 Python 里做能力匹配，然后
   `UPDATE jobs SET status='running', epoch=epoch+1, current_attempt_id=:new, lease_until=:t ... WHERE id=:id AND status='queued' AND epoch=:seen`。
   影响行数为 1 才算赢，随后在同一事务里插入 attempt（`UNIQUE(job_id, epoch)` 兜底）。没有"先 SELECT 再无条件 UPDATE"。
   - PostgreSQL（READ COMMITTED）：并发 UPDATE 在行锁上等待，拿到锁后按已提交的新行重新判断 WHERE，所以只有一个匹配。16 个并发 claimer × 20 轮实测：每轮恰好一个赢家，共记录 249 次 `claim.conflict`（真实竞争，不是运气）。
   - SQLite：写事务以 `BEGIN IMMEDIATE` 开始，写者串行。
2. **Lease。** `jobs.lease_until` 是执行权期限，心跳续期（`lease_ttl` 默认 300s，建议心跳 30s）。过期的 lease 立即失效：owner 不能再心跳 / 上报，Job 可被重新 claim。两件事在同一个时间点切换，不存在"既可 claim 又仍权威"的窗口。
3. **Fencing。** 每个 attempt 变更（heartbeat / complete / fail / release）都带 `(attempt_id, worker_id, epoch)`，并且是一次条件更新：`WHERE status='running' AND current_attempt_id=:att AND epoch=:epoch AND lease_until >= now`。不满足就返回 `409 stale_attempt`（附 `reason`：`worker_mismatch`、`epoch_mismatch`、`not_current`、`lease_expired`、`expired`…），并记一条 `attempt.rejected` 事件。旧 Worker 猜到新 epoch、或拿新 attempt 的 id，同样会被拒。
4. **控制面重启补偿。** 控制面停机期间 Worker 没法心跳，这段时间不该算到它们头上。每次 sweep 都把 `last_alive` 写进数据库；启动时，只对"在 last_alive 时刻仍有效"的 lease 顺延**实际停机时长**。已经过期的 lease 不会复活，快速崩溃重启也只累加真实的不可用时间。这只会让 owner 保持 owner（epoch 不变），不会产生第二个权威 attempt。
5. **幂等。**
   - claim：`request_id` 唯一。响应丢失后用同一个 id 重试，拿到的是同一个 attempt（`replay: true`），不会再领第二个 Job。
   - complete / fail / release：同一 attempt 已经处于目标终态时，直接重放已记录的结果（即使 lease 早已过期）；与已记录结果矛盾（例如 fail 之后 complete）返回 `409 conflict`。条件更新未命中时会重读 attempt：与第一次请求重叠的重试（PostgreSQL 上两者都在等同一把行锁）得到的是重放，不是误报的 stale。
   - heartbeat：天然幂等。
   - 建 Job：`key` 唯一（`INSERT ... ON CONFLICT (key) DO NOTHING`），并发重复提交也只得到一个 Job。

测试里用假时钟覆盖了上面每一条，并在 PostgreSQL 上跑同一套用例（`tests/test_control_plane.py`）。

## 五、存储

表：`jobs`、`attempts`、`workers`、`events`、`meta(schema_version)`。DDL 在 `control/store.py`。

- 生产：PostgreSQL（单控制面实例即可；正确性不依赖内存状态，重启安全）。
- 单机与测试：SQLite（WAL，`BEGIN IMMEDIATE`）。
- 结果只存紧凑指针：`run_id`、`final_sha`、`branch`、`result_uri`、有界 `summary`（默认截断到 4000 字符）。日志和大产物不进数据库。
- 事件表 append-only：`job.created / job.claimed / attempt.started / attempt.heartbeat / attempt.expired / attempt.completed / attempt.failed / attempt.released / attempt.rejected / job.requeued / job.failed / job.completed / job.rejected / job.waiting / worker.registered / worker.offline / worker.online / result.received / claim.conflict / control.started`，每条带 `job_id / attempt_id / worker_id / epoch / ts`。`/v1/metrics` 从这些表推导，重启后数值不丢。
- 注意：每次心跳都写一条事件。按 30s 一次，每个运行中的 attempt 每天约 2900 行。MVP 可以接受，之后可以采样或定期归档。

## 六、Worker

```
start: 记录 python 路径 → register（带能力）→ reconcile
loop:  inbox sync（如配置）→ 配置变了就重新注册，否则 worker heartbeat
       → 每个本地 attempt 推进一步 → 有空位就 claim → 睡到下一轮或被 Stop hook 唤醒
```

**注册信息**：`platform`、`arch`、`launchers`（本机可用才报）、`capabilities`（git / gh / tmux / docker / codex / claude-code / agent-spine 是否存在）、`repos`（本机映射的仓库名）、`info`（ganglion 与 agent_spine 版本）。不上报主机名、用户名、路径。`worker_id` 自己配（默认短主机名）。

**调度**：FIFO + 基本匹配。launcher 必须匹配；`requirements.capabilities` 全部为真；有 `repository.url` 的 Job 任何有 git 的 Worker 都能做；没有 URL 的 Job 只给映射了该仓库名的 Worker；`target: new` 需要 gh（或已有该仓库）。不做打分、成本、亲和性。

**对账（两层，不混淆）**：注册响应带回"控制面认为你拥有的 attempt"。

- 本地活跃、控制面也列出：继续（进程在就观察；不在就幂等地重新准备并启动）。
- 本地活跃、控制面没列出：`stale`。不上报，不杀进程（除非 `stale_policy = "stop"`），通知一次。
- 控制面列出、本地没有记录（本地状态丢了）：按 tmux 会话环境里的 `GANGLION_ATTEMPT_ID` 找回会话并接管；找不到会话就重新准备并启动。
- 有带 `GANGLION_ATTEMPT_ID` 的会话，但哪边都不认识：记 `orphan_session`，不动它。
- 看不清（tmux 查不了、控制面连不上）：保持现状，只续租、不判定。

**先确认再启动**：准备工作区前、以及启动进程前各心跳一次。拿不到确认（控制面不可达）就不启动；被拒绝就转 `stale`。

**工作区隔离**：一个可写目录同一时间只给一个 attempt。
- `[repos]` 映射的仓库：默认 `isolation = "worktree"`，每个 attempt 一个 `git worktree`，分支 `ganglion/<job>-<attempt 前 8 位>`。用户自己的 checkout 不会被写入。`isolation = "shared"` 就在 checkout 里跑（0.1.x 布局），同一 Worker 对同一个 checkout 串行（claim 时带 `exclude_repos`）。
- 带 URL 的 Job：`<workspace_root>/cache/` 下的 mirror 做缓存（clone 或 `remote update`），每个 attempt 从它 `clone --reference --dissociate` 出独立目录。
- `target: new`：`gh repo create` + `gh repo clone`（远端已存在就直接用）；新仓库没有提交可分支，所以就地运行（shared）。
- Spine 在工作区内部建自己的 worktree，Ganglion 不碰。
- 工作区不会被自动删除，里面可能有没发布的工作。

**发布结果**：`publish = "branch"` 时，完成后把 attempt 分支推到 origin，`result_uri = <remote>#<branch>`。这样结果不再依赖这台机器。

## 七、Agent Spine adapter 与 Claude Code launcher

**当前 Spine（1.9.1）没有面向外部编排的接口**：没有 job_id / attempt_id / run_id 入参，没有结构化结果文件；运行状态在 `~/task_log/<PROJ_KEY>/` 下，属于 Spine 内部。因此：

- 启动方式不变：交互式 `claude "/spine-run --auto …"`（prompt 可配置），环境变量额外导出 `GANGLION_JOB_ID / GANGLION_ATTEMPT_ID / GANGLION_EPOCH / GANGLION_RESULT_FILE`（以及 0.1.x 的 `GANGLION_ID`、`GANGLION_STATE`）。
- 结果识别集中在 `SpineAdapter.collect()`，版本相关逻辑不外溢：
  - **结构化结果**（给 Spine 2.x 的契约）：`$GANGLION_RESULT_FILE` 里的 `{"schema":"ganglion.engineering-result/v1","status",…,"run_id","final_sha","summary","result_uri"}`；`status` 为 failed 时 attempt 以 `spine_failed` 失败。
  - **DELIVERY.md**（兼容当前 Spine）：launch prompt 要求 Agent 收尾时在工作区根目录写它；只认启动之后写的；前 20 行作为 summary，`final_sha` 取工作区 HEAD。
  - `spine_protocol = "auto"`（默认，结构化优先，缺失则回退 DELIVERY.md）、`"1"`、`"2"`。
- 控制面只收到紧凑结果，从不读取 Spine 文件。

以下 0.1.x 发现的问题仍然成立，处理方式不变：spine-run 本身不写 DELIVERY.md（由 prompt 要求）；子 agent 会继承环境并触发 Stop hook（hook 现在只唤醒 Worker，判定按记录的工作区路径做，天然去重）；spine-run 需要 `worktree.baseRef=head`、npc / openspec 等依赖（部署文档里说明）；非 `--auto` 会卡在 AskUserQuestion（默认带 `--auto`）；新建仓库为空（需求提交成为第一个 commit）。

**ClaudeCodeLauncher**（`prepare / start / after_start / is_alive / stop / discover`）保留了 0.1.x 的全部生产细节：
- 用 `list-sessions` 精确比较会话名，`=name:` 作 target。`has-session -t gl-a` 会前缀匹配到 `gl-ab`。
- trust 预写：合并写入、保留文件权限、遇到半截 JSON 退避重试、绝不覆盖解析不了的文件、同时写 absolute 和 realpath 两个 key。
- trust 兜底：在 `trust_wait` 秒内轮询屏幕，看到 trust 提示就发一次 Enter。
- 用 `shlex.join` 拼命令；launchd 的 PATH 处理；`AbandonProcessGroup`。
- 会话名 `gl-<job>-<attempt 前 8 位>`，每个 attempt 唯一。身份以会话环境变量 `GANGLION_ATTEMPT_ID` 为准，不解析名字。

**Stop hook**：永远 `exit 0`、不输出。0.2.x 会话只 touch `<state_dir>/wake` 唤醒 Worker，由 Worker 判定、上报。0.1.x 启动的会话（只有 `GANGLION_ID`）保持 0.1.x 行为：DELIVERY.md → 改 0.1.x 状态 → 通知一次。

## 八、通知

通知只是展示层，失败只记日志。为了不重复发送，按"谁知道这件事"分工：

| 由控制面发（每次已提交的状态转换恰好一次） | 由 Worker 发 |
|---|---|
| 🚫 被拒绝、⏳ 等待仓库映射、✅ 已交付（含 summary）、❌ 失败 | 🚀 已启动（带本机 attach 命令）、⚠️ 执行权已转移（stale） |

单机模式下两边在同一进程，用同一个 `[notify]` 配置。Inbox 里格式错误的文件由 inbox adapter 通知，每个文件版本只通知一次。

## 九、配置、认证、信任模型

- 配置分三层：`[worker]/[claude]/[repos]`（本机：身份、URL、工作区、launcher、凭据文件路径）；`[control]`（数据库、监听、token、lease 策略）；Job 字段（仓库名 / URL / ref / owner、goal、launcher、requirements、max_attempts、timeout）。Job 里不放凭据：git / gh / claude 的登录态都留在 Worker 本机。
- 认证：静态 bearer token，从文件或 `GANGLION_API_TOKEN` 读取，不进 git，用 `hmac.compare_digest` 比较。没有 token 服务器拒绝启动；`--insecure-no-auth` 只允许绑定 loopback。
- 信任模型：持有 token 的一方被完全信任，可以提交 Job，也可以以任意 `worker_id` 身份调用。fencing 防的是陈旧和并发，不防恶意内部人员。控制面默认只监听 127.0.0.1，远程 Worker 应通过 TLS 反向代理或 VPN / SSH 隧道访问。RBAC、每个 Worker 一个凭据、多租户留到以后。

## 十、故障语义

| 情况 | 行为 |
|---|---|
| claim 响应丢失 | request id 先落盘，重试同一个 id → 同一个 attempt（`replay`），不会双重持有 |
| Worker 在 claim 后死亡 | lease 到期 → Job 回到可 claim（`attempt.expired`，计入 failures）→ 另一 Worker 用新 epoch 接手 |
| Worker 做了有用的本地工作后死亡 | **MVP 可能丢失未发布的本地工作。** 新 attempt 从仓库重新开始；旧工作区还在旧机器上。`publish = "branch"` 只在完成时推送，Spine 内部状态不做分布式恢复 |
| Worker 进程崩溃，Claude/tmux 还在 | 重启后 reconcile 接管会话（lease 还在的话），不重复启动 |
| 旧 Worker 在重新分配后回来 | 注册响应里没有它的 attempt → 本地标 stale，不上报。即使它上报，fencing 也会拒绝 |
| 控制面重启 | 状态都在数据库里；启动时补偿 lease；Worker 的调用在停机期间得到 `unavailable`，它们继续干活并重试 |
| 数据库 / 网络瞬时故障 | 统一为 `503 unavailable`：不标失败、不杀进程；Worker 保留待上报的结果，稍后用同一个 request id 重试 |
| launcher 意外退出 | 没有结果：以 `launcher_exited` 失败（默认不可重试，`retry_on_launcher_exit = true` 则重排）。有结果：照常完成 |
| 上报的结果被服务端拒收（422，例如字段超长） | 不会无限重发：改为以 `report_rejected` 失败上报。结构化结果的每个字段在 Worker 端先截断 |
| shared checkout 里还有 stale 会话在跑 | 这个 checkout 继续算作忙碌，新 attempt 不会进来（否则旧会话写的 DELIVERY.md 会被当成新 attempt 的结果） |
| Agent 完成但上报失败 | 报告连同 request id 写进 `worker.json`，跨 Worker 重启重试，等待期间继续续租；服务端幂等重放；`report_attempts` 计入 `result_report_retries` |
| 超时（`timeout_seconds`） | 停止会话，以 `timeout` 失败，不可重试 |
| lease 过期期间无人接手，owner 恢复 | 仍被拒绝（过期即失效）。Worker 标 stale，Job 由下一次 claim 重新执行 |

## 十一、与 0.1.x 的兼容与迁移

- 0.1.x 配置文件（`[ganglion]` 表）原样可用，映射为单机模式：SQLite 在 `state_dir/control.db`，`max_attempts = 1`（0.1.x 从不自动重启），`max_concurrent = 8`。
- `ganglion.py --config … run` 仍然可用，旧 launchd plist 不用改。`hooks/stop.sh` 路径不变。
- `~/.ganglion/state.json` **不会**被当作权威状态导入。它只作兼容读取：0.1.x 已处理过的需求（launched / delivered / rejected / error）inbox 不会再提交；`ganglion retry <id>` 会清掉对应的 0.1.x 记录（和 0.1.x 的 retry 一样），之后作为新 Job 提交。0.1.x 启动的会话由 0.1.x 语义的 Stop hook 收尾。
- 主动改变的行为（都有测试覆盖）：
  - 默认每个 attempt 在自己的 worktree 里运行，不再直接写用户 checkout。要 0.1.x 的布局就设 `isolation = "shared"`（同 checkout 串行）。
  - 会话结束但没有结果，会记成失败的 attempt（0.1.x 永远停在 launched）。仍然不会自动重启。
  - 非法 id 记为 `rejected`（0.1.x 记为 error）。
  - 状态查看用 `ganglion status`，不再是 state.json。
- `events.jsonl` 保留为 Worker 本地诊断日志；审计以控制面 events 表为准。

## 十二、已知限制

- 单控制面实例（PostgreSQL 权威、重启安全，但没有 HA）。多实例也能保证正确性（全部是条件更新），但没有测过，sweep 会重复运行（无害）。
- Agent Spine 内部状态不做分布式恢复；未发布的工作随机器丢失。
- Worker 主循环单线程：长时间 clone 或 trust 等待会推迟其它 attempt 的心跳（lease 远大于这些耗时，默认配置下安全）。
- 结构化结果只是契约：当前 Spine 不会写，实际仍走 DELIVERY.md。
- 同一个 token 可以冒充任意 Worker（见信任模型）。
- 工作区和 clone 缓存不会自动清理。
- 心跳事件量随运行时长线性增长。

## 十三、下一步

1. 与 Agent Spine 约定并实现 `GANGLION_RESULT_FILE`（Spine 2.x），把 run_id / 最终状态接进来，逐步淘汰 DELIVERY.md。
2. 每个 Worker 独立凭据（token → worker_id 绑定），服务端校验身份。
3. 可选的周期性发布（中途推 WIP 分支），缩小"Worker 死亡丢工作"的窗口。
4. 工作区 GC（只清理 Job 已终结、分支已推送的工作区）。
5. 事件归档 / 心跳采样；Prometheus 指标补齐直方图。
6. 更多 launcher（Codex、headless `claude -p`），前提是 Spine 侧支持。
