# Ganglion 0.2

一个小型的**分布式控制面 + 一次性 Worker 运行时**：接收工程 Job，把它安全地分给某台 Worker；Worker 准备仓库和环境，在 tmux 里以交互模式拉起 Claude Code 执行 [Agent Spine](https://github.com/winewei/agent-spine) 的 `/spine-run --auto`，持有并续租执行权，最后上报结果。

Ganglion 不含任何软件工程智能。规划、实现、review、修复、测试、集成都由 Agent Spine 负责。Ganglion 只负责 Job、Worker、执行权和机器生命周期。

```
 手机/CLI/REST ──► 控制面 (FastAPI + PostgreSQL) ◄──HTTP──  Worker(macOS)  Worker(Linux) …
 Git inbox ─(inbox adapter)─┘   jobs / attempts / lease / epoch      │ 准备仓库 → tmux → claude "/spine-run --auto …"
                                                                     │ 心跳 → 识别结果 → 上报
```

- **单机用户什么都不用改**：0.1.x 的配置文件和 `ganglion.py … run` 照常可用，控制面内嵌在 Worker 进程里（SQLite），不需要服务器，也不需要 PostgreSQL。
- **多机**：部署一个控制面（`docker compose`），每台机器跑 `ganglion worker start`。

设计、状态机和故障语义见 [DESIGN.md](DESIGN.md)。HTTP 协议以 [openapi.json](openapi.json) 为准（由代码生成，测试校验一致）。

| 文件 | 作用 |
|---|---|
| `ganglion/` | 实现：`control/`（控制面）、`worker/`、`launchers/`、`spine.py`、`inbox.py`、`cli.py` |
| `bin/ganglion` | 免安装入口（`ganglion.py` 是给 0.1.x 部署保留的兼容入口） |
| `hooks/stop.sh` | Claude Code Stop hook |
| `config.example.toml` / `config.v0.1.example.toml` | 0.2.x 配置示例 / 0.1.x 配置格式（仍受支持） |
| `openapi.json` | API 契约 |
| `Dockerfile`、`deploy/docker-compose.yml`、`deploy/control.toml` | 控制面部署 |
| `deploy/com.ganglion.plist`、`deploy/ganglion-worker.service` | Worker：launchd（macOS）/ systemd（Linux） |

## 前提（每台 Worker）

`git`、`tmux` ≥ 3.0、Python 3.11+，登录好的 `claude`（可选：`gh`，用于 `target: new`），以及 Agent Spine：

```bash
uv tool install --force --from git+https://github.com/winewei/agent-spine.git npc
npc playbook install --host claude    # 安装 /spine-run
npc doctor
```

在 `~/.claude/settings.json` 里加上 `"worktree": { "baseRef": "head" }`（spine-run 的要求），并挂上 Stop hook（路径必须是绝对路径）：

```json
{ "hooks": { "Stop": [ { "hooks": [ { "type": "command", "command": "/Users/<你>/ganglion/hooks/stop.sh", "timeout": 30 } ] } ] } }
```

环境里没有 `GANGLION_ID` 时 hook 什么都不做，不影响你平时手动用 Claude Code。

## 单机（一台 Mac）

0.1.x 用户：`git pull` 即可。旧配置、旧 plist、旧 hook 路径都不用改。迁移细节见 DESIGN.md 第十一节。

新用户：

```bash
git clone <本仓库> ~/ganglion
mkdir -p ~/.config/ganglion && cp ~/ganglion/config.example.toml ~/.config/ganglion/config.toml
$EDITOR ~/.config/ganglion/config.toml     # [worker] id、[claude] claude_cmd、[repos]、[inbox]、[notify]
~/ganglion/bin/ganglion --config ~/.config/ganglion/config.toml once      # 前台跑一轮
~/ganglion/bin/ganglion --config ~/.config/ganglion/config.toml status
```

装成 launchd 服务：

```bash
cd ~/ganglion
sed -e "s#__GANGLION_DIR__#$HOME/ganglion#g" -e "s#__PYTHON__#$(command -v python3.12)#g" \
    -e "s#__HOME__#$HOME#g" -e "s#__CONFIG__#$HOME/.config/ganglion/config.toml#g" \
    deploy/com.ganglion.plist > ~/Library/LaunchAgents/com.ganglion.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.ganglion.plist
```

日志在 `~/Library/Logs/ganglion.log`。Worker 重启不会杀掉正在运行的会话（`AbandonProcessGroup`），重启后会重新接管。

## 多机部署

### 1. 控制面

```bash
mkdir -p deploy/secrets && openssl rand -hex 32 > deploy/secrets/api_token
echo "POSTGRES_PASSWORD=$(openssl rand -hex 16)" > deploy/.env
docker compose -f deploy/docker-compose.yml --env-file deploy/.env up -d --build
curl -s 127.0.0.1:8700/healthz
```

`deploy/secrets/` 和 `deploy/.env` 已被 git 忽略。API 只发布在 127.0.0.1。远程 Worker 访问前，先在前面放 TLS 反向代理，或者走 VPN / SSH 隧道（`ssh -L 8700:127.0.0.1:8700 host`）。不用容器也可以：`pip install '.[server]'`，然后 `ganglion --config control.toml server start`。

### 2. 每台 Worker

把同一个 token 放到 Worker 本机（比如 `~/.config/ganglion/token`，权限 600），配置：

```toml
[worker]
id = "linux-vm-01"
control_plane = "https://ganglion.internal:8700"
token_file = "~/.config/ganglion/token"
max_concurrent = 2
publish = "branch"        # 完成后把 attempt 分支推到 origin，结果不依赖这台机器

[claude]
claude_cmd = "/home/me/.local/bin/claude"

[repos]                   # 可选：本机已有的 checkout（仓库名是全局身份，路径只在本机有效）
my-app = "~/code/my-app"
```

- macOS：用上面的 launchd plist。
- Linux：`cp deploy/ganglion-worker.service ~/.config/systemd/user/ && systemctl --user enable --now ganglion-worker`。unit 里的 `KillMode=process` 很重要：否则 Worker 重启时 systemd 会连带杀掉 tmux 里的 agent。
- 容器 / 前台：直接 `ganglion --config … worker start`。

`[inbox]` 只需在一台机器上配置（它把 inbox 文件转成控制面 Job；多配几台也只是重复提交，Job 不会重复）。

## 提交 Job

**Git inbox**（和 0.1.x 一样）：一个 `<id>.md` 就是一条需求，文件名就是 Job 名。

```markdown
---
target: existing        # existing | new | manual
repo: my-app            # 仓库名（全局身份）
repo_url: git@github.com:me/my-app.git   # 可选：有 URL 时任何 Worker 都能 clone
source: human           # human（默认）| agent
parent: some-req-id     # 可选；agent 需求用它计算深度
---
需求正文（整个文件原样投递到 <工作区>/docs/requirements/<id>.md）
```

- `existing`：交给映射了该仓库名（或能 clone 其 URL）的 Worker。
- `new`：`gh repo create <owner>/<repo> --private` 后 clone。
- `manual`：等某个 Worker 在 `[repos]` 里映射该仓库后自动开始，期间通知一次。

id 里不能有空白、`.`、`:`、引号、`/`、`$`，也不能以 `-` 开头（它会成为 tmux 会话名）。

**CLI / REST**：

```bash
ganglion job submit --repo my-app --url git@github.com:me/my-app.git --name add-rate-limit --goal-file req.md
ganglion job status add-rate-limit     # 也可以用 job id 或 key（例如 inbox:add-rate-limit）
ganglion job list --status running
ganglion job events <job>
ganglion retry <job>                   # failed → queued；rejected 不能重试（防繁殖）
ganglion status                        # 本机 attempt + 最近的 Job
ganglion metrics
```

手机上依次收到：🚀 已启动（带 `tmux -CC attach -t gl-<id>-<attempt>`）→ ✅ 已交付（结果摘要）。被拒绝、等待映射、失败、执行权转移也会各通知一次。

## 手工验证：两台 Worker

自动化版本见 `tests/test_distributed.py`（真实进程、HTTP、PostgreSQL）。在真实机器上：

1. 按上文启动控制面，准备两台 Worker A（Mac）和 B（Linux），`lease_ttl` 可以临时设成 60 方便观察。
2. 两台都 `ganglion worker start`，然后 `curl -H "Authorization: Bearer $T" …/v1/workers` 能看到两台 `online`。
3. 提交一个带 `--url` 的 Job。`ganglion job status <name>` 里 `attempts` 只有一条，`worker_id` 是其中一台。只有那台机器上有 `gl-<name>-…` 会话。
4. 让它跑完：状态变为 `completed`，`result.engineering_result` 里有 `final_sha`、`summary`，开了 `publish` 的话还有分支。
5. 再提交一个 Job，等它进入 running 后，在持有它的机器上 `kill -9` Worker 进程（claude 会话保留）。
6. 超过 `lease_ttl` 后，另一台 Worker 以 `epoch 2` 接手：`attempts` 变成 `[expired, active]`。
7. 重启第一台 Worker：它的日志出现 `stale … left running for inspection`，手机收到 ⚠️ 执行权已转移，Job 的 `current_attempt_id` 不变。旧会话里就算写出了 DELIVERY.md，也不会被上报。
8. `ganglion job events <name>` 里能看到 `attempt.expired`、`job.claimed(epoch 2)`，以及旧 Worker 被拒绝的调用（`attempt.rejected`，如果它上报过的话）。

## 运行测试

```bash
python3 -m unittest discover -s tests -v                       # SQLite
GANGLION_TEST_PG_DSN=postgresql://postgres@127.0.0.1:5432/ganglion_test \
  python3 -m unittest discover -s tests -v                     # 再加 PostgreSQL
```

需要 Linux 或 macOS、`git`、`tmux` ≥ 3.0、Python 3.11+；API / 端到端测试还需要 `pip install '.[server]'`（没装会跳过）。claude 和 gh 用 `tests/fakes` 下的假脚本代替，不会调用任何 LLM。每个用例使用独立的 tmux socket 和临时目录，不会碰你真实的 `~/.claude.json`、`~/.ganglion` 或 tmux server。改了 API 之后要重新生成契约：`bin/ganglion server openapi > openapi.json`。

| 文件 | 覆盖 |
|---|---|
| `test_control_plane.py` | claim 竞争（单 Job、多 Job）、丢失 claim 响应、FIFO 与能力匹配、心跳（owner / 非 owner）、lease 过期与重新 claim、fencing（epoch 7/8 场景的全部操作）、完成 / 失败 / 释放的幂等、Job key 幂等（含并发）、重试预算、深度 / parent / 被拒 parent、准入、重启持久化、重启补偿、指标、结果紧凑 |
| `test_api.py` | 认证（缺失 / 错误 token、无 token 拒绝启动）、HTTP 下的完整周期与 fencing、错误映射、服务器重启、OpenAPI 与实现一致 |
| `test_worker.py` | 两 Worker 竞争、clone 工作区与结果分支发布、陈旧的本地进程永不上报（运行中 / 重启时两条路径）、`stale_policy=stop`、重启接管（有 / 无本地状态）、丢失的 claim / complete 响应、上报跨崩溃重试、控制面不可达不影响工作、未确认执行权不启动、结构化结果 / 失败 / 协议选择、launcher 退出重试、超时、shared 串行与 worktree 并发、映射消失时 release |
| `test_local_compat.py` | 0.1.x 配置下的全部 0.1.x 行为（启动、manual 等映射、不重复启动、会话名精确匹配、错误隔离、非法 id、retry、深度 / 环、trust 预写与兜底、`~/.claude.json` 合并、Stop hook、端到端、`new` 仓库、kill -9 重启、单实例、两个崩溃点、状态丢失接管、ntfy、解析），以及 0.1.x 状态兼容、0.1.x 会话的 hook、`ganglion.py` 入口 |
| `test_distributed.py` | 控制面进程 + 两个 Worker 进程：竞争、完成、Worker 崩溃 → lease 过期 → 重新分配 → 旧 Worker 回来被隔离、控制面崩溃重启 |

## 已知限制

见 DESIGN.md 第十二节。最重要的一条：Worker 死亡时，它本地尚未发布的工作会丢失，新 attempt 从仓库重新开始。Ganglion 做的是分布式调度，不是 Agent Spine 执行状态的分布式恢复。
