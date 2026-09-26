# Ganglion

agent-spine 的自动启动器。你把需求写成 Markdown 推到 inbox 仓库，Ganglion 自动在目标工程里用 tmux 拉起一个**交互式** Claude Code session，执行 `/spine-run`；Agent 在工程根目录写出 `DELIVERY.md` 后通知你。

它只替你做这几件机械的事：`cd`、放需求文件、启动 `claude`、敲启动语句。它不含任何 Agent 逻辑。

```
手机 ──push──▶ inbox 仓库 ──git pull──▶ ganglion.py ──tmux new-session──▶ claude "/spine-run …"
                                             │                                   │ Stop hook
                                             └── ntfy「已启动」                   └── hooks/stop.sh ── ntfy「已交付」
```

交付物：

| 文件 | 作用 |
|---|---|
| `ganglion.py` | 主程序（只用标准库，Python 3.11+） |
| `hooks/stop.sh` | Claude Code Stop hook |
| `config.example.toml` | 配置示例，每一项都有注释 |
| `deploy/com.ganglion.plist` | launchd 用户代理（KeepAlive，用 `caffeinate -i` 包住） |
| `DESIGN.md` | 设计取舍，以及和 agent-spine 对接时发现并处理的问题 |
| `tests/` | 自动化测试（Linux 可跑） |

## 首次部署（macOS）

前提：`claude`、`gh`、`git` 已在这台机器上登录好，并且装了 agent-spine：

```bash
uv tool install --force --from git+https://github.com/winewei/agent-spine.git npc
npc playbook install --host claude    # 安装 /spine-run
npc doctor                            # spine-run 的前置检查也会跑它
```

### 1. 安装依赖并获取 Ganglion

```bash
brew install tmux python@3.12
git clone <本仓库> ~/ganglion
```

### 2. 准备 inbox 仓库

在 GitHub 上建一个私有仓库（例如 `ganglion-inbox`），放一个 README 后 clone 到本机：

```bash
gh repo create <你>/ganglion-inbox --private --add-readme
git clone https://github.com/<你>/ganglion-inbox ~/ganglion-inbox
```

Ganglion 只对这个 clone 做 `git pull --ff-only`，从不往里写。

### 3. 写配置

```bash
mkdir -p ~/.config/ganglion
cp ~/ganglion/config.example.toml ~/.config/ganglion/config.toml
$EDITOR ~/.config/ganglion/config.toml
```

至少要改：`inbox_dir`、`owner`、`ntfy_topic`、`claude_cmd`（填 `which claude` 的绝对路径），并在 `[repos]` 里写上现有工程。

手机上装 ntfy App，订阅同一个 topic。

### 4. 挂 Stop hook

把下面内容合并到 `~/.claude/settings.json`（已有 `hooks` 的话合并进去，别覆盖）。把路径换成你的 clone 位置，必须是绝对路径：

```json
{
  "hooks": {
    "Stop": [
      {
        "hooks": [
          { "type": "command", "command": "/Users/<你>/ganglion/hooks/stop.sh", "timeout": 30 }
        ]
      }
    ]
  }
}
```

环境里没有 `GANGLION_ID` 时这个 hook 什么都不做，所以不影响你平时手动用 Claude Code。

agent-spine 需要 worktree 从 HEAD 开：在同一个文件里加上 `"worktree": { "baseRef": "head" }`（见 spine-run 的 Step 0）。

### 5. 先前台试一轮

```bash
python3.12 ~/ganglion/ganglion.py --config ~/.config/ganglion/config.toml once
python3.12 ~/ganglion/ganglion.py --config ~/.config/ganglion/config.toml status
```

### 6. 装成 launchd 服务

```bash
cd ~/ganglion
sed -e "s#__GANGLION_DIR__#$HOME/ganglion#g" \
    -e "s#__PYTHON__#$(command -v python3.12)#g" \
    -e "s#__HOME__#$HOME#g" \
    -e "s#__CONFIG__#$HOME/.config/ganglion/config.toml#g" \
    deploy/com.ganglion.plist > ~/Library/LaunchAgents/com.ganglion.plist
plutil -lint ~/Library/LaunchAgents/com.ganglion.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.ganglion.plist
```

日志在 `~/Library/Logs/ganglion.log`。卸载：`launchctl bootout gui/$(id -u)/com.ganglion`。

改了 PATH 以外的配置不用重启，每轮都会重新读 `config.toml`。

### 7. 看任务

```bash
tmux ls                          # 每个需求对应一个 gl-<id> session
tmux -CC attach -t gl-<id>       # iTerm2 原生窗口
```

## 从手机提交需求

一个 `.md` 文件就是一条需求，**文件名就是需求 id**（例如 `add-rate-limit.md` → `gl-add-rate-limit`）。id 里不能有空白、`.`、`:`、引号，也不能以 `-` 开头，因为 tmux 的 session 名不接受这些字符。

```markdown
---
target: existing        # existing | new | manual
repo: my-app            # existing/manual：对应 [repos] 里的名字；new：要新建的仓库名
source: human           # human（默认）| agent
parent: some-req-id     # 可选；source: agent 时用来算深度
---

给登录接口加限流：同一 IP 每分钟最多 10 次……
（正文原样投递给 Agent，Ganglion 不做解析）
```

- `existing`：从 `[repos]` 里找本地路径。
- `new`：执行 `gh repo create <owner>/<repo> --private` 并 clone 到 `new_repo_root`，映射会记进状态文件，后续需求可以继续用 `target: new` 或 `manual` 引用同名仓库。
- `manual`：先标记为 `waiting_repo` 并通知你。你在 `[repos]` 里补上映射后，下一轮自动继续，不用重启。

提交方式任选：

- **GitHub 网页 / App**：打开 inbox 仓库 → Add file → Create new file → 文件名写 `<id>.md` → Commit to `main`。如果 GitHub App 里找不到新建文件的入口，就用手机浏览器打开 github.com 操作。
- **iOS 快捷指令**：用 “Get Contents of URL” 调 `PUT https://api.github.com/repos/<你>/ganglion-inbox/contents/<id>.md`，body 为 `{"message":"add <id>","content":"<base64 正文>"}`，Header 带 `Authorization: Bearer <fine-grained token，只给这个仓库 Contents 读写>`。
- **Working Copy 等 git 客户端**：正常 commit + push。

之后手机会依次收到：「🚀 已启动」→（Agent 写出 `DELIVERY.md`）「✅ 已交付」，内容是 `DELIVERY.md` 的前 20 行。被拒绝、等待映射、出错也会各通知一次。

出错的需求修好原因后，执行 `ganglion.py --config … retry <id>`，下一轮会重新处理它。

## 运行测试

```bash
python3 -m unittest discover -s tests -v
```

需要 Linux 或 macOS，装有 `git`、`tmux` ≥ 3.0 和 Python 3.11+。测试用本地 bare repo 模拟 inbox，用 `tests/fakes/claude` 和 `tests/fakes/gh` 冒充 CLI。每个用例使用独立的 tmux socket，不会碰到你真实的 `~/.claude.json` 和 tmux server。

覆盖的场景：existing 启动、manual 等映射后启动、不重复启动（包括 session 结束后）、kill -9 后重启对账（以及“launching 中途崩溃”和“状态文件丢失”两种情况）、单实例锁、`MAX_DEPTH` 拒绝（以及 parent 缺失、parent 成环）、trust 兜底、`~/.claude.json` 合并写入（保留字段、文件权限、并发写入时的重试、遇到无法解析的文件时不覆盖）、Stop hook（无 env 时 no-op、只在有 `DELIVERY.md` 时通知且只通知一次、忽略旧的 `DELIVERY.md`、用假 claude 跑端到端）、new 路径的 gh 命令、单条需求出错不影响其它需求、tmux session 名的前缀误匹配。

## 手工验证（沙箱里测不了的部分）

**launchd + KeepAlive**

1. `launchctl print gui/$(id -u)/com.ganglion | grep -E 'state|pid'`，应显示 `state = running`。
2. `kill -9 <pid>`，30 秒内再执行上一条命令，pid 应该变了。
3. 杀进程前若有 `gl-*` session 在跑，`tmux ls` 里它们应该都还在，这是 `AbandonProcessGroup` 在起作用。日志里应看到 `reconciled` 或没有新的 `launching`，不会重复启动。
4. 注销后重新登录，服务会自动起来（`RunAtLoad`）。

**caffeinate**

1. `pmset -g assertions | grep -i caffeinate`，应看到 `PreventUserIdleSystemSleep` 由 caffeinate 持有。
2. 把“系统设置 → 锁定屏幕 → 不活跃时关闭显示器”调短，等待息屏后用手机提交一条需求，应能收到「已启动」。
3. `launchctl bootout` 之后，上面的 assertion 应该消失。

**真 claude 冒烟测试**

1. 在一个从没用 claude 打开过的新工程上提交一条 `existing` 需求。
2. `tmux -CC attach -t gl-<id>`：应直接看到 `/spine-run …` 已经在执行，而不是停在 trust 对话框上。
3. `~/.ganglion/events.jsonl` 里若出现 `trust_fallback`，说明预写没生效、是兜底回车救回来的。这时请检查 `~/.claude.json` 里的项目 key 是否和工程的真实路径一致。
4. 让 Agent 跑完（或在 session 里手动 `echo ok > DELIVERY.md` 后让 claude 停一次），应收到一次「已交付」。

## 已知限制

- 交互式 claude 仍会按 `~/.claude/settings.json` 的权限规则弹确认框，需要你 attach 进去批准。想减少打断，可以在工程或全局 settings 里配 `permissions.allow`，或者用 `claude_args = "--permission-mode acceptEdits"`。
- 完成判定只看 `DELIVERY.md`。spine-run 自己不会写这个文件，是靠 `launch_prompt` 要求它写的，详见 `DESIGN.md`。
