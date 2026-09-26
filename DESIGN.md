# Ganglion 设计说明

需求里没写死、需要做选择的地方，都记在这里。每一条都给出选择和理由。

## 一、对照 agent-spine 验证时发现的问题及处理

我读了 agent-spine（`src/npc/playbooks/spine-run.md`、仓库自带的 `.claude/settings.json`），逐条检查了“Ganglion 拉起 → spine-run 执行 → DELIVERY.md → 通知”这条链路能不能走通。

| # | 问题 | 后果 | 处理 |
|---|---|---|---|
| 1 | spine-run 从头到尾**不会写 `DELIVERY.md`**。它的收尾产物是 `~/task_log/<proj>/<run>/run-summary.md`，外加对话里的一段汇报。 | Stop hook 永远不会通知“已交付”。 | 默认的 `launch_prompt` 明确要求 Agent 在收尾时于工程根目录写 `DELIVERY.md`，并把结论摘要写在前 20 行。 |
| 2 | spine-run 会用 `claude -p` 或原生子 agent 作为 coder / reviewer，这些子进程会继承 `GANGLION_ID`，也会触发 Stop hook，而且 cwd 可能在 worktree 里。 | 如果按 hook 进程的 cwd 去找 `DELIVERY.md`，会找错地方，也可能重复通知。 | hook 按状态文件里记录的工程路径（`requests[id].project`）查找 `DELIVERY.md`，只有找不到时才退回 `CLAUDE_PROJECT_DIR` 或 cwd。去重在 `flock` 保护下完成，多个进程同时触发也只会通知一次。 |
| 3 | spine-run 需要 `worktree.baseRef=head`，还需要 `npc`、`openspec`、review engine 这些依赖。 | 缺任何一个，Agent 都会在 Step 0 停下并说明原因。 | 这些不在 Ganglion 的职责内（它不做 Agent 逻辑）。README 的部署步骤里写了要装什么、要配什么。 |
| 4 | 非 `--auto` 模式下，spine-run 会在决策点调用 AskUserQuestion。 | 无人值守时会一直卡住。 | 默认 prompt 带 `--auto`。想走交互模式，改 `launch_prompt` 即可。 |
| 5 | `target: new` 新建的仓库是空的，没有任何 commit。 | spine-run 要求在一个有名字的分支上。 | 投递需求时的 `req: <id>` 会成为这个仓库的第一个 commit，分支也就有了。`openspec/` 目录由 spine-run 拆解目标时自己处理，Ganglion 不管。 |

**关于工作区是否干净。** spine-run 的 Step 0 要求工作区干净（`npc git ensure-clean` 连未跟踪文件也算脏）。在 Ganglion 的实际流程里，这个条件天然满足：流水线先创建并提交需求（`req: <id>`），得到的是一个全新、干净的工程目录，然后才在里面用 tmux 启动 claude、执行 skill。所以不存在“工作区不干净”或“同一目录残留上一条需求的 `DELIVERY.md`”的问题。Stop hook 里仍保留了一个很便宜的防御：只认修改时间不早于本次启动时间的 `DELIVERY.md`。在全新目录里它永远不会生效，只在有人手工复用目录时起作用。

## 二、实现过程中发现的问题（都有测试覆盖）

- **tmux 的前缀匹配。** `tmux has-session -t gl-a` 在 `gl-a` 不存在时，会匹配到 `gl-ab`。结果是 `gl-a` 永远不会被启动。解决办法是改用 `list-sessions` 做精确比较，给 `capture-pane` 和 `send-keys` 传目标时加 `=` 前缀。对应测试：`test_session_prefix_is_exact`。
- **trust 观察窗口里的竞态。** 启动后主循环要观察几秒屏幕、等可能出现的 trust 提示。如果 Agent 很快就写出 `DELIVERY.md` 并停下，Stop hook 会先把状态写成 `delivered`，主循环随后又把它覆盖回 `launched`。现在主循环只会把状态从 `launching` 推进到 `launched`，不会让状态倒退。这个问题是端到端测试抓出来的。
- **ntfy 的中文标题。** 用 `Title:` 这个 HTTP header 传中文，urllib 会按 latin-1 编码，直接抛异常。现在改用 ntfy 的 JSON 发布接口，标题放在 body 里。
- **Stop hook 用错 Python。** hook 环境里的 `python3` 在 macOS 上可能是系统自带的 3.9，没有 `tomllib`。主进程启动时会把 `sys.executable` 写到 `<state_dir>/python`，`stop.sh` 优先用这个解释器。
- **launchd 环境。** launchd 下 PATH 太短，找不到 Homebrew 的 tmux / gh 和 `~/.local/bin` 里的 claude。另外 tmux server 如果是 Ganglion 首次拉起的，claude 会话的环境也继承自这里。plist 里显式设置了 `PATH`、`HOME` 和 UTF-8 locale，并加上 `AbandonProcessGroup`，Ganglion 重启时不会连带杀掉正在工作的会话。
- **`~/.claude.json` 是 JSON 但解析不了**（比如另一个进程写到一半）。这时会退避重试，重试期间绝不覆盖原文件。一直解析失败的话，就把这条需求记为 error。
- **路径里有符号链接**（比如 macOS 上 `/tmp` 实际是 `/private/tmp`）。trust 同时写入 `absolute()` 和 `realpath()` 两个 key，claude 无论用哪个路径查都能命中。

## 三、设计选择

**状态机。** 每条需求的状态按这个顺序推进：`(未记录) → waiting_repo → launching → launched → delivered`，另有两个旁支终态 `rejected` 和 `error`。

- 只有**未记录**、`waiting_repo`、`launching` 这三种状态会在新一轮被重新处理。
- `launched` 一旦记下，就算 session 已经结束也不会再启动。这是“绝不重复启动”的最强保证。想重跑，就用 `retry` 手动清掉这条记录。
- `error` 不会自动重试，否则每轮都会通知你一次。修好原因后执行 `ganglion.py retry <id>`。

**崩溃安全。** 调用 `tmux new-session` 之前，先把状态落盘为 `launching`。重启时这样对账：

- 状态是 `launching`，session 也在：补记为 `launched`。
- 状态是 `launching`，session 不在：重新处理一遍。投递步骤是幂等的：内容相同就不重写，没有改动就不再 commit。
- 有 `gl-*` session，但状态文件里没有记录（比如状态文件丢了）：接管它，标记为 `launched` 且 `adopted: true`。

**并发。** 状态文件的所有读写都在 `flock(state.lock)` 里完成，写入方式是写临时文件再 `os.replace`。主循环和多个 Stop hook 可以同时访问，不会互相破坏。此外还有一把非阻塞的 `ganglion.lock`，保证同一时间只有一个主进程在跑。

**防繁殖的深度定义。**

- `source: human` 的深度为 0。
- `source: agent` 的深度等于 parent 的深度加 1。
- `source: agent` 但没写 parent 的，深度记为 1（视为直接挂在某条人写的需求下）。
- parent 不在 inbox 中，或者 parent 链成环：直接拒绝。这里宁可误拒也不放行，因为放行就等于绕过了防繁殖。
- 深度大于 `max_depth` 时拒绝。

**需求 id 的限制。** id 不能含空白、`.`、`:`、引号、反斜杠、反引号、`$`，也不能以 `-` 开头。tmux 会把 session 名里的 `.` 和 `:` 改写掉，这会破坏 id 和 session 名之间的一一对应，而对账依赖这个对应关系。所以这类 id 直接报错，不做静默转换。中文 id 是允许的。

**`target: new` 的细节。**

- 如果这个名字已经在 config 的 `[repos]` 里，报错并提示改用 `existing`，避免对着一个已有工程去建新仓库。
- 如果状态文件里已经有映射（之前的 `new` 需求建过，或者本条在崩溃前建过），直接复用，不再调用 gh。
- `gh repo create` 失败时，如果 `gh repo view` 能看到这个仓库，就认为是崩溃前已经建好了，继续 clone。
- 用 `gh repo clone` 而不是 `git clone`，这样会遵循你在 gh 里设置的协议偏好（https 或 ssh）。

**`manual` 的通知。** 进入 `waiting_repo` 时只通知一次，之后每轮静默检查。config 每轮都会重新加载，补上映射后不用重启。

**启动命令。** 实际执行的命令是 `tmux new-session -d -s gl-<id> -c <工程> -e GANGLION_ID=<id> -e GANGLION_STATE=<状态目录> "<claude> <prompt>"`。

- `<claude> <prompt>` 部分用 `shlex.join` 拼出来，prompt 里有引号或 `$` 也不会被 shell 解释。
- 在需求规定的命令之外多了两个可选配置：`claude_cmd`（launchd 下需要写绝对路径）和 `claude_args`（比如权限模式）。两者默认值下，命令和需求里的完全一致。
- Stop hook 需要知道通知配置在哪。这个路径写在 `state.json` 的 `config_path` 字段里，所以不用额外加 `-e`。

**trust 兜底。** 启动后在 `trust_wait` 秒（默认 8 秒）内轮询 `capture-pane`，匹配到 trust 相关文案（`Do you trust the files`、`trust this folder`、`one you trust`、`Yes, proceed`）就发一次 Enter，并记录一条 `trust_fallback` 事件。之所以轮询而不是只看一次，是因为 claude 启动慢的时候，只检查一次容易错过提示。

**通知接口。** `Notifier.send(title, message, tags)`，内置三种实现：`ntfy`、`file`、`null`。也可以写 `backend = "module:factory"` 接入自定义实现。通知失败只记日志，不影响主流程，也不影响 Stop hook。

**Stop hook 的行为。** 永远 `exit 0`，永远不往 stdout 输出内容。原因是 Claude Code 会把 Stop hook 的 exit 2 当作“阻止停下”，这正是要避免的。异常信息写到 `<state_dir>/hook.log`。

**范围内不做的事**（与需求一致）：profile 切换、Web UI、鉴权、部署，以及 `DELIVERY.md` 之外的任何完成判定。
