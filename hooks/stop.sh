#!/bin/sh
# Ganglion 的 Claude Code Stop hook。
# 挂到 ~/.claude/settings.json 的 hooks.Stop 上（见 README）。
#
# - 没有 GANGLION_ID（= 不是 Ganglion 拉起的 session）时直接退出，
#   不影响平时手动使用 Claude Code。
# - 其余逻辑（检查 DELIVERY.md、去重、通知、改状态）在 ganglion.py stop-hook 里。
# - 永远 exit 0：hook 失败绝不能阻止 claude 停下。

[ -n "$GANGLION_ID" ] || exit 0
[ -n "$GANGLION_STATE" ] || exit 0

HERE=$(cd "$(dirname "$0")" && pwd)
GANGLION_PY="$HERE/../ganglion.py"

# ganglion 主进程启动时把自己的解释器路径写到 $GANGLION_STATE/python，
# 避免 hook 环境里的 python3 是 macOS 自带的 3.9（没有 tomllib）。
PY=""
if [ -r "$GANGLION_STATE/python" ]; then
  PY=$(head -n 1 "$GANGLION_STATE/python")
fi
[ -n "$PY" ] && [ -x "$PY" ] || PY=python3

"$PY" "$GANGLION_PY" stop-hook >/dev/null 2>>"$GANGLION_STATE/hook.log" || true
exit 0
