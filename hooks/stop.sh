#!/bin/sh
# Ganglion's Claude Code Stop hook. Register it under hooks.Stop in ~/.claude/settings.json
# (see README).
#
# - Without GANGLION_ID / GANGLION_STATE (a session Ganglion did not launch) it exits at once,
#   so normal manual Claude Code use is unaffected.
# - 2.x sessions: it only wakes the worker, which collects and reports the result.
#   Sessions launched by 1.x keep the 1.x behaviour (DELIVERY.md -> notify once).
# - Always exit 0 and print nothing: a failing hook must never keep claude from stopping.

[ -n "$GANGLION_ID" ] || exit 0
[ -n "$GANGLION_STATE" ] || exit 0

HERE=$(cd "$(dirname "$0")" && pwd)

# The worker records its interpreter in $GANGLION_STATE/python, because the hook's python3
# on macOS may be the system 3.9 (no tomllib).
PY=""
if [ -r "$GANGLION_STATE/python" ]; then
  PY=$(head -n 1 "$GANGLION_STATE/python")
fi
[ -n "$PY" ] && [ -x "$PY" ] || PY=python3

PYTHONPATH="$HERE/..${PYTHONPATH:+:$PYTHONPATH}" "$PY" -m ganglion stop-hook >/dev/null 2>>"$GANGLION_STATE/hook.log" || true
exit 0
