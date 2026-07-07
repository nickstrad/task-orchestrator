#!/usr/bin/env bash
#
# DELETE /tasks/{taskID} on a single worker -- stops the task with that id.
# Usage: ./stop_task.sh <port> <taskID>
#   e.g. ./stop_task.sh 3000 11111111-1111-1111-1111-111111111111

set -u

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <port> <taskID>" >&2
  exit 1
fi

HOST="localhost"
PORT="$1"
TASK_ID="$2"

req() {
  local body
  body=$(curl -sS \
    --connect-timeout 2 --max-time 5 \
    -w $'%{stderr}HTTP %{http_code} (%{time_total}s)\n' \
    "$@")
  printf '%s' "$body" | jq . 2>/dev/null || printf '%s\n' "$body"
}

BASE="http://$HOST:$PORT"
echo "--- DELETE $BASE/tasks/$TASK_ID (stop task) ---"
req -X DELETE "$BASE/tasks/$TASK_ID"
echo
