#!/usr/bin/env bash
#
# POST /tasks -- add the SAME task to all three workers (ports 3000, 3001, 3002).
# Pass a task ID as the first arg, or a fixed default is used.

set -u

HOST="localhost"
PORTS=(3000 3001 3002)
TASK_ID="${1:-11111111-1111-1111-1111-111111111111}"

req() {
  local body
  body=$(curl -sS \
    --connect-timeout 2 --max-time 5 \
    -w $'%{stderr}HTTP %{http_code} (%{time_total}s)\n' \
    "$@")
  printf '%s' "$body" | jq . 2>/dev/null || printf '%s\n' "$body"
}

# Container name must be unique per host, so scope it to the worker's port.
body() {
  local name="$1"
  cat <<JSON
{
  "ID": "$TASK_ID",
  "State": 1,
  "Task": {
    "ID": "$TASK_ID",
    "Name": "$name",
    "State": 1,
    "Image": "strm/helloworld-http"
  }
}
JSON
}

for PORT in "${PORTS[@]}"; do
  BASE="http://$HOST:$PORT"
  NAME="smoke-test-task-$PORT"
  echo "--- POST $BASE/tasks (id=$TASK_ID, name=$NAME) ---"
  req -X POST "$BASE/tasks" \
    -H "Content-Type: application/json" \
    -d "$(body "$NAME")"
  echo
done
