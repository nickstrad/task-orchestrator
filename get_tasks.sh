#!/usr/bin/env bash
#
# GET /tasks on all three workers (ports 3000, 3001, 3002).

set -u

HOST="localhost"
PORTS=(3000 3001 3002)

req() {
  local body
  body=$(curl -sS \
    --connect-timeout 2 --max-time 5 \
    -w $'%{stderr}HTTP %{http_code} (%{time_total}s)\n' \
    "$@")
  printf '%s' "$body" | jq . 2>/dev/null || printf '%s\n' "$body"
}

for PORT in "${PORTS[@]}"; do
  BASE="http://$HOST:$PORT"
  echo "--- GET $BASE/tasks ---"
  req -X GET "$BASE/tasks"
  echo
done
