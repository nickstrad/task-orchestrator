#!/usr/bin/env bash
#
# Remove the smoke-test containers created by add_task.sh so names can be reused.
# One container per worker port: smoke-test-task-<port>.

set -u

PORTS=(3000 3001 3002)

names=()
for PORT in "${PORTS[@]}"; do
  names+=("smoke-test-task-$PORT")
done

echo "Removing containers: ${names[*]}"
# -f force-removes running containers; ignore "no such container" noise.
docker rm -f "${names[@]}" 2>/dev/null || true
echo "Done."
