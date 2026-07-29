#!/usr/bin/env bash
# Start/stop/status for protomolt-serve, detached from any terminal session
# so the MCP endpoint stays up for MCP clients (Kimi, Claude Code, ...).
#
#   scripts/protomolt-serve.sh start    # build if needed, start detached, verify
#   scripts/protomolt-serve.sh stop
#   scripts/protomolt-serve.sh status
#
# Endpoint: http://127.0.0.1:8082/mcp (gRPC catalog on 9090, reflection on).
# Kimi config lives in ~/.kimi-code/mcp.json; a new session picks it up.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="$REPO_ROOT/apps/serve/build/install/protomolt-serve/bin/protomolt-serve"
LOG=/tmp/protomolt-serve.log
HTTP_PORT="${PROTOMOLT_HTTP_PORT:-8082}"
GRPC_PORT="${PROTOMOLT_GRPC_PORT:-9090}"
HOST="${PROTOMOLT_HOST:-127.0.0.1}"

pid() { pgrep -f 'ProtoMoltServe' | head -1 || true; }

mcp_check() {
  curl -s -m 5 -X POST "http://$HOST:$HTTP_PORT/mcp" \
    -H 'Content-Type: application/json' \
    -H 'Accept: application/json, text/event-stream' \
    -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"protomolt-serve.sh","version":"1"}}}' \
    | grep -q '"serverInfo"'
}

case "${1:-start}" in
  start)
    if [ -n "$(pid)" ] && mcp_check; then
      echo "protomolt-serve already running (pid $(pid))."
      exit 0
    fi
    # A dying instance can still match pgrep while it shuts down; wait it out.
    for _ in $(seq 1 10); do
      [ -z "$(pid)" ] && break
      sleep 1
    done
    if [ ! -x "$BINARY" ]; then
      echo "Building protomolt-serve..."
      (cd "$REPO_ROOT" && ./gradlew :protomolt-serve:installDist -x test -q)
    fi
    echo "Starting protomolt-serve (http $HOST:$HTTP_PORT, grpc $GRPC_PORT, log $LOG)..."
    setsid nohup "$BINARY" --host "$HOST" --http-port "$HTTP_PORT" --grpc-port "$GRPC_PORT" \
      > "$LOG" 2>&1 < /dev/null &
    for _ in $(seq 1 15); do
      sleep 1
      if mcp_check; then
        echo "Up: http://$HOST:$HTTP_PORT/mcp (pid $(pid))"
        exit 0
      fi
    done
    echo "Failed to come up; last log lines:" >&2
    tail -20 "$LOG" >&2
    exit 1
    ;;
  stop)
    if [ -n "$(pid)" ]; then
      pkill -f 'ProtoMoltServe' || true
      for _ in $(seq 1 10); do
        [ -z "$(pid)" ] && break
        sleep 1
      done
      echo "Stopped."
    else
      echo "Not running."
    fi
    ;;
  status)
    if [ -n "$(pid)" ] && mcp_check; then
      echo "Running (pid $(pid)), MCP answering at http://$HOST:$HTTP_PORT/mcp"
    else
      echo "Not running."
      exit 1
    fi
    ;;
  *)
    echo "usage: $0 {start|stop|status}" >&2
    exit 2
    ;;
esac
