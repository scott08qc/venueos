#!/bin/bash
set -e

# APP_PORT is injected by the sandbox (3000-3099 range)
# Vite dev server listens on APP_PORT so the sandbox proxy can reach it
# FastAPI backend runs on an internal port, proxied by Vite
VITE_PORT=${APP_PORT:-5173}
BACKEND_PORT=$((VITE_PORT + 100))
export VITE_BACKEND_PORT=$BACKEND_PORT

# Port conflict guard — active in Workshop sandbox, skipped elsewhere
if [ -f /usr/local/lib/workshop-devguard.sh ]; then
    source /usr/local/lib/workshop-devguard.sh
    devguard_acquire "$VITE_PORT" "$BACKEND_PORT"
fi

# Startup timing
T0=$(date +%s%3N 2>/dev/null || python3 -c "import time;print(int(time.time()*1000))")
elapsed() { echo $(( $(date +%s%3N 2>/dev/null || python3 -c "import time;print(int(time.time()*1000))") - T0 )); }

# Keep venv on local container FS (/root) to avoid network-volume hardlink failures.
# Faster than UV_LINK_MODE=copy and survives within-session restarts via the hash guard.
export VENV_DIR="/tmp/.venv-venueos"
export UV_PROJECT_ENVIRONMENT="$VENV_DIR"

# Install Python and JS deps in parallel, with lockfile hash guards
(
  UV_HASH=$(md5sum uv.lock 2>/dev/null | cut -d' ' -f1)
  if [ ! -f "$VENV_DIR/.uv-hash-$UV_HASH" ]; then
    echo "[+$(elapsed)ms] uv sync starting..."
    UV_LINK_MODE=copy uv sync --compile-bytecode --frozen || UV_LINK_MODE=copy uv sync --compile-bytecode    rm -f "$VENV_DIR"/.uv-hash-* 2>/dev/null
    touch "$VENV_DIR/.uv-hash-$UV_HASH"
    echo "[+$(elapsed)ms] uv sync done"
  else
    echo "[+$(elapsed)ms] uv sync skipped (lockfile unchanged)"
  fi
) &
UV_PID=$!

(
  BUN_HASH=$(md5sum bun.lock 2>/dev/null | cut -d' ' -f1)
  if [ ! -f "node_modules/.bun-hash-$BUN_HASH" ]; then
    echo "[+$(elapsed)ms] bun install starting..."
    bun install --frozen-lockfile
    rm -f node_modules/.bun-hash-* 2>/dev/null
    touch "node_modules/.bun-hash-$BUN_HASH"
    echo "[+$(elapsed)ms] bun install done"
  else
    echo "[+$(elapsed)ms] bun install skipped (lockfile unchanged)"
  fi
) &
BUN_PID=$!

# Start FastAPI as soon as Python deps are ready (background)
wait $UV_PID
echo "[+$(elapsed)ms] Starting FastAPI on port $BACKEND_PORT"
uv run uvicorn app:asgi --reload --host 0.0.0.0 --port $BACKEND_PORT \
  --reload-exclude ".venv" --reload-exclude ".git" --reload-exclude "__pycache__" --reload-exclude "*.pyc" --reload-exclude "node_modules" &
BACKEND_PID=$!

# Start Vite as soon as JS deps are ready (foreground)
wait $BUN_PID
echo "[+$(elapsed)ms] Starting Vite on port $VITE_PORT"
bunx vite --host 0.0.0.0 --port $VITE_PORT --strictPort

# Cleanup backend when Vite exits
kill $BACKEND_PID 2>/dev/null
