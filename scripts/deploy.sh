#!/usr/bin/env bash
# deploy.sh — redeploy Agent Arena with minimal disruption
# Usage: ./scripts/deploy.sh [config_path]
#
# What it does:
#   1. Pulls latest code
#   2. Installs dependencies
#   3. Kills existing tmux session
#   4. Starts backend + frontend in new tmux session
#   5. Waits for API to be ready
#   6. Resumes competition if saved state exists, otherwise starts fresh

set -euo pipefail

CONFIG="${1:-configs/production.yaml}"
ADMIN_KEY="${ARENA_ADMIN_KEY:-incore83}"
API_URL="http://localhost:8000"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TMUX_SESSION="arena"

# ── colours ────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[deploy]${NC} $*"; }
warn()  { echo -e "${YELLOW}[deploy]${NC} $*"; }
error() { echo -e "${RED}[deploy]${NC} $*"; exit 1; }

# ── 1. pull latest code ─────────────────────────────────────────────────────
info "Pulling latest code..."
cd "$PROJECT_DIR"
git pull

# ── 2. install dependencies ─────────────────────────────────────────────────
info "Installing dependencies..."
source .venv/bin/activate
pip install -e ".[dev,api]" -q

# ── 3. kill existing tmux session ───────────────────────────────────────────
if tmux has-session -t "$TMUX_SESSION" 2>/dev/null; then
  warn "Killing existing tmux session '$TMUX_SESSION'..."
  tmux kill-session -t "$TMUX_SESSION"
fi

# ── 4. start backend + frontend ─────────────────────────────────────────────
info "Starting tmux session '$TMUX_SESSION'..."
tmux new-session -d -s "$TMUX_SESSION" -n backend

# Window 0: Backend (source .env so API has ARENA_ADMIN_KEY etc.)
tmux send-keys -t "$TMUX_SESSION:backend" \
  "cd $PROJECT_DIR && set -a && source .env && set +a && source .venv/bin/activate && uvicorn agent_arena.api.app:app --host 0.0.0.0 --port 8000 --reload --reload-exclude 'tests/*' --reload-exclude 'scripts/*' --reload-exclude 'docs/*' --reload-exclude 'data/*' --reload-exclude '.claude/*'" Enter

# Window 1: Frontend
tmux new-window -t "$TMUX_SESSION" -n frontend
tmux send-keys -t "$TMUX_SESSION:frontend" \
  "cd $PROJECT_DIR/frontend && npm run dev -- --host 0.0.0.0" Enter

# ── 5. wait for API to be ready ─────────────────────────────────────────────
info "Waiting for API..."
MAX_WAIT=30
WAITED=0
until curl -sf "$API_URL/api/status" > /dev/null 2>&1; do
  sleep 2
  WAITED=$((WAITED + 2))
  if [ $WAITED -ge $MAX_WAIT ]; then
    error "API did not start within ${MAX_WAIT}s. Check: tmux attach -t $TMUX_SESSION"
  fi
  echo -n "."
done
echo ""
info "API is up."

# ── 6. resume or start ──────────────────────────────────────────────────────
info "Checking for saved state..."
CAN_RESUME=$(curl -sf "$API_URL/api/can-resume?config_path=$CONFIG" | python3 -c "import sys,json; print(json.load(sys.stdin).get('can_resume', False))")

if [ "$CAN_RESUME" = "True" ]; then
  LAST_TICK=$(curl -sf "$API_URL/api/can-resume?config_path=$CONFIG" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('last_tick','?'))")
  info "Resuming competition from tick $LAST_TICK..."
  RESULT=$(curl -sf -X POST "$API_URL/api/resume?config_path=$CONFIG" -H "X-Admin-Key: $ADMIN_KEY")
  echo "$RESULT" | python3 -m json.tool
  info "Competition resumed."
else
  warn "No saved state found — starting fresh..."
  RESULT=$(curl -sf -X POST "$API_URL/api/start?config_path=$CONFIG" -H "X-Admin-Key: $ADMIN_KEY")
  echo "$RESULT" | python3 -m json.tool
  info "Competition started."
fi

info "Done. Attach with: tmux attach -t $TMUX_SESSION"
