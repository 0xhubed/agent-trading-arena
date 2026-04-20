#!/usr/bin/env bash
# migrate-to-thinkcentre.sh — Set up Agent Arena on ThinkCentre
#
# Run this script ON the ThinkCentre after cloning the repo.
#
# Prerequisites:
#   - Ubuntu with Python 3.11+, Node.js 18+, git, tmux
#   - API keys (will be prompted)
#
# What it does:
#   1. Installs and configures local PostgreSQL (+ pgvector)
#   2. Migrates data from EliteDesk Postgres (optional)
#   3. Creates Python venv and installs dependencies
#   4. Installs frontend dependencies
#   5. Generates .env from interactive prompts
#   6. Verifies local Postgres connectivity
#   7. Prints next steps (stop on EliteDesk, start here)

set -euo pipefail

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${GREEN}[migrate]${NC} $*"; }
warn()  { echo -e "${YELLOW}[migrate]${NC} $*"; }
error() { echo -e "${RED}[migrate]${NC} $*"; exit 1; }
step()  { echo -e "\n${CYAN}── $* ──${NC}"; }

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

PG_DB="agent_arena"
PG_USER="arena"
PG_PORT="5432"

# ── 1. Install PostgreSQL + pgvector ─────────────────────────────────────────
step "1. PostgreSQL setup"

if command -v psql &> /dev/null && systemctl is-active --quiet postgresql 2>/dev/null; then
  info "PostgreSQL is already installed and running."
else
  info "Installing PostgreSQL..."
  sudo apt-get update -qq
  sudo apt-get install -y -qq postgresql postgresql-contrib

  # Install pgvector (required for learning agents / RAG)
  if ! dpkg -l | grep -q postgresql-.*-pgvector 2>/dev/null; then
    info "Installing pgvector extension..."
    PG_VERSION=$(pg_config --version | grep -oP '\d+' | head -1)
    sudo apt-get install -y -qq "postgresql-${PG_VERSION}-pgvector" 2>/dev/null || {
      warn "pgvector not in apt — installing from source..."
      sudo apt-get install -y -qq postgresql-server-dev-"${PG_VERSION}" build-essential git
      TMPDIR=$(mktemp -d)
      git clone --branch v0.8.0 https://github.com/pgvector/pgvector.git "$TMPDIR/pgvector"
      cd "$TMPDIR/pgvector" && make && sudo make install && cd "$PROJECT_DIR"
      rm -rf "$TMPDIR"
    }
    info "pgvector installed."
  else
    info "pgvector already installed."
  fi

  sudo systemctl enable postgresql
  sudo systemctl start postgresql
  info "PostgreSQL started."
fi

# ── 2. Create database and user ──────────────────────────────────────────────
step "2. Database and user"

# Prompt for password
read -rsp "  Choose a password for Postgres user '$PG_USER': " PG_PASS
echo ""

# Create user if not exists
if sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='${PG_USER}'" | grep -q 1; then
  info "User '$PG_USER' already exists — updating password."
  sudo -u postgres psql -c "ALTER USER ${PG_USER} WITH PASSWORD '${PG_PASS}';" > /dev/null
else
  info "Creating user '$PG_USER'..."
  sudo -u postgres psql -c "CREATE USER ${PG_USER} WITH PASSWORD '${PG_PASS}';" > /dev/null
fi

# Create database if not exists
if sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='${PG_DB}'" | grep -q 1; then
  info "Database '$PG_DB' already exists."
else
  info "Creating database '$PG_DB'..."
  sudo -u postgres psql -c "CREATE DATABASE ${PG_DB} OWNER ${PG_USER};" > /dev/null
fi

# Enable pgvector extension
sudo -u postgres psql -d "$PG_DB" -c "CREATE EXTENSION IF NOT EXISTS vector;" > /dev/null 2>&1 || \
  warn "Could not enable pgvector — RAG/learning features may not work."

# Grant permissions
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE ${PG_DB} TO ${PG_USER};" > /dev/null
sudo -u postgres psql -d "$PG_DB" -c "GRANT ALL ON SCHEMA public TO ${PG_USER};" > /dev/null
info "Database ready: ${PG_DB} (user: ${PG_USER})"

# ── 3. Migrate data from EliteDesk (optional) ────────────────────────────────
step "3. Data migration from EliteDesk (optional)"

echo ""
read -rp "  Migrate existing data from EliteDesk? [y/N]: " MIGRATE_DATA
if [[ "${MIGRATE_DATA,,}" == "y" ]]; then
  read -rp "  EliteDesk host [elitedesk.tail]: " SRC_HOST
  SRC_HOST="${SRC_HOST:-elitedesk.tail}"
  read -rp "  Source database [agent_arena]: " SRC_DB
  SRC_DB="${SRC_DB:-agent_arena}"
  read -rp "  Source user [daniel]: " SRC_USER
  SRC_USER="${SRC_USER:-daniel}"
  read -rp "  Source port [5432]: " SRC_PORT
  SRC_PORT="${SRC_PORT:-5432}"

  info "Dumping from ${SRC_HOST}:${SRC_PORT}/${SRC_DB}..."
  DUMP_FILE="/tmp/agent_arena_dump_$(date +%Y%m%d_%H%M%S).sql"

  if pg_dump -h "$SRC_HOST" -p "$SRC_PORT" -U "$SRC_USER" -d "$SRC_DB" \
    --no-owner --no-privileges -F p -f "$DUMP_FILE" 2>/dev/null; then
    info "Dump saved to $DUMP_FILE ($(du -h "$DUMP_FILE" | cut -f1))"

    info "Restoring to local database..."
    if PGPASSWORD="$PG_PASS" psql -h localhost -U "$PG_USER" -d "$PG_DB" -f "$DUMP_FILE" > /dev/null 2>&1; then
      info "Data migration complete."
    else
      warn "Restore had errors — some tables may not have migrated."
      warn "Check manually: psql -U $PG_USER -d $PG_DB -c '\\dt'"
    fi
    rm -f "$DUMP_FILE"
  else
    warn "pg_dump failed — is EliteDesk Postgres reachable and accepting connections?"
    warn "You can migrate later with:"
    warn "  pg_dump -h $SRC_HOST -U $SRC_USER -d $SRC_DB | psql -U $PG_USER -d $PG_DB"
  fi
else
  info "Skipping data migration — starting fresh."
fi

# ── 4. Python venv + deps ────────────────────────────────────────────────────
step "4. Python environment"

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
source .venv/bin/activate
pip install -e ".[dev,api,learning]" -q
info "Python dependencies installed (including learning extras for pgvector)."

# ── 5. Frontend deps ─────────────────────────────────────────────────────────
step "5. Frontend"

if command -v node &> /dev/null; then
  cd frontend && npm install && cd ..
  info "Frontend dependencies installed."
else
  warn "Node.js not found — install Node.js 18+ and run: cd frontend && npm install"
fi

# ── 6. Generate .env ─────────────────────────────────────────────────────────
step "6. Environment configuration"

if [ -f .env ]; then
  warn ".env already exists — skipping generation."
  warn "Verify DATABASE_URL points to localhost (not EliteDesk)."
else
  info "Generating .env file..."
  echo ""
  echo "Enter API keys (press Enter to skip any):"
  echo ""

  read -rp "  ANTHROPIC_API_KEY (Observer/Claude agents): " ANTHROPIC_KEY
  read -rp "  OLLAMA_API_KEY (Ollama Cloud GPT-OSS): " OLLAMA_KEY
  read -rp "  TOGETHER_API_KEY (Together AI, if used): " TOGETHER_KEY
  read -rp "  OPENROUTER_API_KEY (OpenRouter, if used): " OPENROUTER_KEY
  read -rp "  ARENA_ADMIN_KEY [incore83]: " ADMIN_KEY
  ADMIN_KEY="${ADMIN_KEY:-incore83}"

  cat > .env << EOF
# Agent Arena — ThinkCentre deployment
# Generated by migrate-to-thinkcentre.sh

# API Keys
ANTHROPIC_API_KEY=${ANTHROPIC_KEY}
OLLAMA_API_KEY=${OLLAMA_KEY}
TOGETHER_API_KEY=${TOGETHER_KEY}
OPENROUTER_API_KEY=${OPENROUTER_KEY}

# Admin
ARENA_ADMIN_KEY=${ADMIN_KEY}

# Database (local Postgres on ThinkCentre)
DATABASE_URL=postgresql+asyncpg://${PG_USER}:${PG_PASS}@localhost:${PG_PORT}/${PG_DB}
DATABASE_BACKEND=postgres
EOF

  chmod 600 .env
  info ".env created (permissions: 600)."
fi

# ── 7. Verify local Postgres ─────────────────────────────────────────────────
step "7. Verification"

if pg_isready -h localhost -p "$PG_PORT" -q 2>/dev/null; then
  info "Local Postgres is reachable."

  # Quick connection test with the app user
  if PGPASSWORD="$PG_PASS" psql -h localhost -U "$PG_USER" -d "$PG_DB" -c "SELECT 1;" > /dev/null 2>&1; then
    info "App user '$PG_USER' can connect to '$PG_DB'."
  else
    warn "Connection test failed — check password and pg_hba.conf."
    warn "You may need to add: local $PG_DB $PG_USER scram-sha-256"
    warn "to /etc/postgresql/*/main/pg_hba.conf and restart PostgreSQL."
  fi

  # Check pgvector
  if PGPASSWORD="$PG_PASS" psql -h localhost -U "$PG_USER" -d "$PG_DB" \
    -tAc "SELECT 1 FROM pg_extension WHERE extname='vector'" 2>/dev/null | grep -q 1; then
    info "pgvector extension is enabled."
  else
    warn "pgvector extension not found — learning/RAG features won't work."
  fi
else
  warn "Local Postgres not responding on port $PG_PORT."
fi

# ── 8. Next steps ────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
info "ThinkCentre setup complete."
echo "============================================================"
echo ""
echo "BEFORE starting here, stop Agent Arena on EliteDesk:"
echo ""
echo "  ssh elitedesk.tail"
echo "  curl -X POST http://localhost:8000/api/stop   # save state"
echo "  tmux kill-session -t arena"
echo ""
if [[ "${MIGRATE_DATA,,}" == "y" ]] 2>/dev/null; then
echo "Data was migrated — you can resume the competition:"
echo "  ./scripts/deploy.sh"
echo "  → Will detect saved state and resume from last tick"
else
echo "No data was migrated — starting fresh:"
echo "  ./scripts/deploy.sh"
echo "  → Starts a new competition from scratch"
echo ""
echo "To migrate data later:"
echo "  pg_dump -h elitedesk.tail -U daniel -d agent_arena | \\"
echo "    PGPASSWORD='$PG_PASS' psql -h localhost -U $PG_USER -d $PG_DB"
fi
echo ""
echo "Access points (update bookmarks):"
echo "  Dashboard:  http://thinkcentre.tail:3000"
echo "  API:        http://thinkcentre.tail:8000"
echo "  API Docs:   http://thinkcentre.tail:8000/docs"
echo ""
echo "============================================================"
