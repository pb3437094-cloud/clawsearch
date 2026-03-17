#!/usr/bin/env bash
set -euo pipefail

# Load local secrets without printing them.
if [ -f .env.clawsearch ]; then
  set -a
  # shellcheck disable=SC1091
  source .env.clawsearch
  set +a
else
  echo ".env.clawsearch not found in repo root"
  echo "Copy .env.clawsearch.example to .env.clawsearch and add your real Helius key"
  exit 1
fi

# Activate a local venv if present.
if [ -f .venv/bin/activate ]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
elif [ -f venv/bin/activate ]; then
  # shellcheck disable=SC1091
  source venv/bin/activate
fi

exec python main.py
