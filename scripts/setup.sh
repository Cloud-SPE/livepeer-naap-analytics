#!/usr/bin/env bash
# scripts/setup.sh — Bootstrap the development environment.
# Run once after cloning the repository: make setup

set -euo pipefail

echo "==> Checking Go..."
if ! command -v go &>/dev/null; then
  echo "ERROR: Go not found. Install Go 1.22+ from https://go.dev/dl/"
  exit 1
fi
GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
echo "    Go $GO_VERSION found"

echo "==> Checking Python..."
if ! command -v python3 &>/dev/null; then
  echo "ERROR: Python 3.12+ not found."
  exit 1
fi
PYTHON_VERSION=$(python3 --version | awk '{print $2}')
echo "    Python $PYTHON_VERSION found"

echo "==> Checking Docker..."
if ! command -v docker &>/dev/null; then
  echo "ERROR: Docker not found."
  exit 1
fi
echo "    Docker found"

echo "==> Installing Go dependencies..."
(cd api && go mod download)

echo "==> Installing Python dependencies..."
if command -v uv &>/dev/null; then
  (cd pipeline && uv pip install -e ".[dev]")
else
  echo "    uv not found, falling back to pip"
  (cd pipeline && pip install -e ".[dev]")
fi

echo "==> Copying .env.example..."
if [ ! -f .env ]; then
  cp .env.example .env
  echo "    Created .env — review and update values before running."
else
  echo "    .env already exists, skipping."
fi

echo ""
echo "Setup complete. Run 'make up' to start the full stack."
