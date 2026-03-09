#!/bin/bash
set -e

echo "=== Finance AI Bot — setup ==="

# 1. GCP auth check
if ! gcloud auth application-default print-access-token &>/dev/null; then
  echo ""
  echo "You are not logged in to Google Cloud. Running login..."
  gcloud auth application-default login
else
  echo "✅ GCP credentials OK"
fi

# 2. Create venv if missing
if [ ! -d ".venv" ]; then
  echo "Creating virtual environment..."
  python3 -m venv .venv
  echo "✅ Virtual environment created"
else
  echo "✅ Virtual environment already exists"
fi

# 3. Install dependencies
echo "Installing dependencies..."
.venv/bin/pip install --quiet -r requirements.txt
echo "✅ Dependencies installed"

echo ""
echo "=== Setup complete ==="
echo ""
echo "Next step: open notebooks/dev_chat.ipynb in VS Code or PyCharm"
echo "and select '.venv' as the Python kernel."
