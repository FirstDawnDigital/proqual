#!/bin/bash
# Start ejendomsinvesteringsapp
# Kør: bash webapp/start.sh

cd "$(dirname "$0")/.."

# Aktiver virtual environment hvis det findes
if [ -f ".venv/bin/activate" ]; then
  source .venv/bin/activate
fi

# Installer Flask hvis nødvendig
python3 -c "import flask" 2>/dev/null || pip install flask --break-system-packages -q

python3 webapp/app.py
