#!/bin/bash
# push_static.sh — Generér statisk HTML og push til GitHub Pages
#
# Kør manuelt:    ./push_static.sh
# Automatisk via cron (kl. 07:30 dagligt):
#   crontab -e  og tilføj:
#   30 7 * * * /Users/server/Documents/ejendompython/push_static.sh >> /Users/server/Documents/ejendompython/data/push.log 2>&1

set -e
cd "$(dirname "$0")"

echo "=== $(date '+%Y-%m-%d %H:%M') — Starter statisk HTML-eksport ==="

# Aktiver virtual environment
source .venv/bin/activate

# Generér statisk HTML
python webapp/export_static.py

# Push til GitHub (kræver git remote 'origin' er sat op)
git add docs/index.html
git diff --cached --quiet && echo "Ingen ændringer at pushe." && exit 0

git commit -m "Daglig opdatering $(date +%Y-%m-%d)"
git push origin main

echo "=== Push færdig ==="
