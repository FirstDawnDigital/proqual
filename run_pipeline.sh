#!/bin/bash
# Ejendomspipeline – daglig kørsel
# Køres automatisk via launchd kl. 07:00

set -e
cd /Users/server/Documents/ejendompython

# 1. Hent lejedata fra Gmail (Boligportal + Lejebolig)
.venv/bin/python main.py --run --days-back 2

# 2. Scrape salgsboliger fra Boligsiden
.venv/bin/python main.py --scrape-salg

# 3. Geocod nye salgsboliger og beregn køreafstand (kun nye — springer eksisterende over)
.venv/bin/python webapp/calc_distances.py || \
  echo "ADVARSEL: calc_distances.py fejlede (tjek ORS_API_KEY i config.env)"

# 4. Eksporter til Google Sheets
.venv/bin/python main.py --export-sheets
