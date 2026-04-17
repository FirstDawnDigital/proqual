"""
Beregner køreafstand (bil) fra hjemmeadresse til alle ejendomme
via OpenRouteService (ORS) Matrix API + Nominatim geocoding.

Trin:
  1. Geocoder ejendomme uden koordinater via Nominatim (1 req/sek)
  2. Sender ét batch-kald til ORS Matrix API for alle geocodede ejendomme
  3. Gemmer distance_km i databasen

Forudsætninger:
  - Gratis ORS API-nøgle fra https://openrouteservice.org/dev/#/signup
    (kræver kun email — ingen kreditkort)
  - Sæt nøglen i config.env:  ORS_API_KEY=xxxxxxxxxxxxxxxx
    eller send som argument:   python webapp/calc_distances.py --api-key <nøgle>

Gratis ORS-tier:
  - 2.000 requests/dag
  - Op til 3.500 matrix-elementer pr. kald
  - 695 ejendomme × 1 kilde = 695 elementer → klares i ét enkelt kald

Kør:
  cd /Users/server/Documents/ejendompython
  source .venv/bin/activate
  python webapp/calc_distances.py
"""

import sqlite3
import time
import sys
import argparse
import os
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("Installer requests: pip install requests --break-system-packages")

# ── Config ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent
DB_PATH  = BASE_DIR / "data" / "ejendom.db"

# Hjemmeadresse (kilde for alle afstande)
HOME_ADDRESS = "Egevangen 19, 2700 København, Danmark"
HOME_LAT     = 55.7060   # bruges som fallback hvis geocoding af hjemmet fejler
HOME_LNG     = 12.5100

ORS_MATRIX_URL  = "https://api.openrouteservice.org/v2/matrix/driving-car"
NOMINATIM_URL   = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HDRS  = {"User-Agent": "ejendominvestering-app/1.0 (esbvall@gmail.com)"}

ORS_BATCH_SIZE  = 3000   # godt under ORS-limit på 3.500 elementer


# ── Database ───────────────────────────────────────────────────────────────────
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ── Geocoding via Nominatim ────────────────────────────────────────────────────
import re

# Matcher dansk etage/dør-angivelse inkl. kælder og etage-uden-dør:
#   "2. tv"  "0. th"  "1. mf"  "-1. tv"  "0."  "1."
_FLOOR_DOOR_RE = re.compile(r'\s+-?\d+\.(\s+\S+)?(?=,)')

def normalize_address(address: str) -> str:
    """Fjerner etage/dør-del fra dansk adresse så Nominatim kan finde den."""
    return _FLOOR_DOOR_RE.sub("", address).strip()

def _nominatim_query(q: str) -> tuple[float, float] | None:
    """Ét Nominatim-kald. Returnerer (lat, lng) eller None."""
    try:
        r = requests.get(
            NOMINATIM_URL,
            params={"q": q, "format": "json", "limit": 1},
            headers=NOMINATIM_HDRS,
            timeout=10,
        )
        r.raise_for_status()
        hits = r.json()
        if hits:
            return float(hits[0]["lat"]), float(hits[0]["lon"])
    except Exception as e:
        print(f"    Nominatim fejl: {e}")
    return None

def geocode_nominatim(address: str) -> tuple[float, float] | None:
    """
    Forsøger geocoding i tre trin:
      1. Rensed adresse (etage/dør fjernet)
      2. Kun vejnavn + postnummer (hvis husnummer-bogstav forvirrer)
      3. Kun postnummer (giver bycentrum som fallback)
    """
    clean = normalize_address(address)

    # Trin 1: fuld renset adresse
    result = _nominatim_query(clean + ", Danmark")
    if result:
        return result
    time.sleep(1.1)

    # Trin 2: prøv uden bogstav-suffiks på husnummer (37A → 37)
    simplified = re.sub(r'(\d+)[A-Za-z](?=[\s,])', r'\1', clean)
    if simplified != clean:
        result = _nominatim_query(simplified + ", Danmark")
        if result:
            return result
        time.sleep(1.1)

    # Trin 3: fallback til postnummer+by (bycentrum — upræcis men bedre end ingenting)
    zip_city = re.search(r'(\d{4}\s+\S+)', address)
    if zip_city:
        result = _nominatim_query(zip_city.group(1) + ", Danmark")
        if result:
            print(f"    ↩ Fallback til bycentrum: {zip_city.group(1)}")
            return result
        time.sleep(1.1)

    return None


def geocode_missing(conn):
    """Geocoder ejendomme der mangler lat/lng. Springer over allerede geocodede."""
    cur = conn.cursor()
    cur.execute("""
        SELECT id, address FROM properties_for_sale
        WHERE latitude IS NULL OR longitude IS NULL
        ORDER BY id
    """)
    rows = cur.fetchall()
    if not rows:
        print("✓ Alle ejendomme er allerede geocodet.")
        return

    print(f"\nGeocoder {len(rows)} ejendomme via Nominatim (1 req/sek)...")
    ok = fail = 0
    for i, row in enumerate(rows, 1):
        result = geocode_nominatim(row["address"] + ", Danmark")
        if result:
            lat, lng = result
            cur.execute(
                "UPDATE properties_for_sale SET latitude=?, longitude=? WHERE id=?",
                (lat, lng, row["id"])
            )
            conn.commit()
            ok += 1
            print(f"  [{i}/{len(rows)}] ✓  {row['address'][:55]}")
        else:
            fail += 1
            print(f"  [{i}/{len(rows)}] ✗  {row['address'][:55]}")
        time.sleep(1.1)

    print(f"Geocoding færdig: {ok} ok, {fail} fejlede.\n")


# ── ORS Matrix API ─────────────────────────────────────────────────────────────
def calc_distances_ors(conn, api_key: str):
    """
    Henter køreafstand fra HOME til alle geocodede ejendomme via ORS Matrix API.
    Sender i batches af ORS_BATCH_SIZE hvis der er mange ejendomme.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT id, address, latitude, longitude
        FROM properties_for_sale
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
          AND (distance_km IS NULL OR commute_minutes IS NULL)
        ORDER BY id
    """)
    props = cur.fetchall()

    if not props:
        print("Ingen geocodede ejendomme — kør geocoding-trin først.")
        return

    # Geocod hjemmeadresse
    print(f"Geocoder hjemmeadresse: {HOME_ADDRESS}")
    home = geocode_nominatim(HOME_ADDRESS)
    if home:
        home_lat, home_lng = home
        print(f"  → {home_lat:.4f}, {home_lng:.4f}")
    else:
        home_lat, home_lng = HOME_LAT, HOME_LNG
        print(f"  Geocoding fejlede — bruger hardkodet fallback: {home_lat}, {home_lng}")
    time.sleep(1.1)

    print(f"\nBeregner køreafstand for {len(props)} ejendomme via ORS Matrix API...")

    headers = {
        "Authorization": api_key,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    updated = 0
    # Kør i batches
    for batch_start in range(0, len(props), ORS_BATCH_SIZE):
        batch = props[batch_start : batch_start + ORS_BATCH_SIZE]

        # ORS bruger [lng, lat] rækkefølge (GeoJSON-konvention)
        locations = [[home_lng, home_lat]] + [[p["longitude"], p["latitude"]] for p in batch]
        sources      = [0]
        destinations = list(range(1, len(batch) + 1))

        payload = {
            "locations":     locations,
            "sources":       sources,
            "destinations":  destinations,
            "metrics":       ["distance", "duration"],
            "units":         "km",
        }

        try:
            r = requests.post(ORS_MATRIX_URL, json=payload, headers=headers, timeout=30)
            if r.status_code == 401:
                sys.exit("\n❌ Ugyldig ORS API-nøgle. Tjek din nøgle på openrouteservice.org")
            if r.status_code == 429:
                print("  Rate-limit nået — venter 60 sek...")
                time.sleep(60)
                r = requests.post(ORS_MATRIX_URL, json=payload, headers=headers, timeout=30)
            r.raise_for_status()
        except requests.RequestException as e:
            print(f"  ORS kald fejlede: {e}")
            continue

        data = r.json()
        distances = data.get("distances", [[]])[0]   # liste af km-værdier
        durations = data.get("durations", [[]])[0]   # liste af sekunder

        for i, prop in enumerate(batch):
            km  = distances[i] if i < len(distances) else None
            sec = durations[i] if i < len(durations) else None
            if km is not None:
                cur.execute(
                    "UPDATE properties_for_sale SET distance_km=?, commute_minutes=? WHERE id=?",
                    (round(km, 1), round(sec / 60) if sec is not None else None, prop["id"])
                )
                updated += 1

        conn.commit()
        n_end = min(batch_start + ORS_BATCH_SIZE, len(props))
        print(f"  Batch {batch_start+1}–{n_end}: {len(batch)} ejendomme behandlet ✓")

    print(f"\n✓ Køreafstand gemt for {updated} ejendomme.")


# ── Indgangspunkt ──────────────────────────────────────────────────────────────
def load_api_key(args_key: str | None) -> str:
    """Hent ORS API-nøgle: CLI-argument > config.env > miljøvariabel."""
    if args_key:
        return args_key
    # Prøv config.env
    env_file = BASE_DIR / "config.env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("ORS_API_KEY="):
                key = line.split("=", 1)[1].strip()
                if key:
                    return key
    # Prøv miljøvariabel
    key = os.environ.get("ORS_API_KEY", "")
    if key:
        return key
    return ""


def main():
    parser = argparse.ArgumentParser(description="Beregn køreafstande via ORS")
    parser.add_argument("--api-key", help="ORS API-nøgle (alternativt: ORS_API_KEY i config.env)")
    parser.add_argument("--skip-geocoding", action="store_true", help="Spring Nominatim-trin over")
    args = parser.parse_args()

    api_key = load_api_key(args.api_key)
    if not api_key:
        print("""
❌ Ingen ORS API-nøgle fundet.

Hent en gratis nøgle på: https://openrouteservice.org/dev/#/signup
Kræver kun email — ingen kreditkort.

Sæt derefter nøglen i config.env:
    ORS_API_KEY=din_nøgle_her

Eller send den direkte:
    python webapp/calc_distances.py --api-key din_nøgle_her
""")
        sys.exit(1)

    conn = get_conn()
    try:
        if not args.skip_geocoding:
            geocode_missing(conn)
        calc_distances_ors(conn, api_key)
    finally:
        conn.close()

    print("\nGenstart Flask-serveren for at se afstandene i app'en.")


if __name__ == "__main__":
    main()
