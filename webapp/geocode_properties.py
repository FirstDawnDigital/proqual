"""
Geocoder ejendomme i properties_for_sale via Nominatim (OpenStreetMap).
Gemmer latitude + longitude i databasen.
Kør én gang (og igen efter scrape-opdateringer):

    python webapp/geocode_properties.py

Rate-limit: 1 request/sekund (Nominatim fair-use policy).
695 boliger ≈ 12 minutter. Kan afbrydes og genstartes — springer allerede geocodede over.
"""

import sqlite3
import time
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("Installer requests: pip install requests --break-system-packages")

BASE_DIR = Path(__file__).parent.parent
DB_PATH  = BASE_DIR / "data" / "ejendom.db"

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
HEADERS = {"User-Agent": "ejendominvestering-app/1.0 (esbvall@gmail.com)"}

def geocode(address: str) -> tuple[float, float] | None:
    """Returnerer (lat, lng) eller None ved fejl."""
    try:
        r = requests.get(
            NOMINATIM_URL,
            params={"q": address + ", Danmark", "format": "json", "limit": 1},
            headers=HEADERS,
            timeout=10,
        )
        r.raise_for_status()
        results = r.json()
        if results:
            return float(results[0]["lat"]), float(results[0]["lon"])
    except Exception as e:
        print(f"    Fejl: {e}")
    return None

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Hent alle uden koordinater
    cur.execute("""
        SELECT id, address FROM properties_for_sale
        WHERE latitude IS NULL OR longitude IS NULL
        ORDER BY id
    """)
    rows = cur.fetchall()
    total = len(rows)

    if total == 0:
        print("Alle ejendomme er allerede geocodet.")
        conn.close()
        return

    print(f"Geocoder {total} ejendomme (1 req/sek) ...")
    ok = 0; fail = 0

    for i, row in enumerate(rows, 1):
        addr = row["address"]
        result = geocode(addr)
        if result:
            lat, lng = result
            cur.execute(
                "UPDATE properties_for_sale SET latitude=?, longitude=? WHERE id=?",
                (lat, lng, row["id"])
            )
            conn.commit()
            ok += 1
            print(f"  [{i}/{total}] ✓ {addr[:50]}  →  {lat:.4f}, {lng:.4f}")
        else:
            fail += 1
            print(f"  [{i}/{total}] ✗ {addr[:50]}")
        time.sleep(1.1)  # Nominatim: max 1 req/sek

    conn.close()
    print(f"\nFærdig: {ok} geocodet, {fail} fejlede.")

if __name__ == "__main__":
    main()
