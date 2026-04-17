"""
webapp/turso_sync.py — Synkroniserer lokal SQLite til Turso (hosted SQLite)

Kør manuelt:
  python webapp/turso_sync.py

Kaldes automatisk af main.py's scheduler efter hver Boligsiden-scraping.

Forudsætninger:
  - TURSO_URL og TURSO_AUTH_TOKEN sat i config.env
  - pip install libsql-experimental --break-system-packages
"""

import os
import sys
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DB_PATH  = BASE_DIR / "data" / "ejendom.db"


def load_turso_config() -> tuple[str, str]:
    """Hent Turso URL og token fra config.env eller miljøvariable."""
    # Prøv at indlæse config.env
    env_file = BASE_DIR / "config.env"
    url = token = ""
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            if line.startswith("TURSO_URL="):
                url = line.split("=", 1)[1].strip()
            elif line.startswith("TURSO_AUTH_TOKEN="):
                token = line.split("=", 1)[1].strip()
    # Miljøvariable tilsidesætter config.env
    url   = os.environ.get("TURSO_URL",        url)
    token = os.environ.get("TURSO_AUTH_TOKEN", token)
    return url, token


def sync_to_turso(db_path: Path = DB_PATH) -> dict:
    """
    Synkroniserer den lokale SQLite-database til Turso ved hjælp af
    libsql-experimental's embedded replica-funktion.

    Embedded replica:
      1. Åbner den lokale .db-fil som en libsql-forbindelse
      2. Kalder conn.sync() — uploader lokale ændringer til Turso
      3. Lukker forbindelsen

    Returnerer {'ok': True} eller {'ok': False, 'error': str}.
    """
    url, token = load_turso_config()

    if not url or not token:
        return {
            "ok": False,
            "error": "TURSO_URL eller TURSO_AUTH_TOKEN mangler i config.env",
        }

    if not db_path.exists():
        return {"ok": False, "error": f"Database ikke fundet: {db_path}"}

    try:
        import libsql_experimental as libsql
    except ImportError:
        return {
            "ok": False,
            "error": (
                "libsql_experimental ikke installeret. "
                "Kør: pip install libsql-experimental --break-system-packages"
            ),
        }

    try:
        print(f"Forbinder til Turso: {url[:40]}…")
        conn = libsql.connect(str(db_path), sync_url=url, auth_token=token)
        print("Synkroniserer lokal SQLite → Turso…")
        conn.sync()
        conn.close()
        print("✓ Turso-sync færdig")
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def main():
    result = sync_to_turso()
    if result["ok"]:
        print("✓ Turso synkroniseret succesfuldt")
    else:
        print(f"❌ Turso-sync fejlede: {result['error']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
