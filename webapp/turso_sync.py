"""
webapp/turso_sync.py — Synkroniserer lokal SQLite til Turso.

Synker kun data-tabeller (properties_for_sale, rental_listings, rental_aggregates).
Rører IKKE property_annotations — brugerdata i Turso bevares.

Metode: genererer SQL-dump for hver tabel og piber det til `turso db shell`.

Kør manuelt:
  python webapp/turso_sync.py

Bruges automatisk af main.py's scheduler efter Boligsiden-scraping.
"""

import os
import sqlite3
import subprocess
import tempfile
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DB_PATH  = BASE_DIR / "data" / "ejendom.db"

# Turso-databasenavn (fra URL: libsql://NAVN-org.turso.io)
TURSO_DB_NAME = "ejendom"

# Tabeller der synkes — property_annotations berøres IKKE
# rental_listings udelades: den er kun rådata til aggregering og
# bruges ikke af webapp'en (kun rental_aggregates og properties_for_sale bruges)
SYNC_TABLES = [
    "rental_aggregates",
    "properties_for_sale",
]

# Turso CLI — prøver både PATH og standard installationssti
TURSO_BIN_CANDIDATES = [
    "turso",
    str(Path.home() / ".turso" / "turso"),
]


def find_turso() -> str | None:
    """Finder turso-binæren."""
    for candidate in TURSO_BIN_CANDIDATES:
        try:
            result = subprocess.run(
                [candidate, "--version"],
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                return candidate
        except (FileNotFoundError, subprocess.TimeoutExpired):
            continue
    return None


def load_turso_config() -> tuple[str, str]:
    """Henter TURSO_URL og TURSO_AUTH_TOKEN fra config.env eller miljø."""
    url = token = ""
    env_file = BASE_DIR / "config.env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            if line.startswith("TURSO_URL="):
                url = line.split("=", 1)[1].strip()
            elif line.startswith("TURSO_AUTH_TOKEN="):
                token = line.split("=", 1)[1].strip()
    url   = os.environ.get("TURSO_URL",        url)
    token = os.environ.get("TURSO_AUTH_TOKEN", token)
    return url, token


def dump_table(conn: sqlite3.Connection, table: str) -> str:
    """Genererer SQL (DROP + CREATE + INSERT) for én tabel."""
    lines = [f"DROP TABLE IF EXISTS [{table}];"]

    # Hent schema
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    if not row:
        return ""
    lines.append(row[0] + ";")

    # Hent data
    cur = conn.execute(f"SELECT * FROM [{table}]")
    cols     = [d[0] for d in cur.description]
    col_list = ", ".join(f'"{c}"' for c in cols)

    for data_row in cur:
        values = []
        for v in data_row:
            if v is None:
                values.append("NULL")
            elif isinstance(v, (int, float)):
                values.append(str(v))
            else:
                values.append("'" + str(v).replace("'", "''") + "'")
        lines.append(f"INSERT INTO [{table}] ({col_list}) VALUES ({', '.join(values)});")

    return "\n".join(lines)


def sync_to_turso(db_path: Path = DB_PATH) -> dict:
    """
    Synkroniserer SYNC_TABLES fra lokal SQLite til Turso.
    Returnerer {'ok': True} eller {'ok': False, 'error': str}.
    """
    url, token = load_turso_config()
    if not url or not token:
        return {"ok": False, "error": "TURSO_URL eller TURSO_AUTH_TOKEN mangler i config.env"}

    if not db_path.exists():
        return {"ok": False, "error": f"Database ikke fundet: {db_path}"}

    turso = find_turso()
    if not turso:
        return {"ok": False, "error": "turso CLI ikke fundet — installer fra https://turso.tech"}

    # Generer SQL for alle tabeller
    conn = sqlite3.connect(db_path)
    sql_parts = []
    row_counts = {}
    for table in SYNC_TABLES:
        print(f"  → Forbereder '{table}'…", end=" ", flush=True)
        sql = dump_table(conn, table)
        if not sql:
            print("ikke fundet, springer over")
            continue
        n = sql.count("INSERT INTO")
        row_counts[table] = n
        sql_parts.append(sql)
        print(f"{n} rækker")
    conn.close()

    if not sql_parts:
        return {"ok": False, "error": "Ingen tabeller at synke"}

    full_sql = "\n\n".join(sql_parts)

    # Skriv til midlertidig fil og pip til turso
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql",
                                     delete=False, encoding="utf-8") as f:
        f.write(full_sql)
        tmp_path = f.name

    try:
        print(f"  → Sender til Turso ({sum(row_counts.values())} rækker)…", flush=True)
        with open(tmp_path, encoding="utf-8") as sql_file:
            result = subprocess.run(
                [turso, "db", "shell", TURSO_DB_NAME],
                stdin=sql_file,
                capture_output=True,
                text=True,
                timeout=300,
            )
        if result.returncode != 0:
            return {"ok": False, "error": result.stderr or result.stdout}
        return {"ok": True, "rows": row_counts}
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "Turso-sync timeout efter 5 minutter"}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def main():
    import sys
    print("Turso-sync startet…")
    result = sync_to_turso()
    if result["ok"]:
        total = sum(result.get("rows", {}).values())
        print(f"✓ Turso synkroniseret: {total} rækker i alt")
    else:
        print(f"❌ Turso-sync fejlede: {result['error']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
