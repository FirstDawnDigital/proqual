"""
Ejendomsinvesterings Web App – Flask backend

Lokal kørsel (Mac Mini):
  python webapp/app.py
  Åbn: http://<mac-mini-ip>:5050

Produktion (Render + Turso):
  Sæt miljøvariable:  TURSO_URL  og  TURSO_AUTH_TOKEN
  Start:              gunicorn webapp.app:app
"""

import os
import sqlite3
import json
from pathlib import Path
from flask import Flask, jsonify, request, send_from_directory

app = Flask(__name__, static_folder=".", static_url_path="")

BASE_DIR = Path(__file__).parent.parent
DB_PATH  = BASE_DIR / "data" / "ejendom.db"

# ── Database-forbindelse: lokal SQLite eller Turso ────────────────────────────
TURSO_URL   = os.environ.get("TURSO_URL", "")
TURSO_TOKEN = os.environ.get("TURSO_AUTH_TOKEN", "")
USE_TURSO   = bool(TURSO_URL and TURSO_TOKEN)


def get_db():
    """
    Returnerer en database-forbindelse.
    • Turso (produktion): hvis TURSO_URL + TURSO_AUTH_TOKEN er sat i miljø
    • Lokal SQLite (development): standard sqlite3
    Begge returnerer en conn med row_factory = sqlite3.Row.
    """
    if USE_TURSO:
        try:
            import libsql_experimental as libsql
            conn = libsql.connect(TURSO_URL, auth_token=TURSO_TOKEN)
            conn.row_factory = sqlite3.Row
            return conn
        except ImportError:
            app.logger.warning("libsql_experimental ikke installeret — falder tilbage til SQLite")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_annotations_table(conn):
    """Opretter annotations-tabellen hvis den ikke eksisterer."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS property_annotations (
            property_id      INTEGER PRIMARY KEY,
            status           TEXT    DEFAULT 'neutral',   -- neutral | favorite | disqualified
            renovation_items TEXT    DEFAULT '[]',         -- JSON-array af {desc, cost}
            renovation_status TEXT   DEFAULT 'none',       -- none | progress | done
            notes            TEXT    DEFAULT '',
            updated_at       TEXT    DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


# ── Salgsbolig-query ───────────────────────────────────────────────────────────
QUERY = """
SELECT
    p.id,
    p.address,
    p.zip_code,
    p.city,
    p.price,
    p.size_sqm,
    p.rooms,
    p.property_type,
    p.owner_costs_monthly,
    p.energy_label,
    p.listing_url,
    p.scraped_at,
    CASE WHEN p.size_sqm > 0
         THEN ROUND(CAST(p.price AS REAL) / p.size_sqm, 0)
         ELSE NULL END AS sale_kr_per_sqm,
    ra_zip.price_per_sqm_median AS leje_kr_per_sqm,
    ra_zip.price_per_sqm_low    AS leje_kr_per_sqm_low,
    ra_zip.price_per_sqm_high   AS leje_kr_per_sqm_high,
    CASE WHEN p.size_sqm > 0 AND ra_zip.price_per_sqm_median IS NOT NULL
         THEN ROUND(ra_zip.price_per_sqm_median * p.size_sqm, 0)
         ELSE NULL END AS est_leje_sqm,
    CASE WHEN p.size_sqm > 0 AND ra_zip.price_per_sqm_low IS NOT NULL
         THEN ROUND(ra_zip.price_per_sqm_low * p.size_sqm, 0)
         ELSE NULL END AS est_leje_sqm_low,
    CASE WHEN p.size_sqm > 0 AND ra_zip.price_per_sqm_high IS NOT NULL
         THEN ROUND(ra_zip.price_per_sqm_high * p.size_sqm, 0)
         ELSE NULL END AS est_leje_sqm_high,
    CASE WHEN p.price > 0 AND p.size_sqm > 0 AND ra_zip.price_per_sqm_median IS NOT NULL
         THEN ROUND((ra_zip.price_per_sqm_median * p.size_sqm * 12.0) / p.price * 100, 1)
         ELSE NULL END AS yield_sqm_pct,
    ra_zip.sample_size AS lejedata_antal,
    p.distance_km,
    p.commute_minutes,
    ra_rooms.rent_total_median AS est_leje_rooms,
    ra_rooms.rent_total_low    AS est_leje_rooms_low,
    ra_rooms.rent_total_high   AS est_leje_rooms_high,
    ra_rooms.sample_size       AS lejedata_rum_antal,
    CASE WHEN p.price > 0 AND ra_rooms.rent_total_median IS NOT NULL
         THEN ROUND((ra_rooms.rent_total_median * 12.0) / p.price * 100, 1)
         ELSE NULL END AS yield_rooms_pct
FROM properties_for_sale p
LEFT JOIN rental_aggregates ra_zip
    ON ra_zip.zip_code = p.zip_code
    AND ra_zip.rooms IS NULL
    AND ra_zip.property_type IS NULL
LEFT JOIN rental_aggregates ra_rooms
    ON ra_rooms.zip_code = p.zip_code
    AND ra_rooms.rooms = p.rooms
    AND ra_rooms.property_type IS NULL
WHERE p.price IS NOT NULL
  AND (p.is_active IS NULL OR p.is_active = 1)
ORDER BY yield_sqm_pct DESC NULLS LAST
"""


# ── Endpoints ──────────────────────────────────────────────────────────────────

@app.route("/api/properties")
def api_properties():
    conn = get_db()
    try:
        rows = conn.execute(QUERY).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@app.route("/api/annotations", methods=["GET"])
def api_annotations_get():
    """Returnerer alle annotations som dict keyed på property_id."""
    conn = get_db()
    try:
        ensure_annotations_table(conn)
        rows = conn.execute("SELECT * FROM property_annotations").fetchall()
        result = {}
        for r in rows:
            d = dict(r)
            # Parse JSON-feltet
            try:
                d["renovation_items"] = json.loads(d["renovation_items"] or "[]")
            except Exception:
                d["renovation_items"] = []
            result[d["property_id"]] = d
        return jsonify(result)
    finally:
        conn.close()


@app.route("/api/annotations/<int:property_id>", methods=["POST"])
def api_annotations_save(property_id):
    """Gem eller opdater annotation for én bolig."""
    data = request.get_json(silent=True) or {}
    status            = data.get("status", "neutral")
    renovation_items  = json.dumps(data.get("renovation_items", []), ensure_ascii=False)
    renovation_status = data.get("renovation_status", "none")
    notes             = data.get("notes", "")

    conn = get_db()
    try:
        ensure_annotations_table(conn)
        conn.execute("""
            INSERT INTO property_annotations
                (property_id, status, renovation_items, renovation_status, notes, updated_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(property_id) DO UPDATE SET
                status            = excluded.status,
                renovation_items  = excluded.renovation_items,
                renovation_status = excluded.renovation_status,
                notes             = excluded.notes,
                updated_at        = excluded.updated_at
        """, (property_id, status, renovation_items, renovation_status, notes))
        conn.commit()
        return jsonify({"ok": True})
    finally:
        conn.close()


@app.route("/api/meta")
def api_meta():
    conn = get_db()
    try:
        types = [r[0] for r in conn.execute(
            "SELECT DISTINCT property_type FROM properties_for_sale WHERE property_type IS NOT NULL ORDER BY property_type"
        ).fetchall()]
        count = conn.execute(
            "SELECT COUNT(*) FROM properties_for_sale WHERE price IS NOT NULL AND (is_active IS NULL OR is_active = 1)"
        ).fetchone()[0]
        scraped_at = conn.execute(
            "SELECT MAX(scraped_at) FROM properties_for_sale"
        ).fetchone()[0]
        return jsonify({"property_types": types, "total": count, "scraped_at": scraped_at})
    finally:
        conn.close()


@app.route("/")
def index():
    return send_from_directory(".", "index.html")


if __name__ == "__main__":
    import socket
    host = "0.0.0.0"
    port = 5050
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
    except Exception:
        local_ip = "127.0.0.1"
    print(f"\n{'='*50}")
    print(f"  Ejendomsinvesterings Web App")
    print(f"  Lokal:   http://localhost:{port}")
    print(f"  Telefon: http://{local_ip}:{port}")
    print(f"{'='*50}\n")
    app.run(host=host, port=port, debug=False)
