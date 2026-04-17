"""
sheets_exporter.py – Eksporter huslejedata til Google Sheets

Tre sheets eksporteres:
  1. "Huslejedata"      – aggregeret kr/m² pr. postnummer (til VLOOKUP i Rentabilitetsberegner)
  2. "Husleje pr. rum"  – pivot: median månedsleje pr. postnummer × antal rum (1-5)
  3. "Rådata"           – alle individuelle listings til verifikation og backtesting
"""

import logging
import sqlite3
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)

ROOM_COLS = [1, 2, 3, 4, 5]   # de rum-antal vi viser som kolonner i pivot


# ─────────────────────────────────────────────
# Google auth
# ─────────────────────────────────────────────

def get_sheets_client(credentials_file: str):
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        raise ImportError("Kør: pip install gspread google-auth")

    SCOPES = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive',
    ]
    creds = Credentials.from_service_account_file(credentials_file, scopes=SCOPES)
    return gspread.authorize(creds)


def _get_or_create_worksheet(spreadsheet, name: str, rows: int = 2000, cols: int = 20):
    try:
        ws = spreadsheet.worksheet(name)
    except Exception:
        ws = spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)
    return ws


def _fmt(value, decimals=0):
    """Formater tal til præsentabel streng, tom streng hvis None."""
    if value is None:
        return ""
    if decimals == 0:
        return int(round(value))
    return round(value, decimals)


# ─────────────────────────────────────────────
# Sheet 1: Aggregeret kr/m² (VLOOKUP-sheet)
# ─────────────────────────────────────────────

def export_sqm_aggregates(spreadsheet, db_path: str) -> int:
    """
    Sheet: 'Huslejedata'
    Én række pr. postnummer med lav/median/høj kr/m² og månedsleje.
    Bruges til VLOOKUP i Rentabilitetsberegneren.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("""
        SELECT zip_code,
               price_per_sqm_low, price_per_sqm_median, price_per_sqm_high,
               rent_total_low, rent_total_median, rent_total_high,
               sample_size
        FROM rental_aggregates
        WHERE rooms IS NULL AND property_type IS NULL
        ORDER BY CAST(zip_code AS INTEGER)
    """)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        logger.warning("Ingen aggregater til Huslejedata-sheet")
        return 0

    ws = _get_or_create_worksheet(spreadsheet, "Huslejedata")
    now = datetime.now().strftime("%d-%m-%Y %H:%M")

    header = [
        "Postnummer",
        "Kr/m² lav", "Kr/m² median", "Kr/m² høj",
        "Månedsleje lav", "Månedsleje median", "Månedsleje høj",
        "Datapunkter", "Opdateret",
    ]
    data = [header]
    for r in rows:
        data.append([
            r["zip_code"],
            _fmt(r["price_per_sqm_low"], 1),
            _fmt(r["price_per_sqm_median"], 1),
            _fmt(r["price_per_sqm_high"], 1),
            _fmt(r["rent_total_low"]),
            _fmt(r["rent_total_median"]),
            _fmt(r["rent_total_high"]),
            r["sample_size"],
            now,
        ])

    ws.clear()
    ws.update('A1', data)
    ws.format('A1:I1', {'textFormat': {'bold': True},
                        'backgroundColor': {'red': 0.85, 'green': 0.92, 'blue': 0.98}})
    logger.info(f"Huslejedata: {len(data)-1} postnumre eksporteret")
    return len(data) - 1


# ─────────────────────────────────────────────
# Sheet 2: Pivot – median månedsleje × rum
# ─────────────────────────────────────────────

def export_pivot_by_rooms(spreadsheet, db_path: str) -> int:
    """
    Sheet: 'Husleje pr. rum'
    Pivot-tabel: postnummer som rækker, rum (1-5) som kolonner.
    Celleværdi = median månedsleje i kr.

    Eksempel:
    Postnummer | 1 rum  | 2 rum  | 3 rum  | 4 rum  | 5 rum  | Antal
    2200       | 7.500  | 10.200 | 14.000 |        |        | 28
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Hent alle room-specifikke aggregater (rooms IS NOT NULL, ingen type-filter)
    cursor.execute("""
        SELECT zip_code, rooms, rent_total_median, sample_size
        FROM rental_aggregates
        WHERE rooms IS NOT NULL AND property_type IS NULL
        ORDER BY CAST(zip_code AS INTEGER), rooms
    """)
    agg_rows = cursor.fetchall()

    # Hent totalt antal listings pr. postnummer til kontrol
    cursor.execute("""
        SELECT zip_code, COUNT(*) as total
        FROM rental_listings
        WHERE is_active = 1
        GROUP BY zip_code
        ORDER BY CAST(zip_code AS INTEGER)
    """)
    totals = {r["zip_code"]: r["total"] for r in cursor.fetchall()}
    conn.close()

    if not agg_rows:
        logger.warning("Ingen rum-aggregater til pivot-sheet")
        return 0

    # Byg pivot: {zip_code: {rooms: median_rent}}
    pivot = defaultdict(dict)
    for r in agg_rows:
        if r["rooms"] in ROOM_COLS:
            pivot[r["zip_code"]][r["rooms"]] = r["rent_total_median"]

    ws = _get_or_create_worksheet(spreadsheet, "Husleje pr. rum", rows=500, cols=15)
    now = datetime.now().strftime("%d-%m-%Y %H:%M")

    # Header
    header = ["Postnummer"] + [f"{n} rum" for n in ROOM_COLS] + ["Listings i alt", "Opdateret"]
    data = [header]

    for zip_code in sorted(pivot.keys(), key=lambda z: int(z) if z.isdigit() else 9999):
        row_data = [zip_code]
        for rooms in ROOM_COLS:
            val = pivot[zip_code].get(rooms)
            row_data.append(_fmt(val) if val else "")
        row_data.append(totals.get(zip_code, ""))
        row_data.append(now)
        data.append(row_data)

    ws.clear()
    ws.update('A1', data)

    # Fed header + farve
    ws.format('A1:H1', {'textFormat': {'bold': True},
                        'backgroundColor': {'red': 0.92, 'green': 0.98, 'blue': 0.88}})

    # Frys første række så postnumre altid er synlige
    ws.freeze(rows=1)

    logger.info(f"Husleje pr. rum: {len(data)-1} postnumre eksporteret")
    return len(data) - 1


# ─────────────────────────────────────────────
# Sheet 3: Rådata (individuelle listings)
# ─────────────────────────────────────────────

def export_raw_listings(spreadsheet, db_path: str, limit: int = 5000) -> int:
    """
    Sheet: 'Rådata'
    Alle individuelle lejeboliger – til verifikation og backtesting.
    Maks 5000 rækker (Google Sheets grænse er 10M celler).
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT
            id, source, listing_id,
            address, zip_code, city,
            rent_monthly, size_sqm, rooms, property_type,
            ROUND(CAST(rent_monthly AS REAL) / NULLIF(size_sqm, 0), 1) AS kr_per_sqm,
            listing_url,
            scraped_at, email_received_at
        FROM rental_listings
        WHERE is_active = 1
        ORDER BY scraped_at DESC
        LIMIT {limit}
    """)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        logger.warning("Ingen rådata at eksportere")
        return 0

    ws = _get_or_create_worksheet(spreadsheet, "Rådata", rows=limit + 10, cols=15)

    header = [
        "ID", "Kilde", "Annonce-ID",
        "Adresse", "Postnummer", "By",
        "Månedsleje (kr)", "Størrelse (m²)", "Rum", "Boligtype",
        "Kr/m²", "URL", "Hentet", "Email modtaget",
    ]
    data = [header]
    for r in rows:
        data.append([
            r["id"],
            r["source"],
            r["listing_id"] or "",
            r["address"] or "",
            r["zip_code"] or "",
            r["city"] or "",
            r["rent_monthly"] or "",
            r["size_sqm"] or "",
            r["rooms"] or "",
            r["property_type"] or "",
            r["kr_per_sqm"] or "",
            r["listing_url"] or "",
            (r["scraped_at"] or "")[:16],
            (r["email_received_at"] or "")[:16],
        ])

    ws.clear()
    ws.update('A1', data)
    ws.format('A1:N1', {'textFormat': {'bold': True},
                        'backgroundColor': {'red': 0.98, 'green': 0.95, 'blue': 0.85}})
    ws.freeze(rows=1)

    logger.info(f"Rådata: {len(data)-1} listings eksporteret")
    return len(data) - 1


# ─────────────────────────────────────────────
# Hoved-eksportfunktion (kalder alle tre)
# ─────────────────────────────────────────────

def export_all_sheets(db_path: str, sheet_id: str, credentials_file: str) -> dict:
    """
    Eksporter alle tre sheets i ét kald.
    Returnér dict med antal rækker pr. sheet.
    """
    client = get_sheets_client(credentials_file)
    spreadsheet = client.open_by_key(sheet_id)

    results = {}
    results['huslejedata']    = export_sqm_aggregates(spreadsheet, db_path)
    results['husleje_pr_rum'] = export_pivot_by_rooms(spreadsheet, db_path)
    results['rådata']         = export_raw_listings(spreadsheet, db_path)
    return results


# Behold bagudkompatibel funktion som main.py kalder
def export_rental_aggregates_to_sheets(
    db_path: str, sheet_id: str, credentials_file: str,
    worksheet_name: str = "Huslejedata"
) -> int:
    results = export_all_sheets(db_path, sheet_id, credentials_file)
    return results.get('huslejedata', 0)
