"""
sheets_exporter.py – Eksporter huslejedata til Google Sheets

Tre sheets eksporteres:
  1. "Huslejedata"      – aggregeret kr/m²/år pr. postnummer (til VLOOKUP i Rentabilitetsberegner)
  2. "Husleje pr. rum"  – pivot: median månedsleje pr. postnummer × antal rum (1-5)
  3. "Rådata"           – alle individuelle listings med demand signal og rød-flag indikatorer
"""

import logging
import sqlite3
from datetime import datetime, timezone
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


def _days_active(first_seen: str, last_seen: str, is_active: int,
                 last_checked: str = None) -> str:
    """
    Beregn antal dage en listing har været synlig.
    Kræver last_checked for ALLE listings — aktive såvel som inaktive.
    Uden et live-tjek ved vi ikke om first_seen/last_seen er pålidelige.
    """
    if not first_seen or not last_checked:
        return ""
    try:
        fs = datetime.fromisoformat(first_seen[:19])
        ls = datetime.now() if is_active else datetime.fromisoformat((last_seen or first_seen)[:19])
        return (ls - fs).days
    except Exception:
        return ""


def _demand_signal(days, last_checked: str = None, is_active: int = 1) -> str:
    """
    Klassificér listing baseret på antal dage synlig.

    Aktive listings: neutral tidslabel ("Ny annonce", "2 uger", osv.)
    Inaktive listings: efterspørgselssignal baseret på tid til udlejning.
    Begge: viser '⬜ Ikke tjekket' hvis aldrig live-verificeret.
    """
    if not last_checked:
        return "⬜ Ikke tjekket"
    if days == "" or days is None:
        return ""

    if is_active:
        # Aktiv listing — vis neutral tid-på-markedet label
        if days < 14:
            return "✅ Ny (<14 dage)"
        if days < 30:
            return "📅 2-4 uger"
        if days < 60:
            return "📅 1-2 måneder"
        if days < 90:
            return "📅 2-3 måneder"
        return "📅 3+ måneder"
    else:
        # Inaktiv listing — tid fra opslag til udlejning = efterspørgselssignal
        if days < 14:
            return "🔥 <14 dage"
        if days < 30:
            return "✅ 14-30 dage"
        if days < 60:
            return "🟡 30-60 dage"
        if days < 90:
            return "🟠 60-90 dage"
        return "🐌 90+ dage"


def _relist_flag(relist_count: int, price_change_count: int) -> str:
    """Rød flag hvis listing er blevet re-listet eller har haft prisændringer."""
    flags = []
    if relist_count and relist_count > 0:
        flags.append(f"⚠ {relist_count}× genlistet")
    if price_change_count and price_change_count > 0:
        flags.append(f"💰 {price_change_count}× prisændring")
    return " | ".join(flags) if flags else ""


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
        "Kr/m²/år lav", "Kr/m²/år median", "Kr/m²/år høj",
        "Månedsleje lav", "Månedsleje median", "Månedsleje høj",
        "Datapunkter", "Opdateret",
    ]
    data = [header]
    for r in rows:
        data.append([
            r["zip_code"],
            _fmt(r["price_per_sqm_low"]  * 12, 0) if r["price_per_sqm_low"]  else "",
            _fmt(r["price_per_sqm_median"] * 12, 0) if r["price_per_sqm_median"] else "",
            _fmt(r["price_per_sqm_high"] * 12, 0) if r["price_per_sqm_high"]  else "",
            _fmt(r["rent_total_low"]),
            _fmt(r["rent_total_median"]),
            _fmt(r["rent_total_high"]),
            r["sample_size"],
            now,
        ])

    ws.clear()
    ws.update('A1', data, value_input_option='USER_ENTERED')
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
    ws.update('A1', data, value_input_option='USER_ENTERED')

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

def make_hyperlink(url: str) -> str:
    """Wrap URL i Google Sheets HYPERLINK()-formel. Rå URL hvis for lang."""
    if not url:
        return ""
    # Google Sheets HYPERLINK maks ~2000 tegn
    if len(url) > 1900:
        return url  # for lang – vis rå URL
    escaped = url.replace('"', '%22')
    # Dansk/europæisk Google Sheets bruger semikolon som argumentseparator
    return f'=HYPERLINK("{escaped}";"Se annonce")'


def export_raw_listings(spreadsheet, db_path: str, limit: int = 20000) -> int:
    """
    Sheet: 'Rådata'
    Alle individuelle lejeboliger – til verifikation og backtesting.
    20.000 rækker × 18 kolonner = 360.000 celler (Google Sheets max er 10M).
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT
            id, source, listing_id,
            address, zip_code, city,
            rent_monthly, size_sqm, rooms, property_type,
            ROUND(CAST(rent_monthly AS REAL) / NULLIF(size_sqm, 0) * 12, 0) AS kr_per_sqm,
            listing_url,
            scraped_at, email_received_at,
            first_seen, last_seen, is_active,
            last_checked, relist_count, price_change_count
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

    ws = _get_or_create_worksheet(spreadsheet, "Rådata", rows=limit + 10, cols=20)

    header = [
        "ID", "Kilde", "Annonce-ID",
        "Adresse", "Postnummer", "By",
        "Månedsleje (kr)", "Størrelse (m²)", "Rum", "Boligtype",
        "Kr/m²/år", "URL", "Første gang set", "Sidst set", "Live-tjekket",
        "Dage synlig", "Efterspørgsel", "Flag",
    ]
    data = [header]
    for r in rows:
        lc   = r["last_checked"]
        days = _days_active(r["first_seen"], r["last_seen"], r["is_active"], lc)
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
            make_hyperlink(r["listing_url"]),
            (r["first_seen"] or "")[:10],
            (r["last_seen"] or "")[:10],
            (lc or "")[:10],
            days,
            _demand_signal(days, lc, r["is_active"]),
            _relist_flag(r["relist_count"], r["price_change_count"]),
        ])

    ws.clear()
    ws.update('A1', data, value_input_option='USER_ENTERED')
    ws.format('A1:Q1', {'textFormat': {'bold': True},
                        'backgroundColor': {'red': 0.98, 'green': 0.95, 'blue': 0.85}})
    ws.freeze(rows=1)

    logger.info(f"Rådata: {len(data)-1} listings eksporteret")
    return len(data) - 1


# ─────────────────────────────────────────────
# Sheet 4: Boliger til salg
# ─────────────────────────────────────────────

def export_properties_for_sale(spreadsheet, db_path: str, limit: int = 5000) -> int:
    """
    Sheet: 'Boliger til salg'
    Alle scrapede salgsboliger fra Boligsiden med to afkastestimater:
      1. Baseret på m²:  median kr/m²/md (postnummer) × boligens m²
      2. Baseret på rum: median månedsleje for samme rum-antal i postnummeret
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(f"""
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
            -- Salgspris pr. m²
            CASE
                WHEN p.size_sqm > 0 THEN ROUND(CAST(p.price AS REAL) / p.size_sqm, 0)
                ELSE NULL
            END AS sale_kr_per_sqm,
            -- Afkast via m²: median lejeleje-kr/m²/md × boligens m²
            ra_zip.price_per_sqm_median                          AS leje_kr_per_sqm_median,
            CASE
                WHEN p.size_sqm > 0 AND ra_zip.price_per_sqm_median IS NOT NULL
                THEN ROUND(ra_zip.price_per_sqm_median * p.size_sqm, 0)
                ELSE NULL
            END AS est_leje_sqm,
            CASE
                WHEN p.price > 0 AND p.size_sqm > 0 AND ra_zip.price_per_sqm_median IS NOT NULL
                THEN ROUND((ra_zip.price_per_sqm_median * p.size_sqm * 12.0) / p.price * 100, 0)
                ELSE NULL
            END AS yield_sqm_pct,
            -- Afkast via rum: median månedsleje for samme rum-antal i postnummeret
            ra_rooms.rent_total_median                           AS est_leje_rooms,
            CASE
                WHEN p.price > 0 AND ra_rooms.rent_total_median IS NOT NULL
                THEN ROUND((ra_rooms.rent_total_median * 12.0) / p.price * 100, 0)
                ELSE NULL
            END AS yield_rooms_pct
        FROM properties_for_sale p
        -- Join 1: generelt postnummer-aggregat til m²-beregning
        LEFT JOIN rental_aggregates ra_zip
            ON ra_zip.zip_code = p.zip_code
            AND ra_zip.rooms IS NULL
            AND ra_zip.property_type IS NULL
        -- Join 2: rum-specifikt aggregat
        LEFT JOIN rental_aggregates ra_rooms
            ON ra_rooms.zip_code = p.zip_code
            AND ra_rooms.rooms = p.rooms
            AND ra_rooms.property_type IS NULL
        WHERE p.price IS NOT NULL
        ORDER BY yield_sqm_pct DESC NULLS LAST, p.zip_code
        LIMIT {limit}
    """)
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        logger.warning("Ingen salgsboliger at eksportere")
        return 0

    # Slet og genskab arket for at undgå stale celleformatering
    try:
        old_ws = spreadsheet.worksheet("Boliger til salg")
        spreadsheet.del_worksheet(old_ws)
    except Exception:
        pass
    ws = spreadsheet.add_worksheet(title="Boliger til salg", rows=limit + 10, cols=25)
    now = datetime.now().strftime("%d-%m-%Y %H:%M")

    header = [
        "Adresse", "Postnummer", "By",
        "Pris (kr)", "Størrelse (m²)", "Rum", "Boligtype",
        "Ejerudgifter/md", "Energimærke", "Kr/m² (salg)",
        "Est. leje/md (m²)", "Afkast m² %",
        "Est. leje/md (rum)", "Afkast rum %",
        "Link", "Scraped", "Opdateret",
    ]
    data = [header]
    for r in rows:
        data.append([
            r["address"] or "",
            r["zip_code"] or "",
            r["city"] or "",
            r["price"] or "",
            r["size_sqm"] or "",
            r["rooms"] or "",
            r["property_type"] or "",
            r["owner_costs_monthly"] or "",
            r["energy_label"] or "",
            r["sale_kr_per_sqm"] or "",
            _fmt(r["est_leje_sqm"]) if r["est_leje_sqm"] else "",
            int(r["yield_sqm_pct"]) if r["yield_sqm_pct"] else "",
            _fmt(r["est_leje_rooms"]) if r["est_leje_rooms"] else "",
            int(r["yield_rooms_pct"]) if r["yield_rooms_pct"] else "",
            make_hyperlink(r["listing_url"]),
            (r["scraped_at"] or "")[:10],
            now,
        ])

    ws.update('A1', data, value_input_option='USER_ENTERED')
    ws.format('A1:Q1', {'textFormat': {'bold': True},
                        'backgroundColor': {'red': 0.95, 'green': 0.88, 'blue': 0.98}})
    ws.freeze(rows=1)

    logger.info(f"Boliger til salg: {len(data)-1} boliger eksporteret")
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
    results['huslejedata']        = export_sqm_aggregates(spreadsheet, db_path)
    results['husleje_pr_rum']     = export_pivot_by_rooms(spreadsheet, db_path)
    results['rådata']             = export_raw_listings(spreadsheet, db_path)
    results['boliger_til_salg']   = export_properties_for_sale(spreadsheet, db_path)
    return results


# Behold bagudkompatibel funktion som main.py kalder
def export_rental_aggregates_to_sheets(
    db_path: str, sheet_id: str, credentials_file: str,
    worksheet_name: str = "Huslejedata"
) -> int:
    results = export_all_sheets(db_path, sheet_id, credentials_file)
    return results.get('huslejedata', 0)
