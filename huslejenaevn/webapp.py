"""
huslejenaevn/webapp.py – Lokalt webinterface til søgning og filtrering af
huslejenævnsdata.

Standalone Flask-app (separat fra webapp/app.py) der udstiller
huslejenaevn_decisions-tabellen over en simpel JSON-API + en HTML-side.

Brug:
    python -m huslejenaevn.webapp
    # eller
    python main.py --huslejenaevn-webapp

Åbn: http://localhost:5051/

Den bruger den samme DB som resten af pipelinen (data/ejendom.db). Read-only
i praksis — vi har ingen mutationer i interfacet.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request, send_from_directory

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
DEFAULT_DB_PATH = BASE_DIR / "data" / "ejendom.db"
STATIC_DIR = Path(__file__).parent / "static"

# Whitelist over kolonner der må sorteres på — undgå SQL-injection via ORDER BY.
SORTABLE_COLUMNS = {
    "date_of_decision",
    "date_of_filing",
    "date_of_publishing",
    "municipality_name",
    "in_favour",
    "reason_for_closing",
}

# Whitelist for adressernes sorterbare kolonner.
ADDR_SORTABLE_COLUMNS = {
    "date_of_rent_determination",
    "municipality_name",
    "rent_raw",
    "gross_area",
    "postal_number",
}

# Whitelist for adress-enum-filtre.
ADDR_ENUM_FILTERS = {
    "method_of_rent_determination": [
        "BASED_ON_EXPENSES",
        "BASED_ON_COMPARABLE_RENTS",
        "BASED_ON_FREE_MARKET_PRICE",
        "NOT_SET",
    ],
    "declaration_of_rent": ["TENANCY_SPECIFIC_RENT", "EXAMPLE_RENT"],
    "category_of_rent": ["MONTHLY_RENT", "ANNUAL_RENT_PER_SQUARE_METERS"],
}

# Whitelist over enum-felter klienten må filtrere på (felt → SQL-kolonne).
# Vi tjekker værdien er én af de faktiske enum-værdier vi har set i basen,
# så ingen brugerinput rammer SQL'en direkte.
ENUM_FILTERS = {
    "in_favour":               ["TENANT", "LANDLORD", "SHARED", "NOT_SET"],
    "reason_for_closing":      ["IN_FAVOUR", "IN_PARTIAL_FAVOUR", "REJECTED"],
    "locally_inspected":       ["HELD", "NOT_HELD", "NOT_SET"],
    "general_public_interest": ["YES", "NO", "NOT_SET"],
    "imposition_of_fee":       ["YES", "NO", "NOT_SET"],
    "decisive_board":          ["RENT_BOARD", "RESIDENT_COMPLAINTS_BOARD"],
    "submitter":               ["TENANT", "LANDLORD", "NOT_SET"],
    "case_status":             ["OPEN", "CLOSED"],
}


def _db_path() -> Path:
    """Tillad override via env-var (HUSLEJENAEVN_DB_PATH) — ellers default."""
    import os
    p = os.environ.get("HUSLEJENAEVN_DB_PATH")
    return Path(p) if p else DEFAULT_DB_PATH


def _connect() -> sqlite3.Connection:
    """Read-only connection til DB."""
    db = _db_path()
    if not db.exists():
        raise FileNotFoundError(f"Database mangler: {db}")
    # uri=True + mode=ro gør det read-only — kan ikke skrive ved fejl.
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


# ─────────────────────────────────────────────
# Flask app
# ─────────────────────────────────────────────

app = Flask(__name__, static_folder=str(STATIC_DIR), static_url_path="/static")


@app.route("/")
def index():
    """Server hovedsiden."""
    return send_from_directory(str(STATIC_DIR), "index.html")


@app.route("/api/meta")
def api_meta():
    """
    Distinct-værdier til filter-dropdowns: kommuner, datoer, lov-hjemler,
    samt totale tællinger pr. status-felt (så frontend kan vise badges).
    """
    conn = _connect()
    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT municipality_name, COUNT(*) AS n
            FROM huslejenaevn_decisions
            WHERE municipality_name IS NOT NULL
            GROUP BY municipality_name
            ORDER BY n DESC
        """)
        municipalities = [{"name": r["municipality_name"], "count": r["n"]} for r in cur.fetchall()]

        cur.execute("""
            SELECT MIN(date_of_decision) AS min_d, MAX(date_of_decision) AS max_d
            FROM huslejenaevn_decisions
            WHERE date_of_decision IS NOT NULL
        """)
        date_row = cur.fetchone()
        date_range = {
            "min": (date_row["min_d"] or "")[:10],
            "max": (date_row["max_d"] or "")[:10],
        }

        cur.execute("SELECT COUNT(*) AS n FROM huslejenaevn_decisions")
        total = cur.fetchone()["n"]

        # Top lov-hjemler — udtræk lawText fra statutories_json. Vi laver det
        # i Python da det er en JSON-array — ingen behov for at indeksere.
        cur.execute("SELECT statutories_json FROM huslejenaevn_decisions WHERE statutories_json IS NOT NULL")
        from collections import Counter
        law_counter: Counter = Counter()
        for row in cur.fetchall():
            try:
                for s in json.loads(row["statutories_json"]):
                    lt = (s.get("lawText") or "").strip()
                    if lt:
                        law_counter[lt] += 1
            except Exception:
                pass
        top_laws = [{"name": k, "count": v} for k, v in law_counter.most_common(20)]

        # Breakdowns til filter-chips.
        breakdowns: dict[str, list[dict[str, Any]]] = {}
        for col in ENUM_FILTERS:
            cur.execute(f"""
                SELECT {col} AS v, COUNT(*) AS n
                FROM huslejenaevn_decisions
                WHERE {col} IS NOT NULL
                GROUP BY {col}
                ORDER BY n DESC
            """)
            breakdowns[col] = [{"value": r["v"], "count": r["n"]} for r in cur.fetchall()]

        return jsonify({
            "total_decisions": total,
            "municipalities": municipalities,
            "date_range": date_range,
            "top_laws": top_laws,
            "breakdowns": breakdowns,
        })
    finally:
        conn.close()


@app.route("/api/decisions")
def api_decisions():
    """
    Hovedsøgningen. Query-parametre:
      municipality      string, eksakt match på municipality_name
      date_from         YYYY-MM-DD
      date_to           YYYY-MM-DD
      in_favour         enum (se ENUM_FILTERS)
      reason_for_closing enum
      locally_inspected enum
      general_public_interest enum
      imposition_of_fee enum
      decisive_board    enum
      submitter         enum
      case_status       enum
      law               substring-match på statutories_json (lawText eller chapterText)
      q                 fri tekst — søger i case_identifier, serial_number
      sort              en af SORTABLE_COLUMNS, default date_of_decision
      order             "asc" | "desc", default "desc"
      limit             1..200, default 50
      offset            >= 0, default 0
    """
    conn = _connect()
    try:
        cur = conn.cursor()

        where_clauses: list[str] = []
        params: list[Any] = []

        # Eksakt match på kommune
        if (m := request.args.get("municipality")):
            where_clauses.append("municipality_name = ?")
            params.append(m)

        # Dato-interval (på date_of_decision)
        if (d_from := request.args.get("date_from")):
            where_clauses.append("date_of_decision >= ?")
            params.append(f"{d_from}T00:00:00.000Z")
        if (d_to := request.args.get("date_to")):
            where_clauses.append("date_of_decision <= ?")
            params.append(f"{d_to}T23:59:59.999Z")

        # Enum-filtre — kun whitelistede kombinationer slipper igennem.
        for col, allowed in ENUM_FILTERS.items():
            val = request.args.get(col)
            if val and val in allowed:
                where_clauses.append(f"{col} = ?")
                params.append(val)

        # Lov-hjemmel via JSON-substring (fungerer fordi lawText/chapterText
        # er klartekst i JSON'en). Sikkert nok mod SQL-injection — værdien
        # bindes som parameter til LIKE.
        if (law := request.args.get("law")):
            where_clauses.append("statutories_json LIKE ?")
            params.append(f"%{law}%")

        # Fri tekst — case-id og serial_number.
        if (q := request.args.get("q")):
            where_clauses.append(
                "(case_identifier LIKE ? OR serial_number LIKE ?)"
            )
            params.extend([f"%{q}%", f"%{q}%"])

        # Sort
        sort = request.args.get("sort", "date_of_decision")
        if sort not in SORTABLE_COLUMNS:
            sort = "date_of_decision"
        order = request.args.get("order", "desc").lower()
        if order not in ("asc", "desc"):
            order = "desc"

        try:
            limit = max(1, min(200, int(request.args.get("limit", 50))))
        except ValueError:
            limit = 50
        try:
            offset = max(0, int(request.args.get("offset", 0)))
        except ValueError:
            offset = 0

        where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        # Total count — brug samme WHERE.
        cur.execute(f"SELECT COUNT(*) AS n FROM huslejenaevn_decisions{where_sql}", params)
        total = cur.fetchone()["n"]

        # Records — sorteret + pagineret.
        # NULL'er sidst når desc, først når asc — det er typisk det forventede.
        cur.execute(
            f"""
            SELECT id, api_id, serial_number, case_identifier, case_status,
                   municipality_name, date_of_filing, date_of_decision,
                   date_of_publishing, submitter, decisive_board, in_favour,
                   reason_for_closing, locally_inspected, general_public_interest,
                   imposition_of_fee, decision_document_id
            FROM huslejenaevn_decisions
            {where_sql}
            ORDER BY {sort} {order.upper()}, id {order.upper()}
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        )
        rows = [dict(r) for r in cur.fetchall()]

        # Strip tid fra dato-felter til klart-format.
        for r in rows:
            for f in ("date_of_filing", "date_of_decision", "date_of_publishing"):
                if r.get(f):
                    r[f] = r[f][:10]

        return jsonify({
            "total": total,
            "limit": limit,
            "offset": offset,
            "items": rows,
        })
    finally:
        conn.close()


@app.route("/api/decision/<api_id>")
def api_decision_detail(api_id: str):
    """
    Hent én afgørelse med statutories + raw_json udfoldet.
    """
    conn = _connect()
    try:
        row = conn.execute(
            """
            SELECT * FROM huslejenaevn_decisions WHERE api_id = ?
            """,
            (api_id,),
        ).fetchone()
        if not row:
            return jsonify({"error": "not found"}), 404

        d = dict(row)
        # Parse JSON-felter til strukturerede objekter.
        for f in ("statutories_json", "subjects_json", "raw_json"):
            if d.get(f):
                try:
                    d[f.replace("_json", "")] = json.loads(d[f])
                except Exception:
                    d[f.replace("_json", "")] = None
            d.pop(f, None)

        # Strip tid fra ISO-datoer for klar visning.
        for f in ("date_of_filing", "date_of_decision", "date_of_publishing"):
            if d.get(f):
                d[f] = d[f][:10]

        # Decision-PDF URL hvis vi har et document-id (frontend-konvention).
        if d.get("decision_document_id"):
            d["pdf_url"] = (
                f"https://husleje.huslejenaevn.dk/document/{d['decision_document_id']}"
            )

        return jsonify(d)
    finally:
        conn.close()


@app.route("/api/meta/addresses")
def api_meta_addresses():
    """
    Metadata til adresse-fanen: kommuner, metoder, antal adresser.
    """
    conn = _connect()
    try:
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) AS n FROM huslejenaevn_addresses")
        total = cur.fetchone()["n"]

        cur.execute("""
            SELECT municipality_name, COUNT(*) AS n
            FROM huslejenaevn_addresses
            WHERE municipality_name IS NOT NULL
            GROUP BY municipality_name
            ORDER BY n DESC
        """)
        municipalities = [{"name": r["municipality_name"], "count": r["n"]} for r in cur.fetchall()]

        cur.execute("""
            SELECT MIN(date_of_rent_determination) AS min_d,
                   MAX(date_of_rent_determination) AS max_d
            FROM huslejenaevn_addresses
            WHERE date_of_rent_determination IS NOT NULL
        """)
        date_row = cur.fetchone()
        date_range = {
            "min": (date_row["min_d"] or "")[:10],
            "max": (date_row["max_d"] or "")[:10],
        }

        # Breakdowns til filter-chips.
        breakdowns: dict[str, list[dict]] = {}
        for col in ADDR_ENUM_FILTERS:
            cur.execute(f"""
                SELECT {col} AS v, COUNT(*) AS n
                FROM huslejenaevn_addresses
                WHERE {col} IS NOT NULL
                GROUP BY {col}
                ORDER BY n DESC
            """)
            breakdowns[col] = [{"value": r["v"], "count": r["n"]} for r in cur.fetchall()]

        return jsonify({
            "total_addresses": total,
            "municipalities": municipalities,
            "date_range": date_range,
            "breakdowns": breakdowns,
        })
    finally:
        conn.close()


@app.route("/api/addresses")
def api_addresses():
    """
    Søgning i huslejenaevn_addresses.

    Query-parametre:
      municipality      string — eksakt match på municipality_name
      postal_number     heltal
      date_from         YYYY-MM-DD — på date_of_rent_determination
      date_to           YYYY-MM-DD
      method            method_of_rent_determination enum
      declaration       declaration_of_rent enum
      category          category_of_rent enum
      q                 fri tekst — søger i designation, street_name
      sort              en af ADDR_SORTABLE_COLUMNS, default date_of_rent_determination
      order             "asc" | "desc", default "desc"
      limit             1..200, default 50
      offset            >= 0, default 0
    """
    conn = _connect()
    try:
        cur = conn.cursor()

        where_clauses: list[str] = []
        params: list[Any] = []

        if (m := request.args.get("municipality")):
            where_clauses.append("municipality_name = ?")
            params.append(m)

        if (pn := request.args.get("postal_number")):
            try:
                where_clauses.append("postal_number = ?")
                params.append(int(pn))
            except ValueError:
                pass

        if (d_from := request.args.get("date_from")):
            where_clauses.append("date_of_rent_determination >= ?")
            params.append(f"{d_from}T00:00:00.000Z")
        if (d_to := request.args.get("date_to")):
            where_clauses.append("date_of_rent_determination <= ?")
            params.append(f"{d_to}T23:59:59.999Z")

        # Enum-filtre — whitelist.
        for col, allowed in ADDR_ENUM_FILTERS.items():
            # Map fra kortere query-param til SQL-kolonne.
            qp_map = {
                "method_of_rent_determination": "method",
                "declaration_of_rent": "declaration",
                "category_of_rent": "category",
            }
            val = request.args.get(qp_map.get(col, col))
            if val and val in allowed:
                where_clauses.append(f"{col} = ?")
                params.append(val)

        if (q := request.args.get("q")):
            where_clauses.append(
                "(designation LIKE ? OR street_name LIKE ?)"
            )
            params.extend([f"%{q}%", f"%{q}%"])

        sort = request.args.get("sort", "date_of_rent_determination")
        if sort not in ADDR_SORTABLE_COLUMNS:
            sort = "date_of_rent_determination"
        order = request.args.get("order", "desc").lower()
        if order not in ("asc", "desc"):
            order = "desc"

        try:
            limit = max(1, min(200, int(request.args.get("limit", 50))))
        except ValueError:
            limit = 50
        try:
            offset = max(0, int(request.args.get("offset", 0)))
        except ValueError:
            offset = 0

        where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        cur.execute(f"SELECT COUNT(*) AS n FROM huslejenaevn_addresses{where_sql}", params)
        total = cur.fetchone()["n"]

        cur.execute(
            f"""
            SELECT api_id, designation, street_name, street_building_identifier,
                   floor_identifier, suite_identifier, postal_number,
                   municipality_name, municipality_code,
                   method_of_rent_determination, declaration_of_rent, category_of_rent,
                   rent_raw, rent_amount_monthly, rent_amount_annual_per_sqm,
                   gross_area, date_of_rent_determination
            FROM huslejenaevn_addresses
            {where_sql}
            ORDER BY {sort} {order.upper()}, id {order.upper()}
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        )
        rows = [dict(r) for r in cur.fetchall()]

        # Beregn kr/måned hvis API'et ikke gav det — frontend-konvention: rent_raw * gross_area / 12.
        for r in rows:
            if r.get("date_of_rent_determination"):
                r["date_of_rent_determination"] = r["date_of_rent_determination"][:10]
            if r.get("rent_amount_monthly") is None and r.get("rent_raw") and r.get("gross_area"):
                r["rent_monthly_calc"] = round(r["rent_raw"] * r["gross_area"] / 12)
            else:
                r["rent_monthly_calc"] = r.get("rent_amount_monthly")

        return jsonify({
            "total": total,
            "limit": limit,
            "offset": offset,
            "items": rows,
        })
    finally:
        conn.close()


def main():
    """Entry-point når man kører `python -m huslejenaevn.webapp`."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    db = _db_path()
    if not db.exists():
        print(f"FEJL: Database ikke fundet på {db}")
        print("Kør først: python main.py --huslejenaevn-sync")
        return
    print(f"→ Bruger DB: {db}")
    print("→ Webapp kører på http://localhost:5051/")
    print("  (Ctrl+C for at stoppe)")
    # debug=False så reload ikke roder med Flask-state. Host 0.0.0.0 hvis du
    # vil tilgå det fra andre maskiner på LAN'et.
    app.run(host="127.0.0.1", port=5051, debug=False)


if __name__ == "__main__":
    main()
