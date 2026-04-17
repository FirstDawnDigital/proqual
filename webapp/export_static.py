"""
webapp/export_static.py — Eksporterer ejendomsdata som selvstændig statisk HTML-fil.

Den genererede fil:
  • Indeholder al data bagt ind som JSON (ingen server nødvendig)
  • Bruger localStorage til annotations (favoritter, renoveringsestimater)
  • Kan hostes gratis på GitHub Pages, Netlify eller Cloudflare Pages

Brug:
  python webapp/export_static.py
  python webapp/export_static.py --output docs/index.html   # GitHub Pages
  python webapp/export_static.py --output /tmp/preview.html
"""

import json
import re
import sqlite3
import sys
import argparse
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR   = Path(__file__).parent.parent
DB_PATH    = BASE_DIR / "data" / "ejendom.db"
INDEX_HTML = Path(__file__).parent / "index.html"
OUTPUT_DIR = BASE_DIR / "docs"          # GitHub Pages standard-mappe
OUTPUT_FILE = OUTPUT_DIR / "index.html"


# ── Query (identisk med app.py) ───────────────────────────────────────────────
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


# ── Hent data fra SQLite ──────────────────────────────────────────────────────
def load_data(db_path: Path) -> tuple[list, dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(QUERY).fetchall()
        properties = [dict(r) for r in rows]

        types = [r[0] for r in conn.execute(
            "SELECT DISTINCT property_type FROM properties_for_sale "
            "WHERE property_type IS NOT NULL ORDER BY property_type"
        ).fetchall()]
        count = conn.execute(
            "SELECT COUNT(*) FROM properties_for_sale "
            "WHERE price IS NOT NULL AND (is_active IS NULL OR is_active = 1)"
        ).fetchone()[0]
        scraped_at = conn.execute(
            "SELECT MAX(scraped_at) FROM properties_for_sale"
        ).fetchone()[0]

        meta = {
            "property_types": types,
            "total": count,
            "scraped_at": scraped_at,
        }
        return properties, meta
    finally:
        conn.close()


# ── Ny loadData()-funktion til statisk brug ───────────────────────────────────
STATIC_LOAD_DATA = r"""  // ── Load data (STATISK — data er bagt ind i denne fil) ──────────────────
  async function loadData() {
    try {
      const data = window.__STATIC_DATA__;
      allData    = data.properties;
      const meta = data.meta;

      // Annotations fra localStorage (gemmes lokalt i browseren)
      try {
        const stored = localStorage.getItem("ejendom_annotations");
        if (stored) annotations = JSON.parse(stored);
      } catch (e) {
        console.warn("Kunne ikke indlæse annotations fra localStorage:", e);
      }

      const sel = document.getElementById("f-type");
      meta.property_types.forEach(t => {
        const o = document.createElement("option");
        o.value = t; o.textContent = (TYPE_ICON[t]||"") + "  " + (TYPE_DA[t]||t);
        sel.appendChild(o);
      });
      const dt = meta.scraped_at ? meta.scraped_at.slice(0,16).replace("T"," ") : "–";
      const exp = data.exported_at ? data.exported_at.slice(0,10) : "";
      document.getElementById("meta-info").innerHTML =
        `${meta.total} boliger<br><span style="font-size:10px">Data fra ${dt}${exp ? " · Eksport "+exp : ""}</span>`;
      document.getElementById("loading").style.display = "none";
      renderTable();
    } catch (err) {
      document.getElementById("loading").innerHTML =
        `<span style="color:#dc2626">⚠️ Fejl ved indlæsning af data: ${err.message}</span>`;
    }
  }

"""

# ── Ny saveAnnotation()-funktion til statisk brug ─────────────────────────────
STATIC_SAVE_ANNOTATION = r"""  // ── Save annotation (STATISK — gemmes i browserens localStorage) ─────────
  async function saveAnnotation(id, patch) {
    const current = getAnnotation(id);
    const updated = { ...current, ...patch };
    annotations[id] = updated;
    try {
      localStorage.setItem("ejendom_annotations", JSON.stringify(annotations));
    } catch (e) {
      console.warn("localStorage ikke tilgængeligt — annotation gemmes kun i session:", e);
    }
  }

"""


# ── Patch HTML ────────────────────────────────────────────────────────────────
def patch_html(html: str, properties: list, meta: dict) -> str:
    """
    1. Injicerer __STATIC_DATA__ som <script> lige før det eksisterende <script>
    2. Erstatter loadData() med statisk version
    3. Erstatter saveAnnotation() med localStorage-version
    4. Fjerner fejlbeskeden der nævner Flask-serveren
    """

    # ── 1. Injicér data ──────────────────────────────────────────────────────
    exported_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    static_data = {
        "properties": properties,
        "meta":       meta,
        "exported_at": exported_at,
    }
    data_json = json.dumps(static_data, ensure_ascii=False, separators=(",", ":"))

    data_script = (
        f'\n<script>\n'
        f'/* Ejendomsdata bagt ind ved eksport {exported_at} */\n'
        f'window.__STATIC_DATA__ = {data_json};\n'
        f'</script>\n'
    )

    # Indsæt data-scriptet lige før det primære <script>-tag
    html = html.replace("\n<script>\n", data_script + "\n<script>\n", 1)

    # ── 2. Erstat loadData() ──────────────────────────────────────────────────
    # Matcher fra load-data-kommentaren til save-annotation-kommentaren
    load_pattern = re.compile(
        r'  // ── Load data ─+\n  async function loadData\(\).*?(?=  // ── Save annotation)',
        re.DOTALL,
    )
    if not load_pattern.search(html):
        print("⚠️  Advarsel: Kunne ikke finde loadData()-funktionen i index.html — springer over patching.")
    else:
        html = load_pattern.sub(STATIC_LOAD_DATA, html)

    # ── 3. Erstat saveAnnotation() ────────────────────────────────────────────
    save_pattern = re.compile(
        r'  // ── Save annotation ─+\n  async function saveAnnotation.*?(?=  // ── Cycle status)',
        re.DOTALL,
    )
    if not save_pattern.search(html):
        print("⚠️  Advarsel: Kunne ikke finde saveAnnotation()-funktionen — springer over patching.")
    else:
        html = save_pattern.sub(STATIC_SAVE_ANNOTATION, html)

    # ── 4. Opdater <title> med dato ──────────────────────────────────────────
    html = html.replace(
        "<title>Ejendomsinvestering</title>",
        f"<title>Ejendomsinvestering — {exported_at[:10]}</title>",
    )

    return html


# ── Gem fil ───────────────────────────────────────────────────────────────────
def export_static(db_path: Path = DB_PATH, output: Path = OUTPUT_FILE) -> Path:
    """Eksportér statisk HTML. Returnerer stien til den genererede fil."""
    if not db_path.exists():
        sys.exit(f"❌ Database ikke fundet: {db_path}")
    if not INDEX_HTML.exists():
        sys.exit(f"❌ index.html ikke fundet: {INDEX_HTML}")

    print(f"Henter data fra {db_path.name}…")
    properties, meta = load_data(db_path)
    print(f"  → {len(properties)} boliger, {len(meta['property_types'])} boligtyper")

    html = INDEX_HTML.read_text(encoding="utf-8")
    patched = patch_html(html, properties, meta)

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(patched, encoding="utf-8")

    size_kb = output.stat().st_size / 1024
    print(f"✓ Statisk HTML genereret: {output}  ({size_kb:.0f} KB)")
    return output


# ── Indgangspunkt ─────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Eksportér ejendomsdata til statisk HTML")
    parser.add_argument(
        "--output", "-o",
        default=str(OUTPUT_FILE),
        help=f"Sti til output-fil (default: {OUTPUT_FILE})",
    )
    parser.add_argument(
        "--db",
        default=str(DB_PATH),
        help=f"Sti til SQLite-database (default: {DB_PATH})",
    )
    args = parser.parse_args()

    export_static(db_path=Path(args.db), output=Path(args.output))
    print("\nTip: Push 'docs/' til GitHub og aktiver GitHub Pages (branch: main, mappe: /docs)")


if __name__ == "__main__":
    main()
