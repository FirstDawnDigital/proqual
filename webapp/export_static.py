"""
webapp/export_static.py — Eksporterer ejendomsdata som selvstændig statisk HTML-fil.

DEPLOY: køres automatisk af deploy.sh og launchd-pipeline.
Kræver ingen manuel kommando — opdateres ved `bash deploy.sh`.

Den genererede fil:
  • Henter ejendomsdata fra Cloudflare Worker (kræver WORKER_API_KEY i config.env)
  • Falder tilbage til lokal SQLite hvis Worker ikke er tilgængelig
  • Indeholder al ejendomsdata bagt ind som JSON (ingen server nødvendig)
  • Bruger Cloudflare Worker til annotations (synket med Turso)
  • Kan hostes gratis på GitHub Pages

Brug:
  python webapp/export_static.py                        # privat version (password + Cloudflare)
  python webapp/export_static.py --public               # public demo (ingen password, localStorage)
  python webapp/export_static.py --output docs/index.html

config.env skal indeholde:
  WORKER_API_KEY=<webapp-adgangskode>   ← bruges til at kalde Cloudflare Worker
"""

import json
import os
import re
import sqlite3
import sys
import argparse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR    = Path(__file__).parent.parent
DB_PATH     = BASE_DIR / "data" / "ejendom.db"
INDEX_HTML  = Path(__file__).parent / "index.html"
OUTPUT_DIR  = BASE_DIR / "docs"          # GitHub Pages standard-mappe
OUTPUT_FILE = OUTPUT_DIR / "index.html"

WORKER_URL_DEFAULT = "https://proqual-api.proqual.workers.dev"


# ── Fallback-query (bruges kun hvis Worker ikke er tilgængelig) ───────────────
# VIGTIGT: denne kopi opdateres ikke automatisk — Worker er primær datakilde.
# Opdatér kun ved behov for at holde den i sync med cloudflare/worker.js MAIN_QUERY.
FALLBACK_QUERY = """
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
    p.days_on_market,
    p.price_change_count,
    p.price_change_amount,
    ra_rooms.rent_total_median AS est_leje_rooms,
    ra_rooms.rent_total_low    AS est_leje_rooms_low,
    ra_rooms.rent_total_high   AS est_leje_rooms_high,
    ra_rooms.sample_size       AS lejedata_rum_antal,
    CASE WHEN p.price > 0 AND ra_rooms.rent_total_median IS NOT NULL
         THEN ROUND((ra_rooms.rent_total_median * 12.0) / p.price * 100, 1)
         ELSE NULL END AS yield_rooms_pct,
    -- V2-model: forventet_leje = 4252 + b_zip × size_sqm
    CASE WHEN p.size_sqm > 0
         THEN ROUND(4252.0 + COALESCE(zr.b_zip,
              (SELECT AVG(b_zip) FROM rental_zip_rates)) * p.size_sqm, 0)
         ELSE NULL END AS est_leje_v2,
    CASE WHEN p.price > 0 AND p.size_sqm > 0
         THEN ROUND((4252.0 + COALESCE(zr.b_zip,
              (SELECT AVG(b_zip) FROM rental_zip_rates)) * p.size_sqm) * 12.0 / p.price * 100, 1)
         ELSE NULL END AS yield_v2_pct,
    COALESCE(zr.sample_size, 0) AS lejedata_v2_antal
FROM properties_for_sale p
LEFT JOIN rental_aggregates ra_zip
    ON ra_zip.zip_code = p.zip_code
    AND ra_zip.rooms IS NULL
    AND ra_zip.property_type IS NULL
LEFT JOIN rental_aggregates ra_rooms
    ON ra_rooms.zip_code = p.zip_code
    AND ra_rooms.rooms = p.rooms
    AND ra_rooms.property_type IS NULL
LEFT JOIN rental_zip_rates zr
    ON zr.zip_code = p.zip_code
WHERE p.price IS NOT NULL
  AND (p.is_active IS NULL OR p.is_active = 1)
ORDER BY yield_v2_pct DESC NULLS LAST, yield_sqm_pct DESC NULLS LAST
"""


# ── Worker-konfiguration ──────────────────────────────────────────────────────
def _load_worker_config() -> tuple[str, str]:
    """Henter WORKER_URL og WORKER_API_KEY fra miljø eller config.env."""
    url = os.environ.get("WORKER_URL", WORKER_URL_DEFAULT)
    key = os.environ.get("WORKER_API_KEY", "")
    if not key:
        env_file = BASE_DIR / "config.env"
        if env_file.exists():
            for line in env_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line.startswith("WORKER_API_KEY="):
                    key = line.split("=", 1)[1].strip()
                elif line.startswith("WORKER_URL="):
                    url = line.split("=", 1)[1].strip()
    return url, key


def _fetch_from_worker(worker_url: str, api_key: str) -> tuple[list, dict]:
    """Henter ejendomsdata fra Cloudflare Worker API."""
    def _get(path: str):
        req = urllib.request.Request(
            f"{worker_url}{path}",
            headers={
                "X-API-Key":  api_key,
                "User-Agent": "Mozilla/5.0 (compatible; ejendompython-exporter/1.0)",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())

    properties = _get("/api/properties")
    meta_raw   = _get("/api/meta")
    meta = {
        "property_types": meta_raw.get("property_types", []),
        "total":          meta_raw.get("total", len(properties)),
        "scraped_at":     meta_raw.get("scraped_at"),
    }
    return properties, meta


def _load_from_sqlite(db_path: Path) -> tuple[list, dict]:
    """Fallback: henter data direkte fra lokal SQLite med FALLBACK_QUERY."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows       = conn.execute(FALLBACK_QUERY).fetchall()
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
        return properties, {"property_types": types, "total": count, "scraped_at": scraped_at}
    finally:
        conn.close()


# ── Hent data: Worker først, SQLite som fallback ──────────────────────────────
def load_data(db_path: Path) -> tuple[list, dict]:
    """
    Primær kilde: Cloudflare Worker (kræver WORKER_API_KEY i config.env).
    Fordel: Worker er single source of truth — ingen duplikat SQL-query.
    Fallback: lokal SQLite med FALLBACK_QUERY (viser advarsel).
    """
    worker_url, api_key = _load_worker_config()
    if api_key:
        try:
            print("  → Henter fra Cloudflare Worker…")
            properties, meta = _fetch_from_worker(worker_url, api_key)
            print(f"  → {len(properties)} boliger hentet fra Worker ✓")
            return properties, meta
        except Exception as e:
            print(f"  ⚠️  Worker fejlede ({e}) — falder tilbage til lokal SQLite")
    else:
        print("  ⚠️  WORKER_API_KEY ikke sat i config.env — falder tilbage til lokal SQLite")
        print("       Tilføj: WORKER_API_KEY=<webapp-adgangskode> i config.env")

    return _load_from_sqlite(db_path)


# ── Ny loadData()-funktion til statisk brug ───────────────────────────────────
STATIC_LOAD_DATA = r"""  // ── Load data (STATISK — data er bagt ind i denne fil) ──────────────────
  async function loadData() {
    try {
      const data = window.__STATIC_DATA__;
      allData    = data.properties;
      const meta = data.meta;

      // Annotations fra Cloudflare Worker (samme Turso som live-versionen)
      try {
        const annRes = await apiFetch("/api/annotations");
        if (annRes.ok) {
          annotations = await annRes.json();
          annotationsLoaded = true;
        } else {
          console.warn("Annotations endpoint svarede", annRes.status, "— gem er deaktiveret");
        }
      } catch (e) {
        console.warn("Kunne ikke indlæse annotations fra Worker:", e);
      }

      buildTypeDropdown(meta.property_types);
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
STATIC_SAVE_ANNOTATION = r"""  // ── Save annotation (STATISK — gemmes i Turso via Cloudflare Worker) ───────
  async function saveAnnotation(id, patch) {
    const current = getAnnotation(id);
    const updated = { ...current, ...patch };
    annotations[id] = updated;
    await apiFetch(`/api/annotations/${id}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(updated),
    });
  }

"""


# ── Public demo: loadData + saveAnnotation med localStorage ──────────────────
PUBLIC_LOAD_DATA = r"""  // ── Load data (STATISK PUBLIC — data er bagt ind, annotations i localStorage) ──
  async function loadData() {
    try {
      const data = window.__STATIC_DATA__;
      allData    = data.properties;
      const meta = data.meta;

      // Annotations fra localStorage (lokal browser — ikke synket)
      try {
        const stored = localStorage.getItem("ejendom_annotations_demo");
        if (stored) annotations = JSON.parse(stored);
      } catch (e) {
        console.warn("localStorage ikke tilgængeligt:", e);
      }

      buildTypeDropdown(meta.property_types);
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

PUBLIC_SAVE_ANNOTATION = r"""  // ── Save annotation (PUBLIC — gemmes kun i denne browser) ────────────────
  async function saveAnnotation(id, patch) {
    const current = getAnnotation(id);
    const updated = { ...current, ...patch };
    annotations[id] = updated;
    try {
      localStorage.setItem("ejendom_annotations_demo", JSON.stringify(annotations));
    } catch (e) {
      console.warn("localStorage ikke tilgængeligt:", e);
    }
  }

"""

# ── Patch HTML ────────────────────────────────────────────────────────────────
def patch_html(html: str, properties: list, meta: dict, public: bool = False) -> str:
    """
    1. Injicerer __STATIC_DATA__ som <script> lige før det eksisterende <script>
    2. Erstatter loadData() med statisk version
    3. Erstatter saveAnnotation() med Cloudflare- eller localStorage-version
    4. Ved public=True: fjerner password-prompt og boot-check
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
    load_replacement = PUBLIC_LOAD_DATA if public else STATIC_LOAD_DATA
    load_pattern = re.compile(
        r'  // ── Load data ─+\n  async function loadData\(\).*?(?=  // ── Save annotation)',
        re.DOTALL,
    )
    if not load_pattern.search(html):
        print("⚠️  Advarsel: Kunne ikke finde loadData()-funktionen i index.html — springer over patching.")
    else:
        html = load_pattern.sub(load_replacement, html)

    # ── 3. Erstat saveAnnotation() ────────────────────────────────────────────
    save_replacement = PUBLIC_SAVE_ANNOTATION if public else STATIC_SAVE_ANNOTATION
    save_pattern = re.compile(
        r'  // ── Save annotation ─+\n  async function saveAnnotation.*?(?=  // ── Cycle status)',
        re.DOTALL,
    )
    if not save_pattern.search(html):
        print("⚠️  Advarsel: Kunne ikke finde saveAnnotation()-funktionen — springer over patching.")
    else:
        html = save_pattern.sub(save_replacement, html)

    # ── 4. Public: fjern password-prompt og boot direkte til loadData() ───────
    if public:
        # Erstat boot-sekvensen med direkte loadData()-kald
        html = re.sub(
            r'  if \(getApiKey\(\)\) \{[^}]+\} else \{[^}]+\}',
            '  loadData();',
            html,
            flags=re.DOTALL,
        )

    # ── 4. Opdater <title> med dato ──────────────────────────────────────────
    html = html.replace(
        "<title>Ejendomsinvestering</title>",
        f"<title>Ejendomsinvestering — {exported_at[:10]}</title>",
    )

    return html


# ── Gem fil ───────────────────────────────────────────────────────────────────
def export_static(db_path: Path = DB_PATH, output: Path = OUTPUT_FILE,
                  public: bool = False) -> Path:
    """Eksportér statisk HTML. Returnerer stien til den genererede fil."""
    if not db_path.exists():
        sys.exit(f"❌ Database ikke fundet: {db_path}")
    if not INDEX_HTML.exists():
        sys.exit(f"❌ index.html ikke fundet: {INDEX_HTML}")

    print(f"Henter data fra {db_path.name}…")
    properties, meta = load_data(db_path)
    print(f"  → {len(properties)} boliger, {len(meta['property_types'])} boligtyper")

    html = INDEX_HTML.read_text(encoding="utf-8")
    patched = patch_html(html, properties, meta, public=public)

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(patched, encoding="utf-8")

    size_kb = output.stat().st_size / 1024
    label = "public demo" if public else "statisk HTML"
    print(f"✓ {label} genereret: {output}  ({size_kb:.0f} KB)")
    return output


# ── Indgangspunkt ─────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Eksportér ejendomsdata til statisk HTML")
    parser.add_argument(
        "--output", "-o",
        help="Sti til output-fil (default: docs/index.html, eller docs/demo.html ved --public)",
    )
    parser.add_argument(
        "--db",
        default=str(DB_PATH),
        help=f"Sti til SQLite-database (default: {DB_PATH})",
    )
    parser.add_argument(
        "--public",
        action="store_true",
        help="Generer public demo uden password og med localStorage-annotations",
    )
    args = parser.parse_args()

    if args.output:
        output = Path(args.output)
    elif args.public:
        output = OUTPUT_DIR / "demo.html"
    else:
        output = OUTPUT_FILE

    export_static(db_path=Path(args.db), output=output, public=args.public)
    if args.public:
        print(f"\nPublic demo URL: https://firstdawndigital.github.io/proqual/demo.html")
    else:
        print("\nTip: Push 'docs/' til GitHub og aktiver GitHub Pages (branch: main, mappe: /docs)")


if __name__ == "__main__":
    main()
