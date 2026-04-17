"""
database.py – SQLite databaseopsætning og hjælpefunktioner
"""
import sqlite3
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


def get_connection(db_path: str) -> sqlite3.Connection:
    """Returnér en databaseforbindelse med row_factory sat til dict-lignende adgang."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")  # bedre concurrent adgang
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def migrate_database(db_path: str) -> None:
    """
    Sikker migration: tilføj nye kolonner til eksisterende DB uden at miste data.
    Kaldes automatisk fra initialize_database().
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    migrations = {
        # tabel → liste af (kolonnenavn, SQL-type, default-udtryk)
        "rental_listings": [
            ("first_seen",          "TEXT",    "scraped_at"),
            ("last_seen",           "TEXT",    "scraped_at"),
            ("relist_count",        "INTEGER", "0"),
            ("price_change_count",  "INTEGER", "0"),
            # NULL = aldrig verificeret via live-tjek. Sættes KUN af listing_checker.
            ("last_checked",        "TEXT",    "NULL"),
        ],
        "properties_for_sale": [
            ("days_on_market",      "INTEGER", "NULL"),
            ("price_change_count",  "INTEGER", "NULL"),
            ("price_change_amount", "INTEGER", "NULL"),
            ("latitude",            "REAL",    "NULL"),
            ("longitude",           "REAL",    "NULL"),
            ("commute_minutes",     "INTEGER", "NULL"),
        ],
    }

    added = []
    for table, new_columns in migrations.items():
        cursor.execute(f"PRAGMA table_info({table})")
        existing = {row["name"] for row in cursor.fetchall()}
        for col, col_type, default_expr in new_columns:
            if col not in existing:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
                if default_expr != "NULL":
                    cursor.execute(f"UPDATE {table} SET {col} = {default_expr} WHERE {col} IS NULL")
                added.append(f"{table}.{col}")

    conn.commit()
    conn.close()

    if added:
        logger.info(f"DB-migration: tilføjet kolonner: {', '.join(added)}")


def initialize_database(db_path: str) -> None:
    """Opret alle tabeller hvis de ikke eksisterer, og kør migration."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    conn = get_connection(db_path)
    cursor = conn.cursor()

    # Tabel: Individuelle lejeboliger (rådata)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rental_listings (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            source            TEXT NOT NULL,         -- 'boligportal', 'lejebolig', 'manual'
            listing_id        TEXT,                  -- eksternt ID fra kilden
            address           TEXT,
            zip_code          TEXT NOT NULL,
            city              TEXT,
            rent_monthly      INTEGER,               -- månedlig leje i kr
            size_sqm          REAL,                  -- størrelse i m²
            rooms             INTEGER,               -- antal værelser
            property_type     TEXT,                  -- 'lejlighed', 'villa', 'rækkehus'
            deposit           INTEGER,
            available_from    TEXT,                  -- ISO date string
            listing_url       TEXT,
            scraped_at        TEXT DEFAULT (datetime('now')),
            email_received_at TEXT,
            is_active         INTEGER DEFAULT 1,
            first_seen         TEXT,                  -- første gang set i email
            last_seen          TEXT,                  -- seneste gang set i email
            relist_count       INTEGER DEFAULT 0,     -- antal re-listings (rød flag)
            price_change_count INTEGER DEFAULT 0,     -- antal prisændringer (rød flag)
            last_checked       TEXT,                  -- seneste live-tjek (NULL = aldrig verificeret)
            UNIQUE(source, listing_id)                -- undgå dubletter
        )
    """)

    # Tabel: Aggregerede huslejepriser pr. postnummer (beregnet)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rental_aggregates (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            zip_code             TEXT NOT NULL,
            rooms                INTEGER,           -- NULL = alle rum samlet
            property_type        TEXT,              -- NULL = alle typer
            price_per_sqm_low    REAL,              -- 25. percentil
            price_per_sqm_median REAL,              -- median
            price_per_sqm_high   REAL,              -- 75. percentil
            rent_total_low       REAL,              -- total månedsleje lav
            rent_total_median    REAL,              -- total månedsleje median
            rent_total_high      REAL,              -- total månedsleje høj
            sample_size          INTEGER,
            calculated_at        TEXT DEFAULT (datetime('now'))
        )
    """)

    # Tabel: Boliger til salg
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS properties_for_sale (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            source               TEXT NOT NULL,     -- 'boligsiden', 'boliga', 'manual'
            listing_id           TEXT,
            address              TEXT,
            zip_code             TEXT,
            city                 TEXT,
            price                INTEGER,           -- kontantpris i kr
            size_sqm             REAL,
            rooms                INTEGER,
            property_type        TEXT,
            owner_costs_monthly  INTEGER,           -- ejerudgifter pr. måned
            energy_label         TEXT,
            listing_url          TEXT,
            scraped_at           TEXT DEFAULT (datetime('now')),
            is_active            INTEGER DEFAULT 1,
            -- Markedsdata (fra API)
            days_on_market       INTEGER,           -- dage siden første annoncering
            price_change_count   INTEGER,           -- antal prisændringer
            price_change_amount  INTEGER,           -- samlet prisændring i kr (negativ = reduktion)
            -- Geolokation (fra API eller Nominatim-geocoding)
            latitude             REAL,
            longitude            REAL,
            -- Køretid (beregnet separat med --enrich-commute)
            commute_minutes      INTEGER,           -- køretid i bil til Egevangen 19
            UNIQUE(source, listing_id)
        )
    """)

    # Indeks for hurtigere opslag
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_rental_zip ON rental_listings(zip_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_rental_rooms ON rental_listings(rooms)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_agg_zip ON rental_aggregates(zip_code)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sale_zip ON properties_for_sale(zip_code)")

    conn.commit()
    conn.close()
    logger.info(f"Database initialiseret: {db_path}")

    # Kør migration så eksisterende DB'er får de nye kolonner
    migrate_database(db_path)


def insert_rental_listing(db_path: str, listing: dict) -> bool:
    """
    Upsert-logik for en lejebolig. Returnér True hvis det var en ny listing.

    Ved gensyn (samme source + listing_id):
    - Opdaterer last_seen
    - Tæller prisændring hvis rent_monthly er ændret
    - Tæller re-listing hvis listing var markeret inaktiv
    - Sætter is_active = 1 igen

    Hvis listing_id er None: indsættes altid (kan ikke deduplere).
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()
    now = datetime.now().isoformat(timespec='seconds')

    source     = listing.get('source')
    listing_id = listing.get('listing_id')
    new_rent   = listing.get('rent_monthly')

    try:
        # ── Ingen listing_id: altid INSERT ──
        if not listing_id:
            cursor.execute("""
                INSERT INTO rental_listings
                    (source, listing_id, address, zip_code, city,
                     rent_monthly, size_sqm, rooms, property_type,
                     deposit, available_from, listing_url, email_received_at,
                     first_seen, last_seen, relist_count, price_change_count)
                VALUES
                    (:source, :listing_id, :address, :zip_code, :city,
                     :rent_monthly, :size_sqm, :rooms, :property_type,
                     :deposit, :available_from, :listing_url, :email_received_at,
                     :now, :now, 0, 0)
            """, {**listing, 'now': now})
            conn.commit()
            return True

        # ── Tjek om listing allerede eksisterer ──
        cursor.execute(
            "SELECT id, rent_monthly, is_active FROM rental_listings WHERE source=? AND listing_id=?",
            (source, listing_id)
        )
        existing = cursor.fetchone()

        if existing is None:
            # ── Ny listing ──
            cursor.execute("""
                INSERT INTO rental_listings
                    (source, listing_id, address, zip_code, city,
                     rent_monthly, size_sqm, rooms, property_type,
                     deposit, available_from, listing_url, email_received_at,
                     first_seen, last_seen, relist_count, price_change_count)
                VALUES
                    (:source, :listing_id, :address, :zip_code, :city,
                     :rent_monthly, :size_sqm, :rooms, :property_type,
                     :deposit, :available_from, :listing_url, :email_received_at,
                     :now, :now, 0, 0)
            """, {**listing, 'now': now})
            conn.commit()
            return True

        else:
            # ── Eksisterende listing – opdatér og registrér ændringer ──
            old_rent    = existing['rent_monthly']
            was_inactive = existing['is_active'] == 0

            price_changed = (
                new_rent is not None and
                old_rent is not None and
                int(new_rent) != int(old_rent)
            )

            cursor.execute("""
                UPDATE rental_listings SET
                    last_seen           = ?,
                    is_active           = 1,
                    rent_monthly        = CASE WHEN ? THEN ? ELSE rent_monthly END,
                    price_change_count  = price_change_count + ?,
                    relist_count        = relist_count + ?
                WHERE source = ? AND listing_id = ?
            """, (
                now,
                price_changed, new_rent,
                1 if price_changed else 0,
                1 if was_inactive else 0,
                source, listing_id,
            ))
            conn.commit()
            return False

    except Exception as e:
        logger.error(f"Fejl ved indsættelse af lejebolig: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def insert_property_for_sale(db_path: str, prop: dict) -> bool:
    """
    Upsert en bolig til salg.
    Ny bolig → INSERT. Gensyn → opdatér pris, liggetid og prisændringer.
    commute_minutes og koordinater bevares ved upsert (sættes af enricher).
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO properties_for_sale
                (source, listing_id, address, zip_code, city,
                 price, size_sqm, rooms, property_type,
                 owner_costs_monthly, energy_label, listing_url,
                 days_on_market, price_change_count, price_change_amount,
                 latitude, longitude)
            VALUES
                (:source, :listing_id, :address, :zip_code, :city,
                 :price, :size_sqm, :rooms, :property_type,
                 :owner_costs_monthly, :energy_label, :listing_url,
                 :days_on_market, :price_change_count, :price_change_amount,
                 :latitude, :longitude)
            ON CONFLICT(source, listing_id) DO UPDATE SET
                price               = excluded.price,
                size_sqm            = excluded.size_sqm,
                rooms               = excluded.rooms,
                owner_costs_monthly = excluded.owner_costs_monthly,
                energy_label        = excluded.energy_label,
                listing_url         = excluded.listing_url,
                days_on_market      = excluded.days_on_market,
                price_change_count  = excluded.price_change_count,
                price_change_amount = excluded.price_change_amount,
                latitude            = COALESCE(excluded.latitude,  properties_for_sale.latitude),
                longitude           = COALESCE(excluded.longitude, properties_for_sale.longitude),
                scraped_at          = datetime('now')
                -- commute_minutes bevares (sættes kun af --enrich-commute)
        """, {
            'days_on_market':      prop.get('days_on_market'),
            'price_change_count':  prop.get('price_change_count'),
            'price_change_amount': prop.get('price_change_amount'),
            'latitude':            prop.get('latitude'),
            'longitude':           prop.get('longitude'),
            **prop,
        })

        is_new = cursor.lastrowid and cursor.rowcount > 0
        conn.commit()
        return bool(is_new)
    except Exception as e:
        logger.error(f"Fejl ved indsættelse af salgsbolig: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def mark_stale_listings_inactive(db_path: str, days_threshold: int = 30) -> int:
    """
    Markér listings som inaktive (is_active=0) hvis de ikke er set i email
    de seneste days_threshold dage. Returnér antal markerede listings.

    Kald dette som afslutning på pipeline-kørslen så DB altid afspejler
    det aktuelle marked – listings der forsvinder fra søgeagenten er
    sandsynligvis udlejet.
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE rental_listings
        SET is_active = 0
        WHERE is_active = 1
          AND last_seen < datetime('now', '-' || ? || ' days')
    """, (days_threshold,))

    count = cursor.rowcount
    conn.commit()
    conn.close()

    if count:
        logger.info(f"Markeret {count} listings inaktive (ikke set i >{days_threshold} dage)")
    return count


def calculate_and_save_aggregates(db_path: str) -> int:
    """
    Beregn aggregerede huslejepriser pr. postnummer og antal rum.
    Returnér antal postnummer/rum-kombinationer opdateret.
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    # Hent alle aktive listings med pris og størrelse
    cursor.execute("""
        SELECT zip_code, rooms, property_type,
               CAST(rent_monthly AS REAL) / size_sqm AS price_per_sqm,
               rent_monthly
        FROM rental_listings
        WHERE is_active = 1
          AND rent_monthly IS NOT NULL
          AND size_sqm IS NOT NULL
          AND rent_monthly > 1000    -- filtrer åbenlyst forkerte lejeværdier
          AND rent_monthly < 50000
          AND size_sqm >= 10         -- filtrer fejlparsede størrelser
          AND size_sqm <= 300
    """)

    rows = cursor.fetchall()
    conn.close()

    if not rows:
        logger.warning("Ingen listings fundet til aggregering")
        return 0

    # Grupper data
    from collections import defaultdict
    groups = defaultdict(list)

    for row in rows:
        zip_code = row["zip_code"]
        rooms = row["rooms"]
        ptype = row["property_type"]
        ppsqm = row["price_per_sqm"]
        rent = row["rent_monthly"]

        # Gem i to grupper: specifik (zip+rum+type) og generel (zip+rum)
        if ppsqm and ppsqm > 0:
            groups[(zip_code, rooms, ptype)].append((ppsqm, rent))
            groups[(zip_code, rooms, None)].append((ppsqm, rent))
            groups[(zip_code, None, None)].append((ppsqm, rent))

    def percentile(values, p):
        """
        Percentilberegning med lineær interpolation (samme metode som Excel PERCENTILE.INC).
        For median (p=50) med 4 værdier [64.3, 99.6, 129.5, 133.8] giver dette
        (99.6 + 129.5) / 2 = 114.55, frem for den tidligere floor-metode der gav 129.5.
        """
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        if n == 0:
            return None
        if n == 1:
            return sorted_vals[0]
        # Lineær interpolation: rank i [0, n-1]
        rank = (p / 100) * (n - 1)
        lower = int(rank)
        upper = lower + 1
        if upper >= n:
            return sorted_vals[n - 1]
        frac = rank - lower
        return sorted_vals[lower] + frac * (sorted_vals[upper] - sorted_vals[lower])

    conn = get_connection(db_path)
    cursor = conn.cursor()

    # Ryd eksisterende aggregater
    cursor.execute("DELETE FROM rental_aggregates")

    count = 0
    for (zip_code, rooms, ptype), values in groups.items():
        if len(values) < 2:  # spring over hvis for få datapunkter
            continue

        sqm_prices = [v[0] for v in values]
        rents = [v[1] for v in values]

        cursor.execute("""
            INSERT INTO rental_aggregates
                (zip_code, rooms, property_type,
                 price_per_sqm_low, price_per_sqm_median, price_per_sqm_high,
                 rent_total_low, rent_total_median, rent_total_high,
                 sample_size)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            zip_code, rooms, ptype,
            round(percentile(sqm_prices, 25), 1),
            round(percentile(sqm_prices, 50), 1),
            round(percentile(sqm_prices, 75), 1),
            percentile(rents, 25),
            percentile(rents, 50),
            percentile(rents, 75),
            len(values)
        ))
        count += 1

    conn.commit()
    conn.close()

    logger.info(f"Aggregater opdateret: {count} kombinationer")
    return count


def get_rental_summary(db_path: str) -> dict:
    """Hent en opsummering af databasens indhold."""
    conn = get_connection(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) as total, COUNT(DISTINCT zip_code) as zip_codes FROM rental_listings WHERE is_active=1")
    rental_stats = dict(cursor.fetchone())

    cursor.execute("SELECT COUNT(*) as total FROM rental_aggregates")
    agg_stats = dict(cursor.fetchone())

    cursor.execute("SELECT COUNT(*) as total FROM properties_for_sale WHERE is_active=1")
    sale_stats = dict(cursor.fetchone())

    cursor.execute("SELECT source, COUNT(*) as count FROM rental_listings GROUP BY source")
    by_source = {row["source"]: row["count"] for row in cursor.fetchall()}

    conn.close()

    return {
        "rental_listings": rental_stats,
        "rental_aggregates": agg_stats,
        "properties_for_sale": sale_stats,
        "by_source": by_source
    }


def get_data_quality(db_path: str) -> dict:
    """
    Datakvalitetsrapport: duplikater, NULL-felter, kildedækning.
    Bruges af --status til at vise datakvalitetssektionen.
    """
    conn = get_connection(db_path)
    cursor = conn.cursor()

    # Potentielle duplikater: samme adresse+by+leje+størrelse+kilde
    cursor.execute("""
        SELECT COUNT(*) as extra FROM (
            SELECT source, address, city, rent_monthly, size_sqm,
                   COUNT(*) as n
            FROM rental_listings
            WHERE is_active=1
            GROUP BY source, address, city, rent_monthly, size_sqm
            HAVING n > 1
        )
    """)
    dup_groups = cursor.fetchone()["extra"]

    cursor.execute("""
        SELECT COALESCE(SUM(n - 1), 0) as total FROM (
            SELECT COUNT(*) as n
            FROM rental_listings
            WHERE is_active=1
            GROUP BY source, address, city, rent_monthly, size_sqm
            HAVING n > 1
        )
    """)
    dup_rows = cursor.fetchone()["total"]

    # NULL-felter (alle aktive listings)
    cursor.execute("""
        SELECT
            COUNT(*) FILTER (WHERE zip_code IS NULL OR zip_code = '') as missing_zip,
            COUNT(*) FILTER (WHERE rent_monthly IS NULL)              as missing_rent,
            COUNT(*) FILTER (WHERE size_sqm IS NULL)                  as missing_size,
            COUNT(*) FILTER (WHERE rooms IS NULL)                     as missing_rooms,
            COUNT(*) FILTER (WHERE city IS NULL)                      as missing_city
        FROM rental_listings WHERE is_active=1
    """)
    nulls = dict(cursor.fetchone())

    # Listings pr. postnummer – top 10 og bundliste
    cursor.execute("""
        SELECT zip_code, COUNT(*) as n
        FROM rental_listings WHERE is_active=1
        GROUP BY zip_code ORDER BY n DESC LIMIT 10
    """)
    top_zips = [(r["zip_code"], r["n"]) for r in cursor.fetchall()]

    # Seneste indsættelse
    cursor.execute("""
        SELECT scraped_at FROM rental_listings
        WHERE is_active=1 ORDER BY scraped_at DESC LIMIT 1
    """)
    row = cursor.fetchone()
    latest_scrape = row["scraped_at"][:16] if row else "–"

    # Verificeringsdækning: hvor mange er live-tjekket?
    cursor.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(last_checked) as verified,
            COUNT(CASE WHEN last_checked >= datetime('now', '-7 days') THEN 1 END) as checked_7d,
            MAX(last_checked) as latest_check
        FROM rental_listings WHERE is_active=1
    """)
    verify = dict(cursor.fetchone())

    conn.close()

    return {
        "dup_groups": dup_groups,
        "dup_rows": dup_rows,
        "nulls": nulls,
        "top_zips": top_zips,
        "latest_scrape": latest_scrape,
        "verification": verify,
    }
