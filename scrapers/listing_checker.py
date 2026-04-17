"""
listing_checker.py – Tjek om eksisterende listings stadig er aktive på nettet

Formål: opdatere last_seen og is_active baseret på faktisk webstatus frem for
        email-frekvens. Det giver korrekte demand signal og relist_count data.

Understøtter:
  - Boligportal.dk  (direkte GET på rene listing-URLs)
  - Lejebolig.dk    (følger click.lejebolig.dk redirect → gemmer rigtig URL)

Køres som del af den daglige pipeline. Tjekker max_per_run listings per kørsel,
prioriteret efter ældste last_seen (de listings vi har mindst overblik over).

Brug:
    from scrapers.listing_checker import run_listing_status_check
    stats = run_listing_status_check('data/ejendom.db', max_per_run=100)
"""

import random
import re
import sys
import time
import logging
import sqlite3
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

sys.path.insert(0, str(Path(__file__).parent.parent))
from database import get_connection

logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/124.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'da-DK,da;q=0.9,en;q=0.8',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

# Tekst på Boligportal's "ikke tilgængelig"-side
BP_NOT_FOUND_SIGNALS = [
    # Udlejet (200 OK med denne tekst – det mest almindelige tilfælde)
    'markeret som udlejet',
    'udlejet af udlejer',
    'lejemålet er desværre',
    # Reserveret (udlejer har valgt lejer, men ikke officielt udlejet endnu)
    'markeret som reserveret',
    'lejemålet er blevet markeret som reserveret',
    # Fjernet / udløbet
    'annoncen er ikke længere tilgængelig',
    'annoncen er ikke tilgængelig',
    'denne annonce er fjernet',
    'annoncen er udløbet',
    # Generiske ikke-fundet
    'siden blev ikke fundet',
    'page not found',
]


# ─────────────────────────────────────────────
# URL-hjælpefunktioner
# ─────────────────────────────────────────────

def decode_awstrack_url(url: str) -> str:
    """Udpak den rigtige boligportal.dk-URL fra en awstrack.me tracking-URL."""
    if not url or 'awstrack' not in url:
        return url
    m = re.search(r'/L\d+/(https?[^"\s]+)', url)
    if not m:
        return url
    decoded = urllib.parse.unquote(m.group(1))
    return re.sub(r'[?&]utm_[^&]*', '', decoded).rstrip('?&')


def extract_listing_id_from_url(url: str) -> Optional[str]:
    """Udtræk Boligportal listing-ID fra URL (id-XXXXXXX)."""
    m = re.search(r'id[-_](\d+)', url, re.I)
    return m.group(1) if m else None


# ─────────────────────────────────────────────
# Boligportal status-check
# ─────────────────────────────────────────────

def check_boligportal_active(session: requests.Session, url: str) -> Optional[bool]:
    """
    Tjek om en Boligportal-listing stadig er aktiv.

    Returns:
        True  – listing er aktiv (200 OK, annonce-indhold til stede)
        False – listing er fjernet (404, eller "ikke tilgængelig"-side)
        None  – ukendt (netværksfejl, timeout, serverfejl)
    """
    clean_url = decode_awstrack_url(url)

    try:
        resp = session.get(clean_url, timeout=15, allow_redirects=True)
    except requests.RequestException as e:
        logger.debug(f"Netværksfejl ved {clean_url}: {e}")
        return None

    if resp.status_code == 404:
        logger.debug(f"404 – fjernet: {clean_url}")
        return False

    if resp.status_code >= 500:
        logger.debug(f"Serverfejl {resp.status_code}: {clean_url}")
        return None

    if resp.status_code != 200:
        logger.debug(f"Uventet status {resp.status_code}: {clean_url}")
        return None

    # Tjek at vi ikke er landet på søgeside eller forside (redirect)
    final_url = resp.url
    listing_id = extract_listing_id_from_url(clean_url)
    if listing_id and listing_id not in final_url:
        logger.debug(f"Redirectet væk fra listing {listing_id} → {final_url}")
        return False

    # Søg efter "ikke tilgængelig"-signaler i sideteksten (case-insensitiv)
    content_lower = resp.text[:5000].lower()  # kun toppen af siden
    if any(signal in content_lower for signal in BP_NOT_FOUND_SIGNALS):
        logger.debug(f"'Ikke tilgængelig'-tekst fundet: {clean_url}")
        return False

    return True


# ─────────────────────────────────────────────
# Lejebolig URL-resolver og status-check
# ─────────────────────────────────────────────

def resolve_lejebolig_url(session: requests.Session, tracking_url: str) -> Optional[str]:
    """
    Følg click.lejebolig.dk tracking-redirect og returnér den rigtige lejebolig.dk-URL.
    Returnér None hvis redirect fejler eller ikke lander på lejebolig.dk.
    """
    if not tracking_url or 'click.lejebolig.dk' not in tracking_url:
        return None
    try:
        resp = session.head(tracking_url, timeout=10, allow_redirects=True)
        final = resp.url
        if 'lejebolig.dk' in final and 'click.lejebolig.dk' not in final:
            # Fjern utm-parametre for renere URL
            clean = re.sub(r'[?&]utm_[^&]*', '', final).rstrip('?&')
            return clean
    except requests.RequestException as e:
        logger.debug(f"Redirect-fejl for Lejebolig: {e}")
    return None


def extract_lejebolig_id(url: str) -> Optional[str]:
    """Udtræk numerisk listing-ID fra en lejebolig.dk-URL: /lejebolig/{id}/..."""
    m = re.search(r'/lejebolig/(\d+)', url)
    return m.group(1) if m else None


def check_lejebolig_active(session: requests.Session, url: str) -> Optional[bool]:
    """
    Tjek om en Lejebolig-listing stadig er aktiv.
    Forsøger at følge tracking-URL hvis den ikke er løst endnu.
    """
    real_url = url
    if 'click.lejebolig.dk' in url:
        resolved = resolve_lejebolig_url(session, url)
        if not resolved:
            return None  # Kan ikke bestemme status
        real_url = resolved

    try:
        resp = session.get(real_url, timeout=15, allow_redirects=True)
    except requests.RequestException:
        return None

    if resp.status_code == 404:
        return False
    if resp.status_code != 200:
        return None

    # Signaler på Lejebolig's "ikke tilgængelig"-side (verificeret mod rigtige sider)
    content_lower = resp.text[:5000].lower()
    not_found = [
        'denne bolig er desværre udlejet',   # mest almindelige (verificeret)
        'denne bolig er desværre ikke',
        'annoncen er ikke tilgængelig',
        'annoncen er udløbet',
        'annoncen er ikke længere aktiv',
        'siden blev ikke fundet',
        '404',
    ]
    if any(s in content_lower for s in not_found):
        return False

    return True


# ─────────────────────────────────────────────
# Hoved-batch-tjekker
# ─────────────────────────────────────────────

def run_listing_status_check(
    db_path: str,
    max_per_run: int = 100,
    delay: float = 2.0,
) -> dict:
    """
    Tjek status på aktive listings i databasen.

    Prioriterer listings med ældste last_seen (mindst opdaterede).
    Springer listings over der allerede er tjekket i dag.

    Args:
        db_path:      Sti til SQLite-databasen
        max_per_run:  Maks antal listings at tjekke per kørsel
        delay:        Sekunder mellem requests (respekter serveren)

    Returns:
        Dict med statistik: checked, still_active, marked_inactive, resolved_urls, unknown
    """
    conn = get_connection(db_path)
    cur = conn.cursor()

    # Hent listings der skal tjekkes – ældste last_seen, ikke tjekket i dag
    cur.execute("""
        SELECT id, source, listing_url, listing_id
        FROM rental_listings
        WHERE is_active = 1
          AND listing_url IS NOT NULL
          AND (
              source = 'boligportal' AND listing_url LIKE '%boligportal.dk%'
              OR
              source = 'lejebolig' AND listing_url LIKE '%lejebolig.dk%'
          )
          AND DATE(last_seen) < DATE('now')
        ORDER BY last_seen ASC
        LIMIT ?
    """, (max_per_run,))

    to_check = cur.fetchall()
    conn.close()

    if not to_check:
        logger.info("Ingen listings at tjekke i dag")
        return {'checked': 0, 'still_active': 0, 'marked_inactive': 0,
                'resolved_urls': 0, 'unknown': 0}

    logger.info(f"Tjekker status på {len(to_check)} listings...")

    session = requests.Session()
    session.headers.update(HEADERS)

    stats = {
        'checked': 0,
        'still_active': 0,
        'marked_inactive': 0,
        'resolved_urls': 0,
        'unknown': 0,
    }
    now = datetime.now().isoformat(timespec='seconds')

    for row in to_check:
        db_id    = row['id']
        source   = row['source']
        url      = row['listing_url']

        # ── Tjek status ──
        if source == 'boligportal':
            is_active = check_boligportal_active(session, url)
        elif source == 'lejebolig':
            # Forsøg at resolve tracking-URL, gem rigtig URL + rigtig listing-ID
            if 'click.lejebolig.dk' in url:
                resolved = resolve_lejebolig_url(session, url)
                if resolved:
                    real_id = extract_lejebolig_id(resolved)
                    conn = get_connection(db_path)
                    try:
                        if real_id:
                            # Forsøg at opdatere både URL og listing_id.
                            # Kan fejle med IntegrityError hvis listing_id allerede
                            # eksisterer i en anden række (UNIQUE constraint).
                            try:
                                conn.execute(
                                    "UPDATE rental_listings SET listing_url=?, listing_id=? WHERE id=?",
                                    (resolved, real_id, db_id)
                                )
                            except sqlite3.IntegrityError:
                                # listing_id kolliderer – opdatér kun URL
                                logger.debug(
                                    f"listing_id {real_id} eksisterer allerede – opdaterer kun URL"
                                )
                                conn.execute(
                                    "UPDATE rental_listings SET listing_url=? WHERE id=?",
                                    (resolved, db_id)
                                )
                        else:
                            conn.execute(
                                "UPDATE rental_listings SET listing_url=? WHERE id=?",
                                (resolved, db_id)
                            )
                        conn.commit()
                        stats['resolved_urls'] += 1
                        url = resolved
                    finally:
                        conn.close()
            is_active = check_lejebolig_active(session, url)
        else:
            stats['unknown'] += 1
            continue

        # ── Opdatér database ──
        # last_checked sættes ALTID når vi har lavet et live-tjek — uanset resultat.
        # Det er dette felt der skelner "har vi faktisk tjekket" fra "vi ved det ikke".
        conn = get_connection(db_path)
        try:
            if is_active is True:
                conn.execute(
                    "UPDATE rental_listings SET last_seen=?, last_checked=? WHERE id=?",
                    (now, now, db_id)
                )
                stats['still_active'] += 1
            elif is_active is False:
                conn.execute(
                    "UPDATE rental_listings SET is_active=0, last_seen=?, last_checked=? WHERE id=?",
                    (now, now, db_id)
                )
                stats['marked_inactive'] += 1
                logger.debug(f"Markeret inaktiv: {url}")
            else:
                # Ukendt resultat (timeout/fejl)
                stats['unknown'] += 1
            conn.commit()
        finally:
            conn.close()
        stats['checked'] += 1

        time.sleep(delay)

    logger.info(
        f"Status-check færdig: {stats['still_active']} aktive, "
        f"{stats['marked_inactive']} inaktive, "
        f"{stats['resolved_urls']} URLs resolved, "
        f"{stats['unknown']} ukendte"
    )
    return stats


# ─────────────────────────────────────────────
# Validering: sammenlign DB-status med live-tjek
# ─────────────────────────────────────────────

# Resultat-typer
OK             = "ok"           # DB og live er enige
FALSE_POSITIVE = "false_pos"    # DB: aktiv, Live: fjernet  → forældet data
FALSE_NEGATIVE = "false_neg"    # DB: inaktiv, Live: aktiv  → fejlagtig inaktivering
UNKNOWN        = "unknown"      # Netværksfejl / timeout


def validate_listing_statuses(
    db_path: str,
    sample_size: int = 20,
    delay: float = 1.5,
) -> dict:
    """
    Valider at databasens listing-status stemmer med live-status på nettet.

    Tager et stratificeret sample:
      - ~75 % fra is_active=1  (finder falske positiver: vi tror de er aktive)
      - ~25 % fra is_active=0  (finder falske negativer: vi markerede dem for tidligt)

    Returnerer dict med:
      results  – liste af dicts, én per listing
      summary  – aggregerede tal og accuracy-pct
    """
    conn = get_connection(db_path)
    cur  = conn.cursor()

    n_active   = max(1, int(sample_size * 0.75))
    n_inactive = sample_size - n_active

    # ── Sample aktive listings (tilfældig, alle aldre) ──
    cur.execute("""
        SELECT id, source, address, city, zip_code, rent_monthly,
               listing_url, first_seen, last_seen, is_active
        FROM rental_listings
        WHERE is_active = 1
          AND listing_url IS NOT NULL
          AND (
              (source = 'boligportal' AND listing_url LIKE '%boligportal.dk%')
              OR source = 'lejebolig'
          )
        ORDER BY RANDOM()
        LIMIT ?
    """, (n_active,))
    active_rows = cur.fetchall()

    # ── Sample nyligt-inaktive listings (inaktiveret inden for 60 dage) ──
    cur.execute("""
        SELECT id, source, address, city, zip_code, rent_monthly,
               listing_url, first_seen, last_seen, is_active
        FROM rental_listings
        WHERE is_active = 0
          AND listing_url IS NOT NULL
          AND last_seen >= datetime('now', '-60 days')
          AND (
              (source = 'boligportal' AND listing_url LIKE '%boligportal.dk%')
              OR source = 'lejebolig'
          )
        ORDER BY RANDOM()
        LIMIT ?
    """, (n_inactive,))
    inactive_rows = cur.fetchall()

    conn.close()

    all_rows = list(active_rows) + list(inactive_rows)
    random.shuffle(all_rows)

    if not all_rows:
        return {'results': [], 'summary': {'total': 0, 'error': 'Ingen listings at validere'}}

    session = requests.Session()
    session.headers.update(HEADERS)

    results = []

    for row in all_rows:
        source    = row['source']
        url       = row['listing_url']
        db_active = row['is_active'] == 1

        # ── Live-tjek ──
        if source == 'boligportal':
            live_active = check_boligportal_active(session, url)
        elif source == 'lejebolig':
            # Forsøg URL-resolve hvis det stadig er et tracking-link
            if 'click.lejebolig.dk' in url:
                resolved = resolve_lejebolig_url(session, url)
                if resolved:
                    url = resolved
            live_active = check_lejebolig_active(session, url)
        else:
            live_active = None

        # ── Klassificér resultat ──
        if live_active is None:
            outcome = UNKNOWN
        elif db_active and live_active:
            outcome = OK             # begge siger aktiv ✓
        elif not db_active and not live_active:
            outcome = OK             # begge siger inaktiv ✓
        elif db_active and not live_active:
            outcome = FALSE_POSITIVE # vi tror aktiv, men borte
        else:
            outcome = FALSE_NEGATIVE # vi tror inaktiv, men stadig der

        # Dage siden last_seen
        days_since = None
        if row['last_seen']:
            try:
                ls = datetime.fromisoformat(row['last_seen'][:19])
                days_since = (datetime.now() - ls).days
            except Exception:
                pass

        results.append({
            'id':           row['id'],
            'source':       source,
            'address':      (row['address'] or '')[:40],
            'city':         row['city'] or '',
            'zip_code':     row['zip_code'] or '',
            'rent_monthly': row['rent_monthly'],
            'first_seen':   (row['first_seen'] or '')[:10],
            'last_seen':    (row['last_seen'] or '')[:10],
            'days_since':   days_since,
            'db_active':    db_active,
            'live_active':  live_active,
            'outcome':      outcome,
            'url':          url,
        })

        time.sleep(delay)

    # ── Sammenfatning ──
    n_ok  = sum(1 for r in results if r['outcome'] == OK)
    n_fp  = sum(1 for r in results if r['outcome'] == FALSE_POSITIVE)
    n_fn  = sum(1 for r in results if r['outcome'] == FALSE_NEGATIVE)
    n_unk = sum(1 for r in results if r['outcome'] == UNKNOWN)
    total = len(results)
    deterministic = total - n_unk
    accuracy = round(n_ok / deterministic * 100, 1) if deterministic > 0 else None

    summary = {
        'total':           total,
        'ok':              n_ok,
        'false_positives': n_fp,   # DB siger aktiv, virkelighed: borte
        'false_negatives': n_fn,   # DB siger inaktiv, virkelighed: stadig aktiv
        'unknown':         n_unk,
        'accuracy_pct':    accuracy,
        'sampled_active':  len(active_rows),
        'sampled_inactive':len(inactive_rows),
    }

    logger.info(
        f"Validering: {n_ok} OK, {n_fp} falske positive, "
        f"{n_fn} falske negative, {n_unk} ukendte — accuracy {accuracy}%"
    )
    return {'results': results, 'summary': summary}
