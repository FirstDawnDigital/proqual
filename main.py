"""
main.py – Ejendomsinvesteringsværktøj
Hoved-script der kører datapipelinen.

Brug:
  python main.py --run                   # Kør én fuld pipeline-kørsel
  python main.py --setup                 # Opsæt databasen og test forbindelser
  python main.py --status                # Vis databasestatistik
  python main.py --discover-senders      # Find afsenderadresser i din indbakke
  python main.py --schedule              # Kør som baggrundsjob (hvert 6. time)
  python main.py --email-only            # Kun email-parsing (ingen scraping)
  python main.py --export-sheets         # Eksporter til Google Sheets
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Tilføj projektrod til Python-sti
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

# Indlæs config
load_dotenv(Path(__file__).parent / 'config.env')

from database import (
    initialize_database,
    insert_rental_listing,
    insert_property_for_sale,
    calculate_and_save_aggregates,
    get_rental_summary,
    get_data_quality,
    mark_stale_listings_inactive,
)
from email_parser.gmail_parser import fetch_and_parse_all

console = Console()

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────

def setup_logging():
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    log_file = os.getenv('LOG_FILE', 'data/ejendom.log')
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, encoding='utf-8')
        ]
    )

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Pipeline funktioner
# ─────────────────────────────────────────────

def run_email_pipeline(db_path: str, days_back: int = 7) -> int:
    """Hent emails, parser dem og gem i databasen. Returnér antal nye listings."""
    gmail_address = os.getenv('GMAIL_ADDRESS')
    app_password = os.getenv('GMAIL_APP_PASSWORD')

    if not gmail_address or not app_password:
        console.print("[red]FEJL: GMAIL_ADDRESS og GMAIL_APP_PASSWORD mangler i config.env[/red]")
        return 0

    console.print(f"[cyan]→ Henter emails fra Gmail ({days_back} dage tilbage)...[/cyan]")

    try:
        listings = fetch_and_parse_all(gmail_address, app_password, days_back)
    except Exception as e:
        console.print(f"[red]Gmail-fejl: {e}[/red]")
        logger.error(f"Email pipeline fejlede: {e}")
        return 0

    new_count = 0
    dropped_no_zip = 0
    dropped_no_rent = 0
    from collections import Counter
    dropped_cities = Counter()

    for listing in listings:
        if not listing.get('zip_code'):
            dropped_no_zip += 1
            city = (listing.get('city') or '').strip()
            if city:
                dropped_cities[city] += 1
            else:
                dropped_cities['(ingen by)'] += 1
            continue
        if not listing.get('rent_monthly'):
            dropped_no_rent += 1
            continue
        is_new = insert_rental_listing(db_path, listing)
        if is_new:
            new_count += 1

    console.print(f"[green]✓ Email pipeline: {len(listings)} annoncer fundet, {new_count} nye gemt[/green]")
    if dropped_no_zip:
        console.print(f"[yellow]  ↳ {dropped_no_zip} droppet (ukendt postnummer)[/yellow]")
        top = dropped_cities.most_common(20)
        for city, n in top:
            console.print(f"[dim]     {n:4}×  {city}[/dim]")
        if len(dropped_cities) > 20:
            console.print(f"[dim]     ... og {len(dropped_cities) - 20} andre byer[/dim]")
    if dropped_no_rent:
        console.print(f"[yellow]  ↳ {dropped_no_rent} droppet (ingen leje)[/yellow]")
    return new_count


def run_salg_scraping(
    db_path: str,
    search_url: str = None,
    max_pages: int = None,
    delay_seconds: float = None,
) -> int:
    """
    Scraper Boligsiden for boliger til salg og gemmer i properties_for_sale.
    Returnér antal nye boliger gemt.

    Rækkefølge for søge-URL:
      1. search_url argument (f.eks. fra --salg-url)
      2. BOLIGSIDEN_SEARCH_URL i config.env
      3. Afbryd med besked
    """
    url = search_url or os.getenv('BOLIGSIDEN_SEARCH_URL')
    if not url:
        console.print(
            "[yellow]Springer Boligsiden-scraping over "
            "(sæt BOLIGSIDEN_SEARCH_URL i config.env eller brug --salg-url)[/yellow]"
        )
        return 0

    max_p = max_pages or int(os.getenv('BOLIGSIDEN_MAX_PAGES', '20'))
    delay = delay_seconds if delay_seconds is not None else float(
        os.getenv('BOLIGSIDEN_DELAY_SECONDS', '3.0')
    )

    try:
        from scrapers.boligsiden_scraper import scrape_listings
        console.print(f"[cyan]→ Scraper Boligsiden.dk (maks {max_p} sider)...[/cyan]")
        listings = scrape_listings(url, max_pages=max_p, delay_seconds=delay)

        new_count = 0
        for prop in listings:
            if insert_property_for_sale(db_path, prop):
                new_count += 1

        console.print(
            f"[green]✓ Boligsiden: {len(listings)} boliger fundet, "
            f"{new_count} nye gemt[/green]"
        )
        return new_count

    except Exception as e:
        console.print(f"[red]Boligsiden-scraping fejlede: {e}[/red]")
        logger.error(f"Boligsiden scraping fejlede: {e}", exc_info=True)
        return 0


def cmd_debug_boligsiden(url: str) -> None:
    """
    Hent én side fra Boligsiden og dump JSON-strukturen til terminalen.
    Bruges til at verificere at parseren finder de rigtige felter.
    """
    try:
        from scrapers.boligsiden_scraper import debug_page_structure
        console.print(f"[cyan]Henter og analyserer: {url}[/cyan]")
        debug_page_structure(url)
    except Exception as e:
        console.print(f"[red]Debug fejlede: {e}[/red]")
        logger.error(f"Debug boligsiden fejlede: {e}", exc_info=True)


def run_scraping_pipeline(db_path: str) -> int:
    """Kør web scraper (kræver ENABLE_SCRAPING=true i config)."""
    if os.getenv('ENABLE_SCRAPING', 'false').lower() != 'true':
        logger.debug("Scraping deaktiveret i config")
        return 0

    try:
        from scrapers.boligportal_scraper import scrape_listings
        delay = float(os.getenv('SCRAPING_DELAY_SECONDS', '3'))

        console.print("[cyan]→ Scraper Boligportal.dk...[/cyan]")
        listings = scrape_listings(delay_seconds=delay)

        new_count = 0
        for listing in listings:
            if listing.get('zip_code') and listing.get('rent_monthly'):
                if insert_rental_listing(db_path, listing):
                    new_count += 1

        console.print(f"[green]✓ Scraping: {len(listings)} annoncer fundet, {new_count} nye gemt[/green]")
        return new_count

    except Exception as e:
        console.print(f"[yellow]Scraping fejlede: {e}[/yellow]")
        logger.warning(f"Scraping pipeline fejlede: {e}")
        return 0


def run_listing_status_check(db_path: str, max_per_run: int = 100, delay: float = 2.0) -> dict:
    """Tjek om eksisterende listings stadig er aktive på boligportal/lejebolig."""
    try:
        from scrapers.listing_checker import run_listing_status_check as _check
        console.print(f"[cyan]→ Tjekker status på op til {max_per_run} listings...[/cyan]")
        stats = _check(db_path, max_per_run=max_per_run, delay=delay)
        if stats['checked'] > 0:
            console.print(
                f"[green]✓ Status-check: {stats['still_active']} aktive, "
                f"{stats['marked_inactive']} markeret inaktive"
                + (f", {stats['resolved_urls']} URLs resolved" if stats['resolved_urls'] else "")
                + (f", {stats['unknown']} ukendte" if stats['unknown'] else "")
                + "[/green]"
            )
        else:
            console.print("[dim]→ Ingen listings at status-tjekke i dag[/dim]")
        return stats
    except Exception as e:
        console.print(f"[yellow]Status-check sprunget over: {e}[/yellow]")
        logger.warning(f"Listing status check fejlede: {e}")
        return {}


def run_mark_inactive(db_path: str, days_threshold: int = 30) -> int:
    """Markér listings inaktive hvis de ikke er set i > days_threshold dage."""
    count = mark_stale_listings_inactive(db_path, days_threshold)
    if count:
        console.print(f"[yellow]→ {count} listings markeret inaktive (ikke set i >{days_threshold} dage)[/yellow]")
    else:
        console.print(f"[dim]→ Ingen listings markeret inaktive[/dim]")
    return count


def run_aggregation(db_path: str) -> int:
    """Beregn aggregater og gem dem."""
    console.print("[cyan]→ Beregner huslejeaggregater pr. postnummer...[/cyan]")
    count = calculate_and_save_aggregates(db_path)
    console.print(f"[green]✓ Aggregering: {count} postnummer/rum-kombinationer beregnet[/green]")
    return count


def run_sheets_export(db_path: str) -> int:
    """Eksporter data til Google Sheets."""
    sheet_id = os.getenv('SHEETS_RENTAL_AGGREGATES_ID')
    creds_file = os.getenv('GOOGLE_CREDENTIALS_FILE', 'google_credentials.json')

    if not sheet_id:
        console.print("[yellow]Springer Sheets-eksport over (SHEETS_RENTAL_AGGREGATES_ID ikke sat)[/yellow]")
        return 0

    if not Path(creds_file).exists():
        console.print(f"[yellow]Springer Sheets-eksport over ({creds_file} ikke fundet)[/yellow]")
        console.print("[dim]Kør: python exporters/sheets_exporter.py --setup for at opsætte Google adgang[/dim]")
        return 0

    try:
        from exporters.sheets_exporter import export_all_sheets
        console.print("[cyan]→ Eksporterer til Google Sheets...[/cyan]")
        results = export_all_sheets(db_path, sheet_id, creds_file)
        console.print(f"[green]✓ 'Huslejedata':    {results.get('huslejedata', 0):>4} postnumre (kr/m²)[/green]")
        console.print(f"[green]✓ 'Husleje pr. rum': {results.get('husleje_pr_rum', 0):>4} postnumre (pivot 1-5 rum)[/green]")
        console.print(f"[green]✓ 'Rådata':          {results.get('rådata', 0):>4} individuelle listings[/green]")
        return results.get('huslejedata', 0)
    except Exception as e:
        console.print(f"[red]Sheets-eksport fejlede: {e}[/red]")
        logger.error(f"Sheets eksport fejlede: {e}")
        return 0


def run_turso_sync(db_path: str):
    """Synkroniser lokal SQLite til Turso (kræver TURSO_URL + TURSO_AUTH_TOKEN i config.env)."""
    try:
        from webapp.turso_sync import sync_to_turso
        result = sync_to_turso(db_path=Path(db_path))
        if result["ok"]:
            console.print("[green]✓ Turso synkroniseret[/green]")
        else:
            console.print(f"[yellow]Turso-sync: {result['error']}[/yellow]")
    except Exception as e:
        logger.warning(f"Turso-sync fejlede: {e}")


def run_static_export(db_path: str):
    """Eksportér statisk HTML til docs/index.html (GitHub Pages)."""
    try:
        from webapp.export_static import export_static
        out = export_static(db_path=Path(db_path))
        console.print(f"[green]✓ Statisk HTML eksporteret: {out}[/green]")
    except Exception as e:
        console.print(f"[yellow]Statisk HTML-eksport fejlede: {e}[/yellow]")
        logger.warning(f"Statisk HTML-eksport fejlede: {e}")


def run_full_pipeline(db_path: str, days_back: int = 7):
    """Kør hele pipelinen: email → scraping → inaktivitet → aggregering → Sheets."""
    start = datetime.now()
    console.print(Panel(f"[bold]Ejendomspipeline startet[/bold]\n{start.strftime('%d-%m-%Y %H:%M')}", style="blue"))

    run_email_pipeline(db_path, days_back)
    run_scraping_pipeline(db_path)
    run_listing_status_check(db_path, max_per_run=100, delay=2.0)
    run_mark_inactive(db_path, days_threshold=30)
    run_aggregation(db_path)
    run_sheets_export(db_path)

    elapsed = (datetime.now() - start).total_seconds()
    console.print(f"\n[bold green]Pipeline færdig på {elapsed:.1f} sekunder[/bold green]")


# ─────────────────────────────────────────────
# Setup og status
# ─────────────────────────────────────────────

def cmd_analyze_cities(days_back: int = 30):
    """
    Kør email-parseren i tør-kørsel og vis alle byer parseren ser —
    både dem der matches til et postnummer og dem der droppes.
    Skriver IKKE til databasen.

    Bruges til at finde huller i CITY_TO_ZIP-dictet.
    """
    gmail_address = os.getenv('GMAIL_ADDRESS')
    app_password  = os.getenv('GMAIL_APP_PASSWORD')
    if not gmail_address or not app_password:
        console.print("[red]FEJL: Gmail-credentials mangler i config.env[/red]")
        return

    from collections import Counter
    from email_parser.gmail_parser import fetch_and_parse_all, city_to_zip

    console.print(f"[cyan]→ Henter emails ({days_back} dage tilbage) – ingen DB-skrivning...[/cyan]")

    try:
        listings = fetch_and_parse_all(gmail_address, app_password, days_back)
    except Exception as e:
        console.print(f"[red]Gmail-fejl: {e}[/red]")
        return

    matched   = Counter()   # by → zip (matchede)
    unmatched = Counter()   # by → antal (ikke matchede)
    no_city   = 0

    for l in listings:
        city = (l.get('city') or '').strip()
        if not city:
            no_city += 1
            continue
        if l.get('zip_code'):
            matched[f"{city} → {l['zip_code']}"] += 1
        else:
            unmatched[city] += 1

    total = len(listings)
    n_dropped = sum(unmatched.values()) + no_city

    console.print(f"\n[bold]Resultater for {days_back} dage ({total} annoncer fundet)[/bold]")
    console.print(f"  Matchede:   {total - n_dropped}  ({round((total-n_dropped)/total*100)}%)")
    console.print(f"  Droppede:   {n_dropped}  ({round(n_dropped/total*100)}%)")
    console.print(f"    - Ingen by i annonce:     {no_city}")
    console.print(f"    - By uden postnummer:     {sum(unmatched.values())}")

    if unmatched:
        console.print()
        drop_table = Table(title="Byer der droppes (mangler i CITY_TO_ZIP)", show_header=True)
        drop_table.add_column("By",     style="yellow", min_width=25)
        drop_table.add_column("Antal",  justify="right")
        drop_table.add_column("Forslag til postnummer", style="dim")

        for city_name, count in unmatched.most_common(40):
            # Forsøg at finde et postnummer via delvis match for at give hint
            hint = city_to_zip(city_name) or "?"
            drop_table.add_row(city_name, str(count), hint if hint != city_to_zip(city_name.split()[0]) else "?")

        console.print(drop_table)

    if matched:
        console.print()
        match_table = Table(title=f"Top 20 matchede byer", show_header=True)
        match_table.add_column("By → ZIP",  style="green", min_width=30)
        match_table.add_column("Antal",     justify="right")
        for entry, count in matched.most_common(20):
            match_table.add_row(entry, str(count))
        console.print(match_table)


def cmd_validate_listings(db_path: str, sample_size: int = 20, delay: float = 1.5):
    """
    Valider listing-status: sammenlign DB med live-tjek og rapportér afvigelser.

    Tager et stratificeret sample (aktive + nyligt-inaktive) og viser:
      - Side-om-side tabel: DB-status vs. live-status per listing
      - Falske positiver (DB: aktiv, Live: borte)
      - Falske negativer (DB: inaktiv, Live: stadig aktiv)
      - Samlet accuracy-procent
    """
    from scrapers.listing_checker import validate_listing_statuses, \
        OK, FALSE_POSITIVE, FALSE_NEGATIVE, UNKNOWN

    console.print()
    console.print(Panel(
        f"[bold]Listing-validering[/bold]\n"
        f"Sample: {sample_size} listings  |  Delay: {delay}s  |  ~{int(sample_size * delay)}s i alt",
        style="blue"
    ))
    console.print("[dim]Henter sample og tjekker live-status...[/dim]\n")

    try:
        report = validate_listing_statuses(db_path, sample_size=sample_size, delay=delay)
    except Exception as e:
        console.print(f"[red]Validering fejlede: {e}[/red]")
        logger.error(f"Validering fejlede: {e}", exc_info=True)
        return

    results = report['results']
    summary = report['summary']

    if not results:
        console.print(f"[yellow]{summary.get('error', 'Ingen resultater')}[/yellow]")
        return

    # ── Detaljeret tabel ──
    table = Table(
        title=f"Resultater ({summary['sampled_active']} aktive + {summary['sampled_inactive']} inaktive i sample)",
        show_header=True,
        header_style="bold",
        row_styles=["", "dim"],
    )
    table.add_column("Kilde",       width=12)
    table.add_column("Adresse / By",width=32)
    table.add_column("ZIP",  width=6, justify="right")
    table.add_column("Leje",  width=8, justify="right")
    table.add_column("DB",    width=8, justify="center")
    table.add_column("Live",  width=8, justify="center")
    table.add_column("Sidst set", width=11)
    table.add_column("Resultat",  width=18)

    outcome_style = {
        OK:             ("[green]✓ OK[/green]",             None),
        FALSE_POSITIVE: ("[red]✗ Falsk pos.[/red]",         "red"),
        FALSE_NEGATIVE: ("[yellow]⚠ Falsk neg.[/yellow]",   "yellow"),
        UNKNOWN:        ("[dim]? Ukendt[/dim]",              None),
    }

    for r in results:
        label, row_style = outcome_style.get(r['outcome'], ("?", None))

        db_str   = "[green]Aktiv[/green]"   if r['db_active']  else "[dim]Inaktiv[/dim]"
        live_val = r['live_active']
        if live_val is True:
            live_str = "[green]Aktiv[/green]"
        elif live_val is False:
            live_str = "[red]Borte[/red]"
        else:
            live_str = "[dim]Ukendt[/dim]"

        adresse = r['address'] or r['city']
        if r['city'] and r['city'] not in adresse:
            adresse = f"{adresse[:24]} {r['city']}"

        leje = f"{r['rent_monthly']:,}".replace(",", ".") if r['rent_monthly'] else "–"
        sidst = r['last_seen'] or "–"
        if r['days_since'] is not None:
            sidst += f" ({r['days_since']}d)"

        row_kwargs = {"style": row_style} if row_style else {}
        table.add_row(
            r['source'], adresse[:31], r['zip_code'],
            leje, db_str, live_str, sidst, label,
            **row_kwargs
        )

    console.print(table)

    # ── Sammenfatning ──
    console.print()
    acc = summary['accuracy_pct']
    acc_color = "green" if acc and acc >= 90 else "yellow" if acc and acc >= 75 else "red"

    summary_table = Table(title="Sammenfatning", show_header=False, box=None, padding=(0, 2))
    summary_table.add_column("Metric", style="cyan")
    summary_table.add_column("Værdi",  justify="right")

    summary_table.add_row("Tjekket i alt",     str(summary['total']))
    summary_table.add_row("✓ Korrekt (OK)",    f"[green]{summary['ok']}[/green]")
    summary_table.add_row(
        "✗ Falske positiver  (DB: aktiv, Live: borte)",
        f"[{'red' if summary['false_positives'] else 'green'}]{summary['false_positives']}[/{'red' if summary['false_positives'] else 'green'}]"
    )
    summary_table.add_row(
        "⚠ Falske negativer (DB: inaktiv, Live: aktiv)",
        f"[{'yellow' if summary['false_negatives'] else 'green'}]{summary['false_negatives']}[/{'yellow' if summary['false_negatives'] else 'green'}]"
    )
    summary_table.add_row("? Ukendt (netværksfejl)", str(summary['unknown']))

    if acc is not None:
        summary_table.add_row(
            "Accuracy (ekskl. ukendte)",
            f"[{acc_color}]{acc}%[/{acc_color}]"
        )

    console.print(summary_table)

    # ── Anbefalinger baseret på resultat ──
    console.print()
    if summary['false_positives'] > 2:
        console.print(
            f"[red]⚠  {summary['false_positives']} falske positiver – "
            f"kør '--check-listings' for at opdatere DB[/red]"
        )
    if summary['false_negatives'] > 1:
        console.print(
            f"[yellow]⚠  {summary['false_negatives']} falske negativer – "
            f"listings markeret inaktive for hurtigt. Overvej '--inactive-days 45'[/yellow]"
        )
    if summary['unknown'] > sample_size // 3:
        console.print(
            f"[dim]ℹ  Mange ukendte ({summary['unknown']}) – mulig rate-limiting. "
            f"Prøv '--validate-delay 3'[/dim]"
        )
    if acc and acc >= 90 and summary['false_positives'] == 0:
        console.print("[green]✓ Status-data ser pålidelig ud[/green]")


def cmd_setup(db_path: str):
    """Initialisér databasen og test Gmail-forbindelsen."""
    console.print("[bold]Opsætning af Ejendomssystem[/bold]")
    console.print()

    # Database
    console.print("[cyan]→ Initialiserer database...[/cyan]")
    initialize_database(db_path)
    console.print(f"[green]✓ Database klar: {db_path}[/green]")

    # Test Gmail
    gmail_address = os.getenv('GMAIL_ADDRESS')
    app_password = os.getenv('GMAIL_APP_PASSWORD')

    if gmail_address and app_password:
        console.print(f"[cyan]→ Tester Gmail-forbindelse ({gmail_address})...[/cyan]")
        try:
            from email_parser.gmail_parser import GmailReader
            reader = GmailReader(gmail_address, app_password)
            reader.connect()
            reader.disconnect()
            console.print("[green]✓ Gmail-forbindelse OK[/green]")
        except Exception as e:
            console.print(f"[red]Gmail-fejl: {e}[/red]")
            console.print()
            console.print("[yellow]Husk at:[/yellow]")
            console.print("  1. Aktivere 2-faktor-godkendelse på din Google-konto")
            console.print("  2. Oprette et App Password: Google-konto → Sikkerhed → App-adgangskoder")
            console.print("  3. Indsætte App Password i config.env under GMAIL_APP_PASSWORD")
    else:
        console.print("[yellow]Gmail ikke konfigureret – udfyld GMAIL_ADDRESS og GMAIL_APP_PASSWORD i config.env[/yellow]")

    console.print()
    console.print("[bold]Næste skridt:[/bold]")
    console.print("  1. Kopier config.example.env til config.env og udfyld dine oplysninger")
    console.print("  2. Opret annonce-agenter på Boligportal.dk og Lejebolig.dk")
    console.print("  3. Kør: python main.py --run")


def cmd_status(db_path: str):
    """Vis databasestatistik og datakvalitetsrapport."""
    if not Path(db_path).exists():
        console.print(f"[yellow]Database ikke fundet: {db_path}[/yellow]")
        console.print("Kør: python main.py --setup")
        return

    summary = get_rental_summary(db_path)
    quality = get_data_quality(db_path)

    # ── Hoved-tabel ──
    table = Table(title="Ejendomsdatabase – Status", show_header=True)
    table.add_column("Kategori", style="cyan")
    table.add_column("Antal", justify="right", style="green")

    rental = summary['rental_listings']
    table.add_row("Lejeboliger (aktive)", str(rental.get('total', 0)))
    table.add_row("Postnumre med data", str(rental.get('zip_codes', 0)))
    table.add_row("Aggregerede huslejer", str(summary['rental_aggregates'].get('total', 0)))
    table.add_row("Boliger til salg", str(summary['properties_for_sale'].get('total', 0)))
    table.add_row("Seneste opdatering", quality['latest_scrape'])

    console.print(table)

    # ── Kildeopdeling ──
    if summary.get('by_source'):
        console.print()
        source_table = Table(title="Datakilde fordeling")
        source_table.add_column("Kilde", style="cyan")
        source_table.add_column("Antal listings", justify="right")
        for source, count in summary['by_source'].items():
            source_table.add_row(source, str(count))
        console.print(source_table)

    # ── Datakvalitet ──
    console.print()
    q = quality
    nulls = q['nulls']

    quality_table = Table(title="Datakvalitet", show_header=True)
    quality_table.add_column("Kontrol", style="cyan")
    quality_table.add_column("Antal", justify="right")
    quality_table.add_column("Status", justify="center")

    def _status(n, warn=1, bad=10):
        if n == 0:
            return "[green]✓[/green]"
        elif n < bad:
            return "[yellow]![/yellow]"
        return "[red]✗[/red]"

    dup_rows = q['dup_rows']
    quality_table.add_row("Duplikater (overflødige rækker)", str(dup_rows), _status(dup_rows, 1, 20))
    quality_table.add_row("Mangler postnummer", str(nulls['missing_zip']),  _status(nulls['missing_zip']))
    quality_table.add_row("Mangler leje",        str(nulls['missing_rent']), _status(nulls['missing_rent']))
    quality_table.add_row("Mangler størrelse",   str(nulls['missing_size']), _status(nulls['missing_size'], 10, 100))
    quality_table.add_row("Mangler rum",         str(nulls['missing_rooms']),_status(nulls['missing_rooms'], 10, 100))
    quality_table.add_row("Mangler by",          str(nulls['missing_city']), _status(nulls['missing_city']))

    console.print(quality_table)

    # ── Verificeringsdækning ──
    v = q.get('verification', {})
    if v.get('total', 0) > 0:
        console.print()
        total_v    = v['total']
        verified   = v.get('verified', 0)
        checked_7d = v.get('checked_7d', 0)
        pct        = round(verified / total_v * 100) if total_v else 0
        latest     = (v.get('latest_check') or '')[:16] or '–'
        pct_color  = "green" if pct >= 80 else "yellow" if pct >= 30 else "red"

        verify_table = Table(title="Live-verificering", show_header=False,
                             box=None, padding=(0, 2))
        verify_table.add_column("Metric", style="cyan")
        verify_table.add_column("Værdi",  justify="right")
        verify_table.add_row("Live-tjekket (total)",
                             f"[{pct_color}]{verified}/{total_v} ({pct}%)[/{pct_color}]")
        verify_table.add_row("Tjekket seneste 7 dage", str(checked_7d))
        verify_table.add_row("Seneste live-tjek",      latest)
        if pct < 100:
            days_to_full = max(0, round((total_v - verified) / 100))
            verify_table.add_row(
                "Fuld dækning om ca.",
                f"{days_to_full} dage (ved 100/dag)" if days_to_full > 0 else "< 1 dag"
            )
        console.print(verify_table)

        if pct < 30:
            console.print(
                "[yellow]ℹ  Demand signal i Sheets vises som '⬜ Ikke tjekket' "
                "indtil live-verificering er gennemført[/yellow]"
            )

    # ── Top-postnumre ──
    if q['top_zips']:
        console.print()
        zip_table = Table(title="Top 10 postnumre (listings)")
        zip_table.add_column("Postnummer", style="cyan")
        zip_table.add_column("Listings", justify="right")
        for zip_code, n in q['top_zips']:
            zip_table.add_row(zip_code, str(n))
        console.print(zip_table)


# ─────────────────────────────────────────────
# Debug email – dump rå HTML fra indbakken
# ─────────────────────────────────────────────

def cmd_debug_email(sender: str, days_back: int = 14, output_file: str = "data/debug_email.html"):
    """
    Hent den nyeste email fra 'sender' og gem HTML-body til en fil.
    Bruges til at forstå Lejebolig/Boligportal's faktiske email-format.
    """
    gmail_address = os.getenv('GMAIL_ADDRESS')
    app_password = os.getenv('GMAIL_APP_PASSWORD')

    if not gmail_address or not app_password:
        console.print("[red]FEJL: Gmail-credentials mangler i config.env[/red]")
        return

    # Strip eventuelle < > fra adressen
    sender = sender.strip().strip('<>').strip()

    console.print(f"[cyan]Henter nyeste email fra: {sender}[/cyan]")

    from email_parser.gmail_parser import GmailReader
    reader = GmailReader(gmail_address, app_password)
    try:
        reader.connect()
        emails = reader.get_emails_from_sender(sender, days_back)
    finally:
        reader.disconnect()

    if not emails:
        console.print(f"[yellow]Ingen emails fundet fra {sender} de seneste {days_back} dage[/yellow]")
        return

    # Tag den nyeste
    latest = emails[-1]
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    if latest.get('body_html'):
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(latest['body_html'])
        console.print(f"[green]✓ HTML-body gemt: {output_file}[/green]")
        console.print(f"[dim]Subject: {latest.get('subject', '–')}[/dim]")
        console.print(f"[dim]Dato: {latest.get('received_at', '–')}[/dim]")
        console.print()

        # Vis et uddrag af plain text så man kan se strukturen
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(latest['body_html'], 'lxml')
        text = soup.get_text(separator='\n', strip=True)
        lines = [l for l in text.splitlines() if l.strip()]
        console.print("[bold]Uddrag af emailens tekst (første 40 linjer):[/bold]")
        for line in lines[:40]:
            console.print(f"  {line}")
        console.print(f"\n[dim]... ({len(lines)} linjer total)[/dim]")
        console.print(f"\n[bold]Åbn filen for at se fuld HTML:[/bold] {output_file}")
    else:
        console.print("[yellow]Ingen HTML-body i emailen – prøver plain text:[/yellow]")
        console.print(latest.get('body_text', '')[:2000])


# ─────────────────────────────────────────────
# List Gmail folders/labels
# ─────────────────────────────────────────────

def cmd_list_folders():
    """Vis alle mapper og labels i Gmail-kontoen."""
    gmail_address = os.getenv('GMAIL_ADDRESS')
    app_password = os.getenv('GMAIL_APP_PASSWORD')
    if not gmail_address or not app_password:
        console.print("[red]FEJL: Gmail-credentials mangler i config.env[/red]")
        return

    from email_parser.gmail_parser import GmailReader
    reader = GmailReader(gmail_address, app_password)
    try:
        reader.connect()
        folders = reader.list_folders()
    finally:
        reader.disconnect()

    console.print(f"\n[bold]Gmail-mapper for {gmail_address}:[/bold]\n")
    for f in folders:
        console.print(f"  {f}")
    console.print()
    # Escape [ så Rich ikke fortolker dem som markup-tags
    console.print("[dim]Tip: Boligportal-emails der er filtreret til en label[/dim]")
    console.print(r"[dim]søges nu automatisk via \[Gmail]/All Mail.[/dim]")


# ─────────────────────────────────────────────
# Discover senders
# ─────────────────────────────────────────────

def cmd_discover_senders(days_back: int = 60):
    """
    Scan Gmail-indbakken og vis alle unikke afsendere de seneste N dage.
    Hjælper med at finde de rigtige afsenderadresser fra Boligportal/Lejebolig.
    """
    gmail_address = os.getenv('GMAIL_ADDRESS')
    app_password = os.getenv('GMAIL_APP_PASSWORD')

    if not gmail_address or not app_password:
        console.print("[red]FEJL: GMAIL_ADDRESS og GMAIL_APP_PASSWORD mangler i config.env[/red]")
        return

    import imaplib
    import email as email_lib
    from email.header import decode_header
    from collections import Counter
    from datetime import datetime, timedelta

    console.print(f"[cyan]Forbinder til Gmail ({gmail_address})...[/cyan]")

    try:
        mail = imaplib.IMAP4_SSL('imap.gmail.com', 993)
        mail.login(gmail_address, app_password)
        mail.select('INBOX')

        since_date = (datetime.now() - timedelta(days=days_back)).strftime("%d-%b-%Y")
        _, message_numbers = mail.search(None, f'SINCE {since_date}')

        nums = message_numbers[0].split()
        console.print(f"[cyan]Analyserer {len(nums)} emails fra de seneste {days_back} dage...[/cyan]")

        sender_counts = Counter()
        bolig_keywords = ['bolig', 'leje', 'lejemål', 'villa', 'lejlighed', 'ejendom',
                          'findbolig', 'home', 'estate', 'property']

        all_senders = Counter()

        for num in nums:
            try:
                _, msg_data = mail.fetch(num, '(BODY[HEADER.FIELDS (FROM SUBJECT)])')
                raw = msg_data[0][1]
                msg = email_lib.message_from_bytes(raw)

                from_header = msg.get('From', '')
                subject_raw = msg.get('Subject', '')

                # Dekodér subject
                try:
                    subject_parts = decode_header(subject_raw)
                    subject = ''.join(
                        p[0].decode(p[1] or 'utf-8') if isinstance(p[0], bytes) else p[0]
                        for p in subject_parts
                    )
                except Exception:
                    subject = subject_raw

                # Udtræk email-adresse fra From-feltet
                email_match = __import__('re').search(r'<([^>]+)>', from_header)
                sender_email = email_match.group(1).lower() if email_match else from_header.lower().strip()

                all_senders[sender_email] += 1

                # Flagér boligrelaterede
                combined = (subject + ' ' + from_header).lower()
                if any(kw in combined for kw in bolig_keywords):
                    sender_counts[sender_email] += 1

            except Exception:
                continue

        mail.logout()

        # Vis boligrelaterede afsendere
        if sender_counts:
            table = Table(title=f"Mulige boligrelaterede afsendere (seneste {days_back} dage)", show_header=True)
            table.add_column("Afsender", style="green")
            table.add_column("Antal emails", justify="right", style="cyan")

            for sender, count in sender_counts.most_common(20):
                table.add_row(sender, str(count))

            console.print(table)
            console.print()
            console.print("[bold]Tilføj de relevante adresser til config.env:[/bold]")
            console.print("  BOLIGPORTAL_SENDER=noreply@boligportal.dk")
            console.print("  LEJEBOLIG_SENDER=agent@lejebolig.dk")
            console.print()
            console.print("[dim]Og opdatér gmail_parser.py fetch_and_parse_all() med de korrekte adresser[/dim]")
        else:
            console.print(f"[yellow]Ingen boligrelaterede emails fundet de seneste {days_back} dage.[/yellow]")
            console.print()
            console.print("[bold]Det betyder sandsynligvis:[/bold]")
            console.print("  1. Du har ikke oprettet annonce-agenter på Boligportal/Lejebolig endnu")
            console.print("  2. Emails fra agenterne lander i spam/anden mappe")
            console.print()
            console.print("[bold]Gør dette nu:[/bold]")
            console.print("  → Gå til boligportal.dk → Log ind → Gemte søgninger → Slå email-notifikationer til")
            console.print("  → Gå til lejebolig.dk → Opret søgeagent → Vælg 'Send email ved nye annoncer'")
            console.print()

            if all_senders:
                top_table = Table(title="Top 10 afsendere i din indbakke (alle typer)", show_header=True)
                top_table.add_column("Afsender", style="dim")
                top_table.add_column("Antal", justify="right", style="dim")
                for sender, count in all_senders.most_common(10):
                    top_table.add_row(sender, str(count))
                console.print(top_table)

    except imaplib.IMAP4.error as e:
        console.print(f"[red]Gmail-fejl: {e}[/red]")


# ─────────────────────────────────────────────
# Scheduler (kør automatisk i baggrunden)
# ─────────────────────────────────────────────

def cmd_schedule(db_path: str, interval_hours: int = 6):
    """Kør pipelinen automatisk hvert N. time + daglig Boligsiden-scraping kl. 07:00."""
    import schedule
    import time

    console.print(f"[bold]Starter scheduler[/bold]")
    console.print(f"  • Email-pipeline: hvert {interval_hours}. time")
    console.print(f"  • Boligsiden-scraping + Sheets-eksport: dagligt kl. 07:00")
    console.print("Tryk Ctrl+C for at stoppe\n")

    # Kør email-pipeline straks ved opstart
    run_full_pipeline(db_path)

    # Email-pipeline: hvert N. time
    schedule.every(interval_hours).hours.do(run_full_pipeline, db_path=db_path)

    # Boligsiden-scraping + Sheets-eksport + statisk HTML: dagligt kl. 07:00
    def _daglig_salg_job():
        try:
            run_salg_scraping(db_path)
            run_sheets_export(db_path)
            run_turso_sync(db_path)
            run_static_export(db_path)
        except Exception as e:
            logger.error(f"Daglig Boligsiden-job fejlede: {e}")

    schedule.every().day.at("07:00").do(_daglig_salg_job)

    while True:
        schedule.run_pending()
        time.sleep(60)


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    setup_logging()
    db_path = os.getenv('DB_PATH', 'data/ejendom.db')

    parser = argparse.ArgumentParser(
        description='Ejendomsinvesteringsværktøj – Datapipeline'
    )
    parser.add_argument('--run', action='store_true', help='Kør én fuld pipeline-kørsel')
    parser.add_argument('--setup', action='store_true', help='Initialiser database og test forbindelser')
    parser.add_argument('--status', action='store_true', help='Vis databasestatistik')
    parser.add_argument('--list-folders', action='store_true', help='Vis alle Gmail-mapper og labels')
    parser.add_argument('--discover-senders', action='store_true', help='Find boligrelaterede afsendere i Gmail-indbakken')
    parser.add_argument('--debug-email', metavar='AFSENDER', help='Dump HTML fra nyeste email fra denne afsender (f.eks. noreply@lejebolig.dk)')
    parser.add_argument('--schedule', action='store_true', help='Kør som baggrundsjob hvert 6. time')
    parser.add_argument('--email-only', action='store_true', help='Kun email-parsing')
    parser.add_argument('--export-sheets', action='store_true', help='Eksporter til Google Sheets')
    parser.add_argument('--export-static', action='store_true', help='Eksportér statisk HTML til docs/index.html (GitHub Pages)')
    parser.add_argument('--sync-turso', action='store_true', help='Synkroniser lokal SQLite til Turso (kræver TURSO_URL i config.env)')
    parser.add_argument('--days-back', type=int, default=7, help='Antal dage bagud for email-parsing (default: 7)')
    parser.add_argument('--interval-hours', type=int, default=6, help='Timer mellem kørsler ved --schedule (default: 6)')
    parser.add_argument('--analyze-cities', action='store_true', help='Tør-kørsel: vis alle byer parseren ser og hvilke der mangler postnummer')
    parser.add_argument('--validate-listings', action='store_true', help='Valider listing-status: sammenlign DB med live-tjek og rapportér afvigelser')
    parser.add_argument('--validate-count', type=int, default=20, help='Antal listings at sample ved validering (default: 20)')
    parser.add_argument('--validate-delay', type=float, default=1.5, help='Sekunder mellem requests ved validering (default: 1.5)')
    parser.add_argument('--check-listings', action='store_true', help='Tjek om eksisterende listings stadig er aktive på websitet')
    parser.add_argument('--check-max', type=int, default=100, help='Maks listings at tjekke per kørsel (default: 100)')
    parser.add_argument('--mark-inactive', action='store_true', help='Markér listings inaktive hvis ikke set i >30 dage')
    parser.add_argument('--inactive-days', type=int, default=30, help='Dage uden aktivitet før listing markeres inaktiv (default: 30)')

    # ── Boliger til salg ──
    parser.add_argument('--scrape-salg', action='store_true',
                        help='Scraper Boligsiden for boliger til salg og gem i databasen')
    parser.add_argument('--salg-url', metavar='URL',
                        help='Søge-URL fra Boligsiden (tilsidesætter BOLIGSIDEN_SEARCH_URL i config.env)')
    parser.add_argument('--salg-max-pages', type=int, default=None,
                        help='Maks antal sider at scrape (default: 20, ~400 boliger)')
    parser.add_argument('--salg-delay', type=float, default=None,
                        help='Sekunder mellem requests til Boligsiden (default: 3.0)')
    parser.add_argument('--debug-boligsiden', metavar='URL',
                        help='Dump JSON-strukturen fra én Boligsiden-side (fejlsøgning)')

    # ── Køretidsberigelse ──
    parser.add_argument('--enrich-commute', action='store_true',
                        help='Beregn køretid fra Egevangen 19 til alle salgsboliger (Nominatim + OSRM)')
    parser.add_argument('--commute-force', action='store_true',
                        help='Genberegn køretid selv for boliger der allerede har data')

    args = parser.parse_args()

    # Sikr at database eksisterer
    initialize_database(db_path)

    if args.setup:
        cmd_setup(db_path)
    elif args.status:
        cmd_status(db_path)
    elif args.list_folders:
        cmd_list_folders()
    elif args.debug_email:
        cmd_debug_email(args.debug_email, days_back=args.days_back)
    elif getattr(args, 'discover_senders', False):
        cmd_discover_senders(args.days_back if args.days_back != 7 else 60)
    elif args.run:
        run_full_pipeline(db_path, args.days_back)
    elif args.email_only:
        run_email_pipeline(db_path, args.days_back)
        run_aggregation(db_path)
    elif args.export_sheets:
        run_sheets_export(db_path)
    elif args.export_static:
        run_static_export(db_path)
    elif args.sync_turso:
        run_turso_sync(db_path)
    elif args.analyze_cities:
        cmd_analyze_cities(days_back=args.days_back)
    elif args.validate_listings:
        cmd_validate_listings(db_path, sample_size=args.validate_count, delay=args.validate_delay)
    elif args.check_listings:
        run_listing_status_check(db_path, max_per_run=args.check_max)
    elif args.mark_inactive:
        run_mark_inactive(db_path, days_threshold=args.inactive_days)
    elif args.scrape_salg:
        run_salg_scraping(
            db_path,
            search_url=args.salg_url,
            max_pages=args.salg_max_pages,
            delay_seconds=args.salg_delay,
        )
    elif args.debug_boligsiden:
        cmd_debug_boligsiden(args.debug_boligsiden)
    elif args.enrich_commute:
        from enrichers.commute_enricher import enrich_commute
        stats = enrich_commute(db_path, force=args.commute_force)
        print(f"✓ Køretid beregnet for {stats['routed']} boliger "
              f"({stats['geocoded']} geocoded, {stats['failed']} fejlede)")
    elif args.schedule:
        cmd_schedule(db_path, args.interval_hours)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
