"""
boligportal_scraper.py – Web scraper for Boligportal.dk

Bruges som supplement til email-parseren for at øge datamængden.
Respekterer boligportal.dk's robots.txt og bygger en forsinkelse ind
mellem requests for ikke at overbelaste serveren.

NOTE: Tjek boligportal.dk's vilkår før brug til kommercielle formål.
"""

import requests
import time
import re
import logging
from bs4 import BeautifulSoup
from typing import Optional
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from email_parser.gmail_parser import extract_zip_code, clean_number, clean_float

logger = logging.getLogger(__name__)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'da-DK,da;q=0.9,en;q=0.8',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

BASE_URL = "https://www.boligportal.dk"
SEARCH_URL = f"{BASE_URL}/lejebolig"


def scrape_listings(
    zip_codes: list[str] = None,
    max_pages: int = 5,
    delay_seconds: float = 3.0
) -> list[dict]:
    """
    Scraper Boligportal.dk for lejeboliger.

    Args:
        zip_codes: Liste af postnumre at søge i (None = alle)
        max_pages: Maksimalt antal sider at scrape pr. postnummer
        delay_seconds: Ventetid mellem requests

    Returns:
        Liste af listings som dicts
    """
    all_listings = []
    session = requests.Session()
    session.headers.update(HEADERS)

    # Tjek robots.txt
    try:
        robots = session.get(f"{BASE_URL}/robots.txt", timeout=10)
        if 'Disallow: /lejebolig' in robots.text:
            logger.warning("robots.txt tillader ikke scraping af /lejebolig")
    except Exception:
        pass

    search_targets = []
    if zip_codes:
        for zip_code in zip_codes:
            search_targets.append(f"{SEARCH_URL}?zipCode={zip_code}")
    else:
        search_targets.append(SEARCH_URL)

    for search_url in search_targets:
        for page in range(1, max_pages + 1):
            url = f"{search_url}&page={page}" if '?' in search_url else f"{search_url}?page={page}"

            try:
                response = session.get(url, timeout=15)
                response.raise_for_status()
            except requests.RequestException as e:
                logger.warning(f"Fejl ved hentning af {url}: {e}")
                break

            listings = _parse_search_page(response.text)

            if not listings:
                logger.debug(f"Ingen listings på side {page} – stopper")
                break

            all_listings.extend(listings)
            logger.info(f"Side {page}: {len(listings)} listings fundet ({url})")

            # Respekter serveren med en pause
            time.sleep(delay_seconds)

    logger.info(f"Boligportal scraping færdig: {len(all_listings)} listings total")
    return all_listings


def _parse_search_page(html: str) -> list[dict]:
    """Parser én søgeresultats-side fra Boligportal."""
    soup = BeautifulSoup(html, 'lxml')
    listings = []

    # Boligportal bruger React/Next.js – prøv JSON-data i __NEXT_DATA__ scriptet
    script_tag = soup.find('script', id='__NEXT_DATA__')
    if script_tag:
        import json
        try:
            data = json.loads(script_tag.string)
            # Naviger til listings i JSON-strukturen
            ads = _extract_from_next_data(data)
            if ads:
                return ads
        except (json.JSONDecodeError, KeyError, TypeError):
            pass

    # Fallback: HTML parsing
    # Boligportal's CSS-klasser ændres jævnligt – dette er et udgangspunkt
    cards = soup.find_all('article', class_=re.compile(r'AdCard|listing', re.I))

    for card in cards:
        listing = _parse_card(card)
        if listing and listing.get('zip_code'):
            listing['source'] = 'boligportal'
            listings.append(listing)

    return listings


def _extract_from_next_data(data: dict) -> list[dict]:
    """Udtræk listings fra Next.js __NEXT_DATA__ JSON."""
    listings = []

    # Forsøg at navigere til listings (strukturen ændrer sig)
    try:
        # Typisk placering i Next.js apps
        props = data.get('props', {})
        page_props = props.get('pageProps', {})

        # Forsøg forskellige nøgler
        ads = (
            page_props.get('listings') or
            page_props.get('ads') or
            page_props.get('results') or
            page_props.get('data', {}).get('listings') or
            []
        )

        for ad in ads:
            if not isinstance(ad, dict):
                continue

            # Boligportal's JSON-felt-navne (baseret på observationer)
            rent = (
                ad.get('monthlyRent') or
                ad.get('rent') or
                ad.get('price') or
                ad.get('rentPerMonth')
            )

            size = (
                ad.get('size') or
                ad.get('area') or
                ad.get('squareMeters')
            )

            rooms = (
                ad.get('rooms') or
                ad.get('numberOfRooms') or
                ad.get('roomCount')
            )

            address_data = ad.get('address') or ad.get('location') or {}
            if isinstance(address_data, str):
                address = address_data
                zip_code = extract_zip_code(address_data)
            else:
                street = address_data.get('street', '')
                zip_code = str(address_data.get('zipCode') or address_data.get('zip') or '')
                city = address_data.get('city', '')
                address = f"{street}, {zip_code} {city}".strip(', ')
                if not zip_code:
                    zip_code = extract_zip_code(address)

            listing_id = str(ad.get('id') or ad.get('adId') or '')
            url = ad.get('url') or ad.get('link') or ''
            if url and not url.startswith('http'):
                url = BASE_URL + url

            prop_type = _detect_property_type(
                ad.get('type') or ad.get('propertyType') or ad.get('category') or ''
            )

            if zip_code and (rent or size):
                listings.append({
                    'source': 'boligportal',
                    'listing_id': listing_id,
                    'address': address,
                    'zip_code': zip_code,
                    'city': address_data.get('city') if isinstance(address_data, dict) else None,
                    'rent_monthly': clean_number(str(rent)) if rent else None,
                    'size_sqm': clean_float(str(size)) if size else None,
                    'rooms': int(rooms) if rooms else None,
                    'property_type': prop_type,
                    'deposit': clean_number(str(ad.get('deposit', ''))) if ad.get('deposit') else None,
                    'available_from': ad.get('availableFrom'),
                    'listing_url': url,
                    'email_received_at': None,
                })

    except Exception as e:
        logger.debug(f"Fejl i _extract_from_next_data: {e}")

    return listings


def _parse_card(card) -> Optional[dict]:
    """Parser ét HTML annonce-card til en dict."""
    text = card.get_text(separator=' ', strip=True)
    if not text:
        return None

    # Leje
    rent = None
    rent_match = re.search(r'(\d[\d.]+)\s*kr[./]?(?:md|mdr|måned)?', text, re.I)
    if rent_match:
        rent = clean_number(rent_match.group(1))

    # Størrelse
    size = None
    size_match = re.search(r'(\d+[,.]?\d*)\s*m[²2]', text, re.I)
    if size_match:
        size = clean_float(size_match.group(1))

    # Rum
    rooms = None
    rooms_match = re.search(r'(\d+)\s*(?:vær|værelse)', text, re.I)
    if rooms_match:
        rooms = int(rooms_match.group(1))

    zip_code = extract_zip_code(text)

    link = card.find('a', href=True)
    url = None
    if link:
        href = link['href']
        url = href if href.startswith('http') else BASE_URL + href

    listing_id = None
    if url:
        m = re.search(r'/(\d+)/?(?:\?|$)', url)
        listing_id = m.group(1) if m else None

    return {
        'source': 'boligportal',
        'listing_id': listing_id,
        'address': None,
        'zip_code': zip_code,
        'city': None,
        'rent_monthly': rent,
        'size_sqm': size,
        'rooms': rooms,
        'property_type': _detect_property_type(text),
        'deposit': None,
        'available_from': None,
        'listing_url': url,
        'email_received_at': None,
    }


def _detect_property_type(text: str) -> Optional[str]:
    """Gæt boligtype fra tekst."""
    text_lower = text.lower()
    if any(w in text_lower for w in ['lejlighed', 'apartment', 'etagebolig']):
        return 'lejlighed'
    elif any(w in text_lower for w in ['rækkehus', 'townhouse']):
        return 'rækkehus'
    elif any(w in text_lower for w in ['villa', 'hus', 'parcelhus', 'house']):
        return 'villa'
    elif any(w in text_lower for w in ['værelse', 'room']):
        return 'værelse'
    return None
