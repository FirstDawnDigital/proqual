"""
boligsiden_scraper.py – API-baseret scraper for Boligsiden.dk (boliger til salg)

Boligsiden eksponerer et REST API på api.boligsiden.dk/cases.
Scraperen kalder dette API direkte i stedet for at parse HTML.

Brug fra main.py:
    python main.py --scrape-salg
    python main.py --scrape-salg --salg-url "https://www.boligsiden.dk/tilsalg/..."
    python main.py --scrape-salg --salg-max-pages 10

Konfigurér i config.env:
    BOLIGSIDEN_SEARCH_URL=https://www.boligsiden.dk/tilsalg/villa,...?priceMax=2500000&polygon=...
    BOLIGSIDEN_MAX_PAGES=20
    BOLIGSIDEN_API_KEY=<nøgle fra browser DevTools>   ← påkrævet første gang

Sådan finder du API-nøglen:
    1. Åbn søgesiden i Chrome
    2. F12 → Network → Fetch/XHR
    3. Klik på et 'cases?...' request
    4. Under Request Headers: kopier værdien af 'X-Api-Key'
    5. Indsæt i config.env som BOLIGSIDEN_API_KEY=...
"""

import json
import logging
import re
import time
from typing import Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, urljoin, unquote

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL    = "https://www.boligsiden.dk"
API_BASE    = "https://api.boligsiden.dk"
CASES_URL   = f"{API_BASE}/search/list/cases"   # bekræftet via DevTools

# Mapning: URL-boligtype (dansk fra boligsiden.dk/tilsalg/...) → API-addressType (engelsk)
# Bekræftet via Chrome DevTools network request
DANISH_TO_API_TYPE = {
    'villa':            'villa',
    'ejerlejlighed':    'condo',
    'raekkehus':        'terraced house',   # mellemrum, ikke bindestreg
    'rækkehus':         'terraced house',
    'landejendom':      'farm',
    'villalejlighed':   'villa apartment',  # mellemrum
    'andelsbolig':      'cooperative',
    'sommerhus':        'holiday cottage',
    'fritidshus':       'holiday cottage',
    'dobbelthus':       'terraced house',
    'hobby':            'hobby farm',       # mellemrum
}

# Mapning: API-type → intern type
API_TO_INTERNAL_TYPE = {
    'villa':            'villa',
    'condo':            'ejerlejlighed',
    'terraced house':   'rækkehus',
    'terraced+house':   'rækkehus',
    'farm':             'landejendom',
    'villa apartment':  'villalejlighed',
    'cooperative':      'andelsbolig',
    'holiday cottage':  'sommerhus',
    'hobby farm':       'landejendom',
}

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/124.0.0.0 Safari/537.36'
    ),
    'Accept':          'application/json',
    'Accept-Language': 'da-DK,da;q=0.9',
    'Origin':          'https://www.boligsiden.dk',
    'Referer':         'https://www.boligsiden.dk/',
}


def scrape_listings(
    search_url: str,
    max_pages: int = 20,
    delay_seconds: float = 2.0,
    api_key: str = None,
) -> list[dict]:
    """
    Scraper Boligsiden via deres REST API.

    Args:
        search_url:    Søge-URL fra boligsiden.dk (konverteres til API-parametre)
        max_pages:     Maks antal sider (default 20 ≈ 400 boliger ved per_page=20)
        delay_seconds: Pause mellem requests
        api_key:       X-Api-Key (hentes fra config.env hvis None)

    Returns:
        Liste af property-dicts klar til insert_property_for_sale()
    """
    import os
    if api_key is None:
        api_key = os.getenv('BOLIGSIDEN_API_KEY', '')

    # Byg API-parametre fra søge-URL
    base_params = _search_url_to_api_params(search_url)
    base_params['per_page'] = 20

    session = requests.Session()
    session.headers.update(HEADERS)
    if api_key:
        session.headers['X-Api-Key'] = api_key

    all_listings: list[dict] = []

    for page in range(1, max_pages + 1):
        params = {**base_params, 'page': page}
        logger.info(f"Boligsiden API side {page}/{max_pages}: {CASES_URL}?{urlencode(params)[:100]}...")

        try:
            resp = session.get(CASES_URL, params=params, timeout=20)
        except requests.RequestException as e:
            logger.warning(f"Request fejlede (side {page}): {e}")
            break

        if resp.status_code == 401:
            logger.error("API-nøgle afvist (401). Opdatér BOLIGSIDEN_API_KEY i config.env.")
            break
        if resp.status_code == 403:
            logger.error("Adgang nægtet (403). API-nøgle kan være udløbet.")
            break
        if not resp.ok:
            logger.warning(f"HTTP {resp.status_code} på side {page} – stopper")
            break

        try:
            data = resp.json()
        except Exception as e:
            logger.warning(f"JSON-parsing fejlede: {e} – response: {resp.text[:200]}")
            break

        # Find listings i response
        cases = _extract_cases_from_response(data)
        if not cases:
            logger.info(f"Ingen cases på side {page} – søgning færdig")
            break

        parsed = [_parse_case(c) for c in cases]
        parsed = [p for p in parsed if p]
        all_listings.extend(parsed)
        logger.info(f"Side {page}: {len(parsed)} boliger hentet (total: {len(all_listings)})")

        # Stop hvis vi er på den sidste side
        total = _get_total_count(data)
        if total and len(all_listings) >= total:
            logger.info(f"Alle {total} boliger hentet")
            break
        if len(cases) < base_params.get('per_page', 20):
            logger.info("Færre results end per_page – sidste side nået")
            break

        if page < max_pages:
            time.sleep(delay_seconds)

    logger.info(f"Boligsiden API scraping færdig: {len(all_listings)} boliger total")
    return all_listings


# ──────────────────────────────────────────────────────────────────
# URL → API parametre
# ──────────────────────────────────────────────────────────────────

def _search_url_to_api_params(search_url: str) -> dict:
    """
    Konvertér en Boligsiden søge-URL til API-parametre.

    Bekræftede parametre fra Chrome DevTools:
      addressTypes, priceMax, polygon, sortBy, sortAscending

    Eksempel URL:
      https://www.boligsiden.dk/tilsalg/villa,ejerlejlighed,raekkehus?
        priceMax=2500000&polygon=12.68,56.04|12.43,56.16|...&sortBy=timeOnMarket
    """
    parsed = urlparse(search_url)
    qs = parse_qs(parsed.query, keep_blank_values=True)
    params = {}

    # ── Boligtyper fra URL-stien ──
    path_parts = parsed.path.strip('/').split('/')
    types_part = path_parts[-1] if path_parts else ''
    danish_types = [t.strip() for t in unquote(types_part).split(',')]
    api_types = [DANISH_TO_API_TYPE.get(t) for t in danish_types]
    api_types = [t for t in api_types if t]
    if api_types:
        params['addressTypes'] = ','.join(api_types)
    else:
        # Fallback: bekræftede typer fra DevTools
        params['addressTypes'] = 'villa,condo,terraced house,farm,villa apartment,hobby farm'

    # ── Polygon ──
    # parse_qs dekoder automatisk %7C → | og %2C → ,
    # requests' params= re-encoder dem korrekt ved afsendelse
    polygon_raw = qs.get('polygon', [None])[0]
    if polygon_raw:
        params['polygon'] = polygon_raw

    # ── Pris ──
    # Bekræftet parameternavn: priceMax (ikke maxPrice)
    if 'priceMax' in qs:
        params['priceMax'] = qs['priceMax'][0]
    if 'priceMin' in qs:
        params['priceMin'] = qs['priceMin'][0]

    # ── Sortering ──
    # Bekræftet: sortBy=timeOnMarket&sortAscending=true
    params['sortBy'] = qs.get('sortBy', ['timeOnMarket'])[0]
    params['sortAscending'] = 'true'

    # ── Øvrige filtre ──
    for key in ('roomsMin', 'roomsMax', 'sizeMin', 'sizeMax'):
        if key in qs:
            params[key] = qs[key][0]

    logger.debug(f"API-parametre: {params}")
    return params


# ──────────────────────────────────────────────────────────────────
# Response-parsing
# ──────────────────────────────────────────────────────────────────

def _extract_cases_from_response(data) -> list:
    """Find listen af cases i API-responset. Bekræftet nøgle: 'cases'"""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # Bekræftet: Boligsiden bruger 'cases' som top-level nøgle
        for key in ('cases', 'caseList', 'results', 'data', 'listings', 'items'):
            val = data.get(key)
            if isinstance(val, list):
                return val
    return []


def _get_total_count(data) -> Optional[int]:
    """Hent det totale antal resultater. Bekræftet nøgle: 'totalHits'"""
    if isinstance(data, dict):
        # Bekræftet: Boligsiden bruger 'totalHits'
        for key in ('totalHits', 'total', 'totalCount', 'count', 'totalResults'):
            val = data.get(key)
            if isinstance(val, int):
                return val
    return None


def _parse_case(c: dict) -> Optional[dict]:
    """
    Parser ét case-objekt fra Boligsiden API.

    Bekræftede feltnavne fra API (april 2026):
      caseID, priceCash, housingArea, numberOfRooms, monthlyExpense,
      energyLabel, addressType, slugAddress, address.cityName,
      address.houseNumber, address.streetName, address.zipCode
    """
    if not isinstance(c, dict):
        return None

    # ── Pris ──────────────────────────────────────────────────────
    # Bekræftet: priceCash
    price = _to_int(c.get('priceCash') or c.get('price') or c.get('cashPrice'))

    # ── Adresse ───────────────────────────────────────────────────
    addr = c.get('address') or {}
    if not isinstance(addr, dict):
        addr = {}

    def _str_or_name(val) -> str:
        """Håndtér felter der kan være enten string eller {'name': '...'}"""
        if isinstance(val, str):
            return val
        if isinstance(val, dict):
            return str(val.get('name') or val.get('value') or val.get('text') or '')
        return str(val) if val is not None else ''

    street   = _str_or_name(addr.get('streetName') or addr.get('street') or addr.get('road') or '')
    house_nr = _str_or_name(addr.get('houseNumber') or addr.get('number') or '')
    floor    = _str_or_name(addr.get('floor') or '')
    door     = _str_or_name(addr.get('door') or addr.get('side') or '')

    # Udtræk gadenavn fra slugAddress hvis det mangler i address-objektet
    # Format: "ellevej-80-3300-frederiksvaerk" → "Ellevej"
    if not street:
        slug_addr_tmp = c.get('slugAddress') or c.get('slug') or ''
        if slug_addr_tmp:
            parts = slug_addr_tmp.split('-')
            zip_idx = next((i for i, p in enumerate(parts) if re.match(r'^\d{4}$', p)), None)
            if zip_idx and zip_idx > 0:
                end = zip_idx - 1 if re.match(r'^\d+[a-z]?$', parts[zip_idx - 1]) else zip_idx
                street = ' '.join(p.capitalize() for p in parts[:end])

    # Postnummer: kan ligge direkte eller i nested city/zipCode
    zip_code = str(
        addr.get('zipCode') or addr.get('postalCode') or
        _nested(addr, 'postalCode', 'postalCode') or
        c.get('zipCode') or ''
    ).strip()

    # Forsøg at udtrække zip fra slug hvis det mangler
    # slug format: "ellevej-80-3300-frederiksvaerk-..."
    if not zip_code:
        slug = c.get('slugAddress') or c.get('slug') or ''
        zip_match = re.search(r'-(\d{4})-', str(slug))
        if zip_match:
            zip_code = zip_match.group(1)

    # By: Bekræftet: address.cityName eller address.city.name
    city = (
        addr.get('cityName') or
        _nested(addr, 'city', 'name') or
        addr.get('municipalityName') or
        _nested(addr, 'municipality', 'name') or
        c.get('city')
    )
    # Tom streng → None
    if not city:
        city = None

    # Byg adressestreng
    addr_line = street
    if house_nr: addr_line += f' {house_nr}'
    if floor:    addr_line += f' {floor}.'
    if door:     addr_line += f' {door}'
    parts = [p for p in [addr_line.strip(),
                         f'{zip_code} {city}'.strip() if (zip_code or city) else ''] if p]
    address_str = ', '.join(parts)

    # Fallback: udtræk fra slug
    if not address_str:
        slug_addr = c.get('slugAddress') or ''
        if slug_addr:
            address_str = slug_addr.replace('-', ' ').title()

    # ── Størrelse & rum ───────────────────────────────────────────
    # Bekræftet: housingArea og numberOfRooms
    size_sqm = _to_float(c.get('housingArea') or c.get('livingArea') or c.get('area'))
    rooms    = _to_int(c.get('numberOfRooms') or c.get('rooms') or c.get('roomCount'))

    # ── Boligtype ─────────────────────────────────────────────────
    # Bekræftet: addressType (f.eks. 'villa', 'condo')
    api_type = str(c.get('addressType') or c.get('propertyType') or c.get('type') or '').lower()
    property_type = API_TO_INTERNAL_TYPE.get(api_type) or _normalize_property_type(api_type)

    # ── Ejerudgifter & energimærke ────────────────────────────────
    # Bekræftet: monthlyExpense og energyLabel (lowercase!)
    owner_costs  = _to_int(c.get('monthlyExpense') or c.get('ownerExpenses') or c.get('monthlyExpenses'))
    energy_label = c.get('energyLabel') or c.get('energyRating')
    if energy_label:
        energy_label = str(energy_label).strip().upper()
        energy_label = energy_label if energy_label in ('A', 'A2010', 'A2015', 'A2020', 'B', 'C', 'D', 'E', 'F', 'G') else (
            energy_label[0] if energy_label and energy_label[0] in 'ABCDEFG' else None
        )

    # ── URL ───────────────────────────────────────────────────────
    # Format: /adresse/{slugAddress}  (UUID i stien virker ikke)
    case_id     = str(c.get('caseID') or c.get('caseId') or c.get('id') or '').strip() or None
    slug_addr   = c.get('slugAddress') or c.get('slug') or ''
    listing_url = f"{BASE_URL}/adresse/{slug_addr}" if slug_addr else None

    # ── Liggetid ──────────────────────────────────────────────────
    # Bekræftet API-struktur (april 2026):
    #   daysListed: {days: N}
    #   timeOnMarket: {current: {days: N}, total: {days: N, realtors: [...]}}
    days_on_market = None
    dl = c.get('daysListed')
    if isinstance(dl, dict):
        days_on_market = _to_int(dl.get('days'))
    if days_on_market is None:
        tom = c.get('timeOnMarket')
        if isinstance(tom, dict):
            days_on_market = (_to_int((tom.get('total') or {}).get('days'))
                              or _to_int((tom.get('current') or {}).get('days')))

    # ── Prishistorik ──────────────────────────────────────────────
    # Bekræftet API-struktur (april 2026) — stadig ukendt, logger ved første fund
    # Bekræftet API-felt (april 2026): priceChangePercentage (float, negativ = reduktion)
    # API giver ikke count eller absolut beløb — vi beregner approx fra pris × pct
    price_change_pct = c.get('priceChangePercentage')
    if price_change_pct is not None and price_change_pct != 0 and price is not None:
        price_change_count  = 1
        price_change_amount = round(price * price_change_pct / 100)
    else:
        price_change_count  = None
        price_change_amount = None

    # ── Koordinater ───────────────────────────────────────────────
    # Boligsiden API returnerer muligvis koordinater i address-objektet
    latitude  = _to_float(
        addr.get('latitude') or addr.get('lat') or
        c.get('latitude') or c.get('lat')
    )
    longitude = _to_float(
        addr.get('longitude') or addr.get('lng') or addr.get('lon') or
        c.get('longitude') or c.get('lng') or c.get('lon')
    )

    # ── Validering ────────────────────────────────────────────────
    if not (price or size_sqm) or not (zip_code or address_str):
        return None

    return {
        'source':              'boligsiden',
        'listing_id':          case_id,
        'address':             address_str or None,
        'zip_code':            zip_code or None,
        'city':                city,
        'price':               price,
        'size_sqm':            size_sqm,
        'rooms':               rooms,
        'property_type':       property_type,
        'owner_costs_monthly': owner_costs,
        'energy_label':        energy_label,
        'listing_url':         listing_url,
        'days_on_market':      days_on_market,
        'price_change_count':  price_change_count,
        'price_change_amount': price_change_amount,
        'latitude':            latitude,
        'longitude':           longitude,
    }


# ──────────────────────────────────────────────────────────────────
# Hjælpefunktioner
# ──────────────────────────────────────────────────────────────────

def _nested(obj: dict, *keys):
    cur = obj
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur

def _to_int(val) -> Optional[int]:
    if val is None: return None
    try:
        return int(float(str(val).replace('.', '').replace(',', '.').replace(' ', '')))
    except (ValueError, TypeError):
        return None

def _to_float(val) -> Optional[float]:
    if val is None: return None
    try:
        return float(str(val).replace(',', '.').replace(' ', ''))
    except (ValueError, TypeError):
        return None

def _extract_zip(text: str) -> Optional[str]:
    m = re.search(r'\b([1-9]\d{3})\b', str(text))
    return m.group(1) if m else None

PROPERTY_TYPE_MAP = {
    'villa': 'villa', 'hus': 'villa', 'parcelhus': 'villa',
    'ejerlejlighed': 'ejerlejlighed', 'lejlighed': 'ejerlejlighed',
    'rækkehus': 'rækkehus', 'raekkehus': 'rækkehus',
    'landejendom': 'landejendom', 'gård': 'landejendom',
    'villalejlighed': 'villalejlighed', 'andelsbolig': 'andelsbolig',
}

def _normalize_property_type(text: str) -> Optional[str]:
    text_lower = text.lower()
    for key, val in PROPERTY_TYPE_MAP.items():
        if key in text_lower:
            return val
    return None


# ──────────────────────────────────────────────────────────────────
# URL-paginering (bruges stadig til HTML-fallback)
# ──────────────────────────────────────────────────────────────────

def _build_page_url(base_url: str, page: int) -> str:
    parsed = urlparse(base_url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    if page == 1:
        params.pop('page', None)
    else:
        params['page'] = [str(page)]
    new_query = urlencode({k: v[0] for k, v in params.items()})
    return urlunparse(parsed._replace(query=new_query))


# ──────────────────────────────────────────────────────────────────
# Debug
# ──────────────────────────────────────────────────────────────────

def debug_page_structure(url: str) -> None:
    """
    Test API-kaldet direkte og vis JSON-strukturen.
    Forudsætter at BOLIGSIDEN_API_KEY er sat i config.env.
    """
    import os
    api_key = os.getenv('BOLIGSIDEN_API_KEY', '')

    session = requests.Session()
    session.headers.update(HEADERS)

    print(f"\n{'='*60}")
    print("TEST 1: Direkte API-kald til api.boligsiden.dk/cases")
    print('='*60)

    params = _search_url_to_api_params(url)
    params['per_page'] = 3
    params['page'] = 1

    print(f"URL: {CASES_URL}")
    print(f"Parametre: {params}")

    if api_key:
        session.headers['X-Api-Key'] = api_key
        print(f"X-Api-Key: {api_key[:6]}...{api_key[-4:]} (sat)")
    else:
        print("X-Api-Key: MANGLER (sæt BOLIGSIDEN_API_KEY i config.env)")

    try:
        resp = session.get(CASES_URL, params=params, timeout=15)
        print(f"\nHTTP status: {resp.status_code}")
        print(f"Content-type: {resp.headers.get('content-type', '?')}")

        if resp.status_code == 200:
            try:
                data = resp.json()
                print(f"\n=== JSON top-level nøgler ===")
                if isinstance(data, dict):
                    for k, v in list(data.items())[:15]:
                        if isinstance(v, list):
                            print(f"  {k}: list[{len(v)}]")
                            if v and isinstance(v[0], dict):
                                print(f"    Første items nøgler: {list(v[0].keys())[:12]}")
                        elif isinstance(v, dict):
                            print(f"  {k}: dict{{{', '.join(list(v.keys())[:6])}}}")
                        else:
                            print(f"  {k}: {type(v).__name__} = {repr(v)[:60]}")
                elif isinstance(data, list):
                    print(f"  Liste med {len(data)} elementer")
                    if data and isinstance(data[0], dict):
                        print(f"  Første elements nøgler: {list(data[0].keys())[:15]}")

                cases = _extract_cases_from_response(data)
                print(f"\n=== Fandt {len(cases)} cases ===")
                if cases:
                    print("Første case nøgler:", list(cases[0].keys())[:15])
                    print("\nFørste case rå data:")
                    for k, v in list(cases[0].items())[:20]:
                        print(f"  {k}: {repr(v)[:80]}")
                    parsed = _parse_case(cases[0])
                    if parsed:
                        print("\nFørste case parsed:")
                        for k, v in parsed.items():
                            print(f"  {k}: {repr(v)}")
            except Exception as e:
                print(f"JSON parse fejl: {e}")
                print(f"Response body: {resp.text[:500]}")
        else:
            print(f"Response: {resp.text[:300]}")

    except Exception as e:
        print(f"Request fejlede: {e}")

    # ── Test uden API-nøgle ──
    if not api_key:
        print("\n" + "="*60)
        print("TEST 2: Forsøg uden API-nøgle")
        print("="*60)
        session2 = requests.Session()
        session2.headers.update(HEADERS)
        try:
            resp2 = session2.get(CASES_URL, params=params, timeout=15)
            print(f"Status: {resp2.status_code}")
            if resp2.status_code == 200:
                print("✓ API virker uden nøgle!")
                data2 = resp2.json()
                cases2 = _extract_cases_from_response(data2)
                print(f"Cases fundet: {len(cases2)}")
            else:
                print(f"Response: {resp2.text[:200]}")
        except Exception as e:
            print(f"Fejl: {e}")

    print("\n" + "="*60)
    print("NÆSTE SKRIDT:")
    if not api_key:
        print("1. Find X-Api-Key i Chrome DevTools → Network → cases? → Request Headers")
        print("2. Tilføj til config.env: BOLIGSIDEN_API_KEY=<din-nøgle>")
        print("3. Kør igen: python main.py --debug-boligsiden '<url>'")
    else:
        print("Kør: python main.py --scrape-salg --salg-max-pages 1")
    print("="*60)
