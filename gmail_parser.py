"""
gmail_parser.py – Læs og parser boligannoncer fra Gmail via IMAP

Understøtter:
  - Boligportal.dk annonce-agenter
  - Lejebolig.dk annonce-agenter

Kræver et Gmail App Password (ikke din rigtige adgangskode).
Opret under: Google-konto → Sikkerhed → App-adgangskoder
"""

import imaplib
import email
import re
import logging
from datetime import datetime, timedelta
from email.header import decode_header
from bs4 import BeautifulSoup
from typing import Optional

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Hjælpefunktioner
# ─────────────────────────────────────────────

def decode_str(s) -> str:
    """Dekodér email-header (håndterer UTF-8, latin-1 osv.)"""
    if isinstance(s, bytes):
        try:
            return s.decode("utf-8")
        except UnicodeDecodeError:
            return s.decode("latin-1", errors="replace")
    return s or ""


def extract_zip_code(text: str) -> Optional[str]:
    """Udtræk dansk postnummer (4 cifre) fra en tekststreng."""
    match = re.search(r'\b([1-9]\d{3})\b', text)
    return match.group(1) if match else None


def clean_number(text: str) -> Optional[int]:
    """Konvertér "5.200 kr." eller "5200" til int 5200."""
    if not text:
        return None
    cleaned = re.sub(r'[^\d]', '', str(text))
    return int(cleaned) if cleaned else None


def clean_float(text: str) -> Optional[float]:
    """Konvertér "85 m²" eller "85,5" til float. Returnér kun det første tal."""
    if not text:
        return None
    # Udtræk første tal (med evt. decimal) – ignorer enheder som m², vær. osv.
    match = re.search(r'(\d+[,.]?\d*)', str(text))
    if not match:
        return None
    cleaned = match.group(1).replace(',', '.')
    try:
        return float(cleaned)
    except ValueError:
        return None


# ─────────────────────────────────────────────
# Gmail forbindelse
# ─────────────────────────────────────────────

class GmailReader:
    def __init__(self, gmail_address: str, app_password: str):
        self.address = gmail_address
        self.password = app_password
        self.mail = None

    def connect(self):
        """Forbind til Gmail via IMAP SSL med 30 sekunders timeout."""
        try:
            self.mail = imaplib.IMAP4_SSL('imap.gmail.com', 993)
            # Sæt socket timeout så vi ikke hænger uendeligt
            self.mail.sock.settimeout(30)
            self.mail.login(self.address, self.password)
            logger.info(f"Forbundet til Gmail: {self.address}")
        except imaplib.IMAP4.error as e:
            logger.error(f"Gmail login fejlede: {e}")
            raise

    def disconnect(self):
        """Luk IMAP-forbindelsen."""
        if self.mail:
            try:
                self.mail.logout()
            except Exception:
                pass

    def list_folders(self) -> list[str]:
        """Returnér liste af alle Gmail-mapper/labels."""
        if not self.mail:
            self.connect()
        _, folders = self.mail.list()
        result = []
        for f in folders:
            if isinstance(f, bytes):
                f = f.decode('utf-8', errors='replace')
            # IMAP LIST-svar: (\Flags) "/" "Mappenavn"  eller  (\Flags) "/" Mappenavn
            # Vi vil have alt efter det sidste mellemrums-separerede token (mappenavnet)
            m = re.search(r'\) "?" "?(.+?)"?\s*$', f)
            if m:
                name = m.group(1).strip().strip('"')
                if name:
                    result.append(name)
        return sorted(result)

    def get_emails_from_sender(self, sender_email: str, days_back: int = 7) -> list[dict]:
        """
        Hent alle emails fra en bestemt afsender inden for de seneste N dage.

        Strategi (hurtig til langsom):
        1. Søg i INBOX med standard IMAP (ms-hurtig)
        2. Hvis 0 resultater: søg på tværs af alle labels med X-GM-RAW
           (Gmails eget søgeindeks – hurtig selv på store postkasser)

        Sæt GMAIL_FOLDER i config.env til et specifikt labelnavn hvis du
        altid vil søge i en bestemt mappe i stedet.
        """
        if not self.mail:
            self.connect()

        import os
        since_date = (datetime.now() - timedelta(days=days_back)).strftime("%d-%b-%Y")

        # ── Trin 1: søg i INBOX (altid hurtigt) ──
        custom_folder = os.getenv('GMAIL_FOLDER', '').strip().strip('"')
        primary_folder = custom_folder if custom_folder else 'INBOX'
        quoted = f'"{primary_folder}"' if any(c in primary_folder for c in '[] /') else primary_folder

        self.mail.select(quoted, readonly=True)
        _, message_numbers = self.mail.search(None, f'(FROM "{sender_email}" SINCE {since_date})')
        nums = message_numbers[0].split()
        logger.info(f"INBOX: {len(nums)} emails fra {sender_email}")

        # ── Trin 2: hvis ingen resultater, brug X-GM-RAW på tværs af alle labels ──
        # X-GM-RAW er Gmails native søgeindeks – hurtig selv på store postkasser.
        # Søgestrengen skal være ét quoted argument: 'X-GM-RAW "from:x after:y"'
        if not nums:
            since_gmfmt = (datetime.now() - timedelta(days=days_back)).strftime("%Y/%m/%d")
            all_mail_names = ['"[Gmail]/All Mail"', '"[Google Mail]/All Mail"']
            for all_mail in all_mail_names:
                try:
                    status, _ = self.mail.select(all_mail, readonly=True)
                    if status != 'OK':
                        continue
                    # Søgestreng skal quotes som ét samlet IMAP-literal
                    raw_query = f'from:{sender_email} after:{since_gmfmt}'
                    _, message_numbers = self.mail.search(
                        None, f'X-GM-RAW "{raw_query}"'
                    )
                    nums = message_numbers[0].split()
                    logger.info(f"All Mail X-GM-RAW: {len(nums)} emails fra {sender_email}")
                    break
                except Exception as e:
                    logger.warning(f"X-GM-RAW fejlede på {all_mail}: {e}")
                    continue

        # Hent emails i batches på 10 ad gangen (IMAP FETCH er meget hurtigere i batches)
        emails = []
        BATCH_SIZE = 10
        num_list = nums  # allerede en liste af byte-strenge

        for i in range(0, len(num_list), BATCH_SIZE):
            batch = num_list[i:i + BATCH_SIZE]
            batch_str = b','.join(batch)
            try:
                _, msg_data_list = self.mail.fetch(batch_str, '(RFC822)')
            except Exception as e:
                logger.warning(f"Batch-fetch fejlede: {e}")
                continue

            # msg_data_list er en liste af tupler (header, raw) efterfulgt af b')'
            raw_emails = [item[1] for item in msg_data_list if isinstance(item, tuple) and len(item) == 2]

            for raw_email in raw_emails:
                try:
                    msg = email.message_from_bytes(raw_email)
                    subject = decode_str(decode_header(msg['Subject'])[0][0])
                    received_at = msg.get('Date', '')

                    body_html = ""
                    body_text = ""

                    if msg.is_multipart():
                        for part in msg.walk():
                            content_type = part.get_content_type()
                            if content_type == 'text/html':
                                payload = part.get_payload(decode=True)
                                body_html = decode_str(payload)
                            elif content_type == 'text/plain' and not body_text:
                                payload = part.get_payload(decode=True)
                                body_text = decode_str(payload)
                    else:
                        payload = msg.get_payload(decode=True)
                        if msg.get_content_type() == 'text/html':
                            body_html = decode_str(payload)
                        else:
                            body_text = decode_str(payload)

                    emails.append({
                        'subject': subject,
                        'body_html': body_html,
                        'body_text': body_text,
                        'received_at': received_at,
                    })
                except Exception as e:
                    logger.warning(f"Fejl ved parsing af email: {e}")
                    continue

        logger.info(f"Fandt {len(emails)} emails fra {sender_email}")
        return emails


# ─────────────────────────────────────────────
# Boligportal parser – skræddersyet til faktisk email-format
# ─────────────────────────────────────────────

class BoligportalEmailParser:
    """
    Parser for email-notifikationer fra Boligportal.dk (verificeret format).

    Hvert annonce-card er en <div class="listing-item-section"> med
    præcis 5 tekstlinjer i fast rækkefølge:
      Linje 0: "Region, By"           – f.eks. "København, Ballerup"
      Linje 1: Titel/beskrivelse
      Linje 2: Gadenavn               – f.eks. "Telegrafvej"
      Linje 3: "36 m² • Lejlighed • 1 værelses"
      Linje 4: "5.550 kr"

    Listing-ID udtrækkes fra URL: .../id-5501399?...
    Postnummer slås op via by-navn (ingen ZIP i emailen).
    """
    SOURCE = "boligportal"

    def parse_email(self, email_data: dict) -> list[dict]:
        if email_data.get('body_html'):
            return self._parse_html(email_data['body_html'], email_data.get('received_at', ''))
        return []

    def _parse_html(self, html: str, received_at: str) -> list[dict]:
        soup = BeautifulSoup(html, 'lxml')
        listings = []

        cards = soup.find_all('div', class_='listing-item-section')
        logger.debug(f"Boligportal: fandt {len(cards)} listing-item-section cards")

        for card in cards:
            listing = self._parse_card(card, received_at)
            if listing:
                listings.append(listing)

        return listings

    def _parse_card(self, card, received_at: str) -> Optional[dict]:
        """Parser ét Boligportal annonce-card."""

        # Udtræk alle ikke-tomme tekstlinjer
        lines = [l.strip() for l in card.get_text(separator='\n').splitlines() if l.strip()]

        if len(lines) < 3:
            return None

        # ── Linje 0: "Region, By" ──
        region_city = lines[0]
        city = region_city.split(',')[-1].strip() if ',' in region_city else region_city.strip()

        # ── Linje 2 (eller sidst-2): gadenavn som adresse ──
        # Linje 1 = titel, linje 2 = gade, linje 3 = specs, linje 4 = pris
        # Men antallet kan variere – find specs-linjen via bullet-tegnet
        spec_line = None
        price_line = None
        street = None

        for i, line in enumerate(lines):
            if '•' in line and 'm²' in line:
                spec_line = line
                # Linjen før er gaden, linjen efter er prisen
                if i > 0:
                    street = lines[i - 1]
                if i + 1 < len(lines):
                    price_line = lines[i + 1]
                break

        if not spec_line:
            return None

        # ── Specs: "36 m² • Lejlighed • 1 værelses" ──
        parts = [p.strip() for p in spec_line.split('•')]
        size = None
        property_type = None
        rooms = None

        for p in parts:
            if 'm²' in p or 'm2' in p:
                size = clean_float(p)
            elif re.search(r'værelse|vær\.', p, re.I):
                m = re.search(r'(\d+)', p)
                if m:
                    rooms = int(m.group(1))
            else:
                pt = p.lower().strip()
                if 'lejlighed' in pt:
                    property_type = 'lejlighed'
                elif 'rækkehus' in pt:
                    property_type = 'rækkehus'
                elif 'villa' in pt or 'hus' in pt:
                    property_type = 'villa'
                elif 'værelse' in pt:
                    property_type = 'værelse'

        # ── Pris: "5.550 kr" ──
        rent = None
        if price_line:
            m = re.search(r'([\d.]+)\s*kr', price_line, re.I)
            if m:
                rent = clean_number(m.group(1))

        # ── Postnummer via by-mapping ──
        zip_code = extract_zip_code(city) or city_to_zip(city)

        # ── URL og listing-ID ──
        url = None
        listing_id = None
        # Boligportal-links er wrapped i tracking-URLs (awstrack.me)
        # Den indlejrede URL er URL-encodet i href
        for a in card.find_all('a', href=True):
            href = a['href']
            # Find listing-ID: id-XXXXXXX
            id_match = re.search(r'id[-_](\d+)', href, re.I)
            if id_match:
                listing_id = id_match.group(1)
                url = href
                break

        if not listing_id and url:
            listing_id = str(abs(hash(url)) % 10**9)

        # Spring over hvis ingen brugbare data
        if not rent and not size:
            return None

        return {
            'source': self.SOURCE,
            'listing_id': listing_id,
            'address': f"{street}, {city}" if street else city,
            'zip_code': zip_code,
            'city': city,
            'rent_monthly': rent,
            'size_sqm': size,
            'rooms': rooms,
            'property_type': property_type,
            'deposit': None,
            'available_from': None,
            'listing_url': url,
            'email_received_at': received_at,
        }


# ─────────────────────────────────────────────
# By → postnummer mapping (Lejebolig sender ikke postnummer i emailen)
# ─────────────────────────────────────────────

CITY_TO_ZIP = {
    # København og omegn
    "københavn k": "1000", "københavn v": "1500", "københavn n": "2200",
    "københavn nv": "2400", "københavn s": "2300", "københavn sv": "2450",
    "københavn ø": "2100", "frederiksberg": "2000", "vanløse": "2720",
    "brønshøj": "2700", "valby": "2500", "vesterbro": "1620",
    "nørrebro": "2200", "østerbro": "2100", "amager": "2300",
    "sundbyøster": "2300", "sundbyvester": "2450", "bispebjerg": "2400",
    # Storkøbenhavn nord
    "hellerup": "2900", "charlottenlund": "2920", "klampenborg": "2930",
    "skodsborg": "2942", "vedbæk": "2950", "rungsted": "2960",
    "hørsholm": "2970", "kokkedal": "2980", "nivå": "2990",
    "humlebæk": "3050", "espergærde": "3060", "helsingør": "3000",
    "fredensborg": "3480", "hillerød": "3400", "birkerød": "3460",
    "allerød": "3450", "lynge": "3540", "farum": "3520",
    "værløse": "3500", "ballerup": "2750", "måløv": "2760",
    "skovlunde": "2740", "herlev": "2730", "gladsaxe": "2860",
    "søborg": "2860", "bagsværd": "2880", "lyngby": "2800",
    "virum": "2830", "holte": "2840", "nærum": "2850",
    # Storkøbenhavn syd/vest
    "glostrup": "2600", "brøndby": "2605", "rødovre": "2610",
    "albertslund": "2620", "taastrup": "2630", "greve": "2670",
    "solrød": "2680", "ishøj": "2635", "vallensbæk": "2665",
    "høje taastrup": "2630", "hedehusene": "2640", "køge": "4600",
    "dragør": "2791", "tårnby": "2770", "kastrup": "2770",
    # Nordsjælland
    "tisvildeleje": "3220", "gilleleje": "3250", "græsted": "3230",
    "helsinge": "3200", "skævinge": "3320", "jægerspris": "3630",
    "frederikssund": "3600", "ølstykke": "3650", "stenløse": "3660",
    "måløv": "2760",
    # Sjælland
    "roskilde": "4000", "skibby": "4050", "kirke såby": "4060",
    "kirke hyllinge": "4070", "ringsted": "4100", "sorø": "4180",
    "slagelse": "4200", "korsør": "4220", "næstved": "4700",
    "haslev": "4690", "faxe": "4640", "stevns": "4660",
    "greve": "2670", "solrød strand": "2680", "lejre": "4070",
    "holbæk": "4300", "kalundborg": "4400", "nykøbing sj": "4500",
    "regstrup": "4420", "tølløse": "4340",
    # Fyn
    "odense": "5000", "odense c": "5000", "odense n": "5210",
    "odense sv": "5250", "odense s": "5260", "odense nv": "5210",
    "svendborg": "5700", "nyborg": "5800", "middelfart": "5500",
    "assens": "5610", "faaborg": "5600",
    # Jylland – Aarhus
    "aarhus": "8000", "aarhus c": "8000", "aarhus n": "8200",
    "aarhus v": "8210", "viby j": "8260", "brabrand": "8220",
    "risskov": "8240", "lystrup": "8520", "egå": "8250",
    # Jylland – øvrige
    "aalborg": "9000", "aalborg c": "9000", "aalborg sv": "9200",
    "vejle": "7100", "horsens": "8700", "silkeborg": "8600",
    "herning": "7400", "esbjerg": "6700", "kolding": "6000",
    "fredericia": "7000", "viborg": "8800", "randers": "8900",
    "skive": "7800", "holstebro": "7500", "ikast": "7430",
}

def city_to_zip(city_name: str) -> Optional[str]:
    """Slå postnummer op ud fra bynavn. Returnér None hvis ukendt."""
    if not city_name:
        return None
    key = city_name.strip().lower()
    # Direkte opslag
    if key in CITY_TO_ZIP:
        return CITY_TO_ZIP[key]
    # Delvis match – find første by der indeholder søgeteksten
    for city, zip_code in CITY_TO_ZIP.items():
        if key in city or city in key:
            return zip_code
    return None


# ─────────────────────────────────────────────
# Lejebolig parser – skræddersyet til faktisk email-format
# ─────────────────────────────────────────────

class LejeboligEmailParser:
    """
    Parser for email-notifikationer fra Lejebolig.dk.

    Email-strukturen (verificeret mod rigtig email):
    - Hvert annonce-card er en <table class="mobileleasetable">
    - Pris: <span><b>12.995,-</b></span>
    - Rum:  <span> efter img med 'resultat-stat-vaerelser-light.png'
    - m²:   <span> efter img med 'resultat-stat-areal-light.png'
    - By/type: <td>Lejlighed i Glostrup</td>
    - Titel: <td class="item-headline">Skøn 3-værelses...</td>
    - Link:  <a href="https://click.lejebolig.dk/..."> (tracking-URL)
    """
    SOURCE = "lejebolig"

    def parse_email(self, email_data: dict) -> list[dict]:
        if email_data.get('body_html'):
            return self._parse_html(email_data['body_html'], email_data.get('received_at', ''))
        return []

    def _parse_html(self, html: str, received_at: str) -> list[dict]:
        soup = BeautifulSoup(html, 'lxml')
        listings = []

        # Hvert annonce-card er en table med class="mobileleasetable"
        cards = soup.find_all('table', class_='mobileleasetable')
        logger.debug(f"Lejebolig: fandt {len(cards)} mobileleasetable-cards")

        for card in cards:
            listing = self._parse_card(card, received_at)
            if listing:
                listings.append(listing)

        return listings

    def _parse_card(self, card, received_at: str) -> Optional[dict]:
        """Udtræk data fra ét Lejebolig annonce-card."""

        # ── Titel (item-headline) ──
        headline_td = card.find('td', class_='item-headline')
        title = headline_td.get_text(strip=True) if headline_td else ''

        # ── By og boligtype – f.eks. "Lejlighed i Glostrup" ──
        city = None
        property_type = None
        # Find td der matcher mønsteret "<type> i <by>"
        for td in card.find_all('td'):
            text = td.get_text(strip=True)
            m = re.match(r'^(Lejlighed|Villa|Rækkehus|Hus|Værelse|Andel)\s+i\s+(.+)$', text, re.I)
            if m:
                raw_type = m.group(1).lower()
                city = m.group(2).strip()
                type_map = {
                    'lejlighed': 'lejlighed', 'villa': 'villa',
                    'rækkehus': 'rækkehus', 'hus': 'villa',
                    'værelse': 'værelse', 'andel': 'andel',
                }
                property_type = type_map.get(raw_type, raw_type)
                break

        # ── Postnummer – prøv titel først, derefter by-mapping ──
        zip_code = extract_zip_code(title) or extract_zip_code(city or '')
        if not zip_code and city:
            zip_code = city_to_zip(city)

        # ── Pris: <span><b>12.995,-</b></span> ──
        rent = None
        for b_tag in card.find_all('b'):
            m = re.match(r'^([\d.,]+),-$', b_tag.get_text(strip=True))
            if m:
                rent = clean_number(m.group(1))
                break

        # ── Rum og m² sidder i <span> efter specifikke ikonbilleder ──
        rooms = None
        size = None
        for img in card.find_all('img'):
            src = img.get('src', '')
            # Tag næste sibling <span> efter ikonet
            next_span = img.find_next_sibling('span')
            if not next_span:
                # Prøv parent's næste span
                parent = img.parent
                if parent:
                    next_span = parent.find('span')

            if 'vaerelser' in src or 'vaerelser' in src.lower():
                if next_span:
                    rooms_text = next_span.get_text(strip=True)
                    try:
                        rooms = int(re.sub(r'\D', '', rooms_text))
                    except (ValueError, TypeError):
                        pass

            elif 'areal' in src.lower():
                if next_span:
                    size_text = next_span.get_text(strip=True)
                    size = clean_float(size_text)

        # ── URL – tracking-link fra click.lejebolig.dk ──
        url = None
        listing_id = None
        first_link = card.find('a', href=re.compile(r'click\.lejebolig\.dk'))
        if first_link:
            url = first_link['href']
            # Forsøg at udtrække ID fra tracking-URL's upn-parameter
            id_match = re.search(r'lease/(\d+)', url)
            if id_match:
                listing_id = id_match.group(1)
            else:
                # Brug hash af URL som fallback-ID
                listing_id = str(abs(hash(url)) % 10**9)

        # Spring card over hvis ingen brugbare data
        if not rent and not size:
            return None

        return {
            'source': self.SOURCE,
            'listing_id': listing_id,
            'address': title if title else None,
            'zip_code': zip_code,
            'city': city,
            'rent_monthly': rent,
            'size_sqm': size,
            'rooms': rooms,
            'property_type': property_type,
            'deposit': None,
            'available_from': None,
            'listing_url': url,
            'email_received_at': received_at,
        }


# ─────────────────────────────────────────────
# Hoved-orkestrator
# ─────────────────────────────────────────────

def fetch_and_parse_all(
    gmail_address: str,
    app_password: str,
    days_back: int = 7
) -> list[dict]:
    """
    Hent og parser alle boligannoncer fra Gmail de seneste N dage.
    Afsenderadresser læses fra miljøvariable (config.env) – sæt f.eks.:
      BOLIGPORTAL_SENDER=info@boligportal.dk
      LEJEBOLIG_SENDER=noreply@lejebolig.dk
    Returnér liste af dicts klar til indsættelse i databasen.
    """
    import os
    all_listings = []

    # Læs afsenderadresser fra config – strip whitespace og < > der kan snige sig ind
    def clean_sender(val: str) -> str:
        return val.strip().strip('<>').strip()

    bp_sender = clean_sender(os.getenv('BOLIGPORTAL_SENDER', 'noreply@boligportal.dk'))
    lb_sender = clean_sender(os.getenv('LEJEBOLIG_SENDER',  'noreply@lejebolig.dk'))

    reader = GmailReader(gmail_address, app_password)

    try:
        reader.connect()

        # Boligportal
        logger.info(f"Søger efter emails fra: {bp_sender}")
        bp_emails = reader.get_emails_from_sender(bp_sender, days_back)
        bp_parser = BoligportalEmailParser()
        for e in bp_emails:
            all_listings.extend(bp_parser.parse_email(e))

        # Lejebolig
        logger.info(f"Søger efter emails fra: {lb_sender}")
        lb_emails = reader.get_emails_from_sender(lb_sender, days_back)
        lb_parser = LejeboligEmailParser()
        for e in lb_emails:
            all_listings.extend(lb_parser.parse_email(e))

    finally:
        reader.disconnect()

    logger.info(f"Email-parsing færdig: {len(all_listings)} annoncer fundet")
    return all_listings


if __name__ == "__main__":
    # Hurtig test
    import os
    from dotenv import load_dotenv
    load_dotenv('config.env')

    logging.basicConfig(level=logging.INFO)
    results = fetch_and_parse_all(
        os.getenv('GMAIL_ADDRESS'),
        os.getenv('GMAIL_APP_PASSWORD'),
        days_back=30
    )

    for r in results[:5]:
        print(r)
    print(f"\nTotal: {len(results)} annoncer")
