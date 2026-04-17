"""
gmail_parser.py – Læs og parser boligannoncer fra Gmail via IMAP

Understøtter:
  - Boligportal.dk annonce-agenter
  - Lejebolig.dk annonce-agenter

Kræver et Gmail App Password (ikke din rigtige adgangskode).
Opret under: Google-konto → Sikkerhed → App-adgangskoder
"""

import hashlib
import imaplib
import email
import re
import logging
import urllib.parse
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
    """
    Udtræk dansk postnummer (4 cifre) fra en tekststreng.
    Ignorer 4-cifrede tal der er indlejret i bindestreg-numre (f.eks. listing-ID'er som '20-9735-1234').
    """
    # Kræv at postnummeret ikke er omgivet af cifre med bindestreg (f.eks. 20-9735-1234)
    match = re.search(r'(?<!\d-)([1-9]\d{3})(?!-\d)', text)
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
        """Forbind til Gmail via IMAP SSL med 120 sekunders timeout."""
        try:
            self.mail = imaplib.IMAP4_SSL('imap.gmail.com', 993)
            # 120 sek timeout – nok til store batches (30 sek var for lidt ved >1000 emails)
            self.mail.sock.settimeout(120)
            self.mail.login(self.address, self.password)
            logger.info(f"Forbundet til Gmail: {self.address}")
        except imaplib.IMAP4.error as e:
            logger.error(f"Gmail login fejlede: {e}")
            raise

    def reconnect(self, folder: str = None):
        """Genforbind til Gmail (bruges ved timeout under batch-fetch)."""
        logger.info("Genforbinder til Gmail...")
        try:
            self.disconnect()
        except Exception:
            pass
        self.mail = None
        self.connect()
        if folder:
            quoted = f'"{folder}"' if any(c in folder for c in '[] /') else folder
            self.mail.select(quoted, readonly=True)

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
        # current_folder sporer den faktiske mappe vi er i – bruges ved reconnect!
        current_folder = primary_folder  # opdateres nedenfor hvis vi skifter til All Mail
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
                    # Vigtigt: opdatér current_folder til den mappe vi rent faktisk
                    # har valgt, så reconnect() genforbinder til den rigtige mappe.
                    current_folder = all_mail.strip('"')
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

            # Forsøg batch-fetch med ét automatisk reconnect ved fejl
            msg_data_list = None
            for attempt in range(2):
                try:
                    _, msg_data_list = self.mail.fetch(batch_str, '(RFC822)')
                    break
                except Exception as e:
                    if attempt == 0:
                        logger.warning(f"Batch-fetch fejlede (forsøg {i//BATCH_SIZE+1}): {e} – genforbinder...")
                        try:
                            self.reconnect(current_folder)
                        except Exception as re_err:
                            logger.error(f"Genforbindingsfejl: {re_err}")
                            break
                    else:
                        logger.warning(f"Batch-fetch fejlede igen – springer batch over: {e}")

            if msg_data_list is None:
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

            # Log progress ved store mængder (hvert 200. email)
            fetched_so_far = min(i + BATCH_SIZE, len(num_list))
            if len(num_list) > 200 and fetched_so_far % 200 < BATCH_SIZE:
                logger.info(f"  Henter emails: {fetched_so_far}/{len(num_list)}...")

        logger.info(f"Fandt {len(emails)} emails fra {sender_email}")
        return emails


# ─────────────────────────────────────────────
# Boligportal parser – skræddersyet til faktisk email-format
# ─────────────────────────────────────────────


# Boligportal-kategorinavne der fejlagtigt parser som bynavn.
# Disse er søgeagent-kategorier, ikke rigtige byer – skip hele kortet.
_BP_NON_CITY = {
    "nyproduktion",
    "hele danmark",
    "alle regioner",
    "region nordjylland",
    "region midtjylland",
    "region syddanmark",
    "region sjælland",
    "region hovedstaden",
    "andelsbolig",
    "delelejlighed",
    "erhvervslokale",
    "erhverv",
    "sommerhus",
    "kolonihavehus",
    "husbåd",
}


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

        # Skip kategorinavne der fejlagtigt parser som bynavn
        # (fx "Nyproduktion", "Hele Danmark", regionsnavne)
        if city.lower() in _BP_NON_CITY:
            logger.debug(f"Boligportal: springer kategori-card over (by='{city}')")
            return None

        # Normér whitespace (fjern linjeskift der kan snige sig ind fra HTML)
        city = ' '.join(city.split())

        # Skip hvis city indeholder komma eller danske præpositioner der indikerer fritekst
        # (fx "ny ejendom på Amager", "Tækkerhusene, Hillerød", "skøn lejlighed ved...")
        _CITY_BAD_WORDS = {'på', 'ved', 'med', 'til', 'for', 'fra', 'og', 'af'}
        if ',' in city or any(f' {w} ' in f' {city.lower()} ' for w in _CITY_BAD_WORDS):
            logger.debug(f"Boligportal: springer fritekst-by over (by='{city}')")
            return None

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
        # Boligportal-links er wrapped i awstrack.me tracking-URLs.
        # Den rigtige boligportal.dk-URL sidder URL-encodet efter /L0/ i href.
        for a in card.find_all('a', href=True):
            href = a['href']

            # Forsøg at udpakke den indlejrede boligportal-URL fra awstrack.me
            awstrack_match = re.search(r'/L\d+/(https?[^"\s]+)', href)
            if awstrack_match:
                clean_url = urllib.parse.unquote(awstrack_match.group(1))
                # Fjern utm-parametre så URL'en er mere læsbar
                clean_url = re.sub(r'[?&]utm_[^&]*', '', clean_url).rstrip('?&')
            else:
                clean_url = href

            # Find listing-ID: id-XXXXXXX i den decodede URL
            id_match = re.search(r'id[-_](\d+)', clean_url, re.I)
            if id_match:
                listing_id = id_match.group(1)
                url = clean_url
                break

        if not listing_id and url:
            # Stabil URL-hash (hashlib, ikke hash() som er ikke-deterministisk)
            listing_id = hashlib.md5(url.encode()).hexdigest()[:12]

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
    # København – bydele og kvarterer (Boligportal bruger disse som bynavn)
    "indre by": "1000", "christianshavn": "1401",
    "vesterbro": "1620", "kgs. enghave": "1620", "vesterbro/kgs. enghave": "1620",
    "sydhavn": "2450", "islands brygge": "2300", "amager": "2300",
    "amagerbro": "2300", "sundbyøster": "2300", "sundbyvester": "2450",
    "ørestad": "2300",
    "nørrebro": "2200", "nordvest": "2400", "utterslev": "2400",
    "bispebjerg": "2400", "husum": "2700", "tingbjerg": "2700",
    "brønshøj": "2700", "bronshøj": "2700",
    "østerbro": "2100",
    # Storkøbenhavn nord (manglende)
    "dyssegård": "2870", "gentofte": "2820", "jægersborg": "2820",
    "ordrup": "2920", "vangede": "2870", "buddinge": "2860",
    "mørkhøj": "2730",
    # Storkøbenhavn syd/vest (manglende)
    "avedøre": "2650", "hvidovre": "2650", "amager fælled": "2300",
    "kongens lyngby": "2800", "frederiksberg c": "1900",
    # Sjælland (manglende)
    "viby sjælland": "4130",
    "frederiksværk": "3300", "hundested": "3390", "melby": "3370",
    "birkerød": "3460", "lillerød": "3450", "farum": "3520",
    "værløse": "3500", "smørum": "2765", "ledøje": "2765",
    "slangerup": "3550", "skibby": "4050",
    # Jylland – Aarhus-omegn (manglende)
    "højbjerg": "8270", "åbyhøj": "8230",
    "tilst": "8381", "gellerup": "8220", "hasle": "8210",
    "skejby": "8200", "lisbjerg": "8200", "stavtrup": "8260",
    "tranbjerg": "8310", "mårslet": "8320", "beder": "8330",
    "malling": "8340", "solbjerg": "8355", "sabro": "8471",
    "hinnerup": "8382", "hadsten": "8370",
    # Jylland – Aarhus
    "aarhus": "8000", "aarhus c": "8000", "aarhus n": "8200",
    "aarhus v": "8210", "viby j": "8260", "brabrand": "8220",
    "risskov": "8240", "lystrup": "8520", "egå": "8250",
    # Sjælland (manglende)
    "nykøbing f": "4800", "nykøbing falster": "4800",
    "nørre alslev": "4840", "sakskøbing": "4990", "maribo": "4930",
    "nakskov": "4900", "vordingborg": "4760", "præstø": "4720",
    "faxe ladeplads": "4654", "klippinge": "4672",
    "jyllinge": "4040", "gundsømagle": "4000",
    # Fyn (manglende)
    "kerteminde": "5300", "bogense": "5400", "otterup": "5450",
    # Jylland – Aarhus-omegn (manglende)
    "odder": "8300", "skanderborg": "8660", "hedensted": "8722",
    "skødstrup": "8541", "hjortshøj": "8530", "egå": "8250",
    "løgten": "8541", "rodskov": "8543",
    # Jylland – øvrige (manglende)
    "give": "7323", "grindsted": "7200", "tørring": "7160",
    "brædstrup": "8740", "nim": "8740",
    # Jylland – øvrige
    "aalborg": "9000", "aalborg c": "9000", "aalborg sv": "9200",
    "vejle": "7100", "horsens": "8700", "silkeborg": "8600",
    "herning": "7400", "esbjerg": "6700", "kolding": "6000",
    "fredericia": "7000", "viborg": "8800", "randers": "8900",
    "skive": "7800", "holstebro": "7500", "ikast": "7430",
    # ── Tilføjet: Nordhavn og manglende Kbh-kvarterer ──
    "nordhavn": "2100", "refshaleøen": "1432", "teglholmen": "2450",
    "valby bakke": "2500", "carlsberg": "1799",
    # ── Tilføjet: Nordsjælland ──
    "brønsholm": "2980", "kokkedal": "2980", "niverød": "2990",
    "snekkersten": "3070", "hornbæk": "3100", "ølsted": "3310",
    "esbønderup": "3230", "nødebo": "3480", "gørløse": "3230",
    "annisse": "3220", "vejby": "3210",
    # ── Tilføjet: Sjælland ──
    "himmelev": "4000", "tune": "4030", "stenlille": "4295",
    "hvalsø": "4330", "kirke eskilstrup": "4593", "høng": "4270",
    "svinninge": "4520", "hørve": "4534", "fårevejle": "4540",
    "asnæs": "4550", "odsherred": "4500", "nykøbing": "4500",
    "rørvig": "4581", "sjællands odde": "4583", "hundested": "3390",
    "karlstrup": "4622", "solrød strand": "2680",
    # ── Tilføjet: Fyn ──
    # NB: "højby" er flyttet til Sjælland (4573) – Funen-Højby er uden for søgeområdet
    "højby": "4573", "bellinge": "5250", "tarup": "5210",
    "hjallese": "5260", "beder": "8330", "harlev": "8462",
    # ── Tilføjet: Jylland, Aarhus-omegn ──
    "skødstrup": "8541", "hjortshøj": "8530", "hårup": "8370",
    "vivild": "8961", "pindstrup": "8550",
    # ── Tilføjet: Jylland, øvrige ──
    "lemvig": "7620", "struer": "7600", "thyborøn": "7680",
    "thisted": "7700", "nykøbing mors": "7900", "skive": "7800",
    "bjerringbro": "8850", "farsø": "9640", "løgstør": "9670",
    "aars": "9600", "hobro": "9500", "mariager": "9550",
    "grenaa": "8500", "ebeltoft": "8400", "syddjurs": "8400",
    "djursland": "8500", "hadsten": "8370", "hammel": "8450",
    "hinnerup": "8382", "søften": "8382", "lystrup": "8520",
    "trige": "8380", "spørring": "8380",
    # ── Tilføjet: Aalborg-omegn ──
    "nørresundby": "9400", "svenstrup": "9230", "støvring": "9530",
    "klarup": "9270", "storvorde": "9280", "gandrup": "9362",
    "sæby": "9300", "frederikshavn": "9900", "skagen": "9990",
    "hirtshals": "9850", "hjørring": "9800", "brønderslev": "9700",
    # ── Tilføjet: Esbjerg-omegn ──
    "bramming": "6740", "varde": "6800", "ribe": "6760",
    "fanø": "6720", "blåvand": "6857", "skjern": "6900",
    "ringkøbing": "6950", "hvide sande": "6960",
    # ── Tilføjet: Sønderjylland ──
    "aabenraa": "6200", "haderslev": "6100", "sønderborg": "6400",
    "tønder": "6270", "gråsten": "6300", "augustenborg": "6440",
    "nordborg": "6430", "broager": "6310",
    # ── Tilføjet: Små Sjællandske landsbyer (fra analyze-cities) ──
    "græse bakkeby": "3600", "snostrup": "3550", "freerslev": "3600",
    "asminderød": "3480", "kvistgård": "3490", "grønholt": "3480",
    "alsønderup": "3400", "gadevang": "3400", "gurre": "3100",
    "ålsgårde": "3140", "dronningmølle": "3120", "nakkehoved": "3250",
    "smidstrup": "3250", "udsholt": "3230", "tibirke sand": "3220",
    "liseleje": "3360", "skærød": "3200",
    "gl. hagested": "4390", "gislinge": "4532", "tuse": "4300",
    "gevninge": "4000", "skuldelev": "4050", "lyndby": "4070",
    "allerslev": "4070", "kirke sonnerup": "4070", "osted": "4320",
    "nye glim": "4000", "store merløse": "4330", "grevinge": "4571",
    "havdrup": "4622", "karlslunde": "2690",
    "ammendrup": "3400", "sønder strødam": "3660", "nyrup": "4480",
    "laugø": "4300", "udby": "4490",
    "tørslev hage": "3600",
    # "sønderby": "5631" FJERNET – Fyn, uden for søgeområdet
    # "lestrup": "9530" FJERNET – Jylland, uden for søgeområdet
    "ørby": "4540",
    # ── Tilføjet: fra analyze-cities 2026-04-16 (kørsel 1) ──
    "tengslemark": "4591", "vipperød": "4390",
    "ny hammersholt": "3400", "borup": "4140",
    "blistrup": "3230", "kirke syv": "4070",
    "hvedstrup": "2640", "tureby": "4640",
    # ── Tilføjet: fra --run 2026-04-16 (kørsel 2) ──
    "bjæverskov": "4632", "tikøb": "3080", "hårlev": "4652",
    "sigerslevøster": "3550", "meløse": "3310",
    "knabstrup": "4440", "tulstrup": "3400",
    "bybjerg": "4070", "undløse": "4340",
    # "jordhøj": "9881" FJERNET – Jylland, uden for søgeområdet
    "kildekrog": "2942",
    "ll. grandløse": "4420", "gøderup": "3400",
    # "langesø": "5481" FJERNET – Fyn, uden for søgeområdet
    "skibstrup": "3060",
}

def city_to_zip(city_name: str) -> Optional[str]:
    """Slå postnummer op ud fra bynavn. Returnér None hvis ukendt."""
    if not city_name:
        return None
    key = city_name.strip().lower()
    # Direkte opslag
    if key in CITY_TO_ZIP:
        return CITY_TO_ZIP[key]
    # Delvis match – find første by der indeholder søgeteksten.
    # Kræver min. 4 tegn for at undgå at korte navne (fx "egå") matcher
    # som delstreng i længere bynavne (fx "egå" i "dyssegård").
    for city, zip_code in CITY_TO_ZIP.items():
        if len(city) >= 4 and (key in city or city in key):
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
        seen_ids: set = set()

        # ── Primær parsing: store card-format listings (mobileleasetable) ──
        cards = soup.find_all('table', class_='mobileleasetable')
        logger.debug(f"Lejebolig: fandt {len(cards)} mobileleasetable-cards")

        for card in cards:
            listing = self._parse_card(card, received_at)
            if listing:
                listings.append(listing)
                if listing.get('listing_id'):
                    seen_ids.add(listing['listing_id'])

        # ── Sekundær parsing: tekst-liste nederst i emailen ──
        # Lejebolig viser 4 store cards øverst, men tilføjer en ekstra tekstliste
        # med yderligere listings nederst. Format per linje:
        #   <a href="click.lejebolig.dk/...">N værelser for X kr. pr. måned</a>
        #   <span style="display:inline-block">Type i By, Ym².</span>
        secondary = self._parse_secondary_listings(soup, received_at)
        logger.debug(f"Lejebolig: fandt {len(secondary)} sekundære listings")

        for listing in secondary:
            lid = listing.get('listing_id')
            if lid and lid in seen_ids:
                continue   # allerede fanget af kortparseren
            listings.append(listing)
            if lid:
                seen_ids.add(lid)

        return listings

    def _parse_secondary_listings(self, soup: BeautifulSoup, received_at: str) -> list[dict]:
        """
        Parser den sekundære tekst-liste nederst i Lejebolig-emails.

        Hvert element er et <span style="display:inline-block"> med teksten
        "Type i By, Xm²." efterfulgt af et click.lejebolig.dk-link i nærmeste <a>.

        Eksempel:
            <a href="click.lejebolig.dk/...">4 værelser for 18.950 kr. pr. måned</a>.
            <span style="display:inline-block">Lejlighed i København S, 101m².</span>
        """
        listings = []

        for span in soup.find_all('span', style=lambda s: s and 'display:inline-block' in s.replace(' ', '')):
            span_text = span.get_text(strip=True)

            # "Type i By, Xm²." – typen kan indeholde / (fx "Hus/villa")
            span_match = re.match(
                r'^([\w/]+(?:\s+[\w/]+)?)\s+i\s+(.+?),\s*(\d+(?:[,.]\d+)?)\s*m²\.?$',
                span_text, re.I
            )
            if not span_match:
                continue

            raw_type      = span_match.group(1).strip()
            city          = span_match.group(2).strip()
            size          = clean_float(span_match.group(3))

            # Afvis hvis city er for lang, indeholder tal, komma eller fritekst-ord
            # (komma indikerer "Adresse, By"; fritekst-ord indikerer beskrivelse)
            _BAD = {'på', 'ved', 'med', 'til', 'for', 'fra', 'og', 'af'}
            if (len(city) > 40
                    or re.search(r'\d', city)
                    or ',' in city
                    or any(f' {w} ' in f' {city.lower()} ' for w in _BAD)):
                continue

            # Find nærmeste <a href="click.lejebolig.dk"> i forældredelementet
            parent = span.parent
            a_tag = None
            if parent:
                a_tag = parent.find('a', href=re.compile(r'click\.lejebolig\.dk'))

            url        = a_tag['href'] if a_tag else None
            link_text  = a_tag.get_text(strip=True) if a_tag else ''

            # Udtræk rum og leje fra linktekst: "4 værelser for 18.950 kr. pr. måned"
            rooms_m = re.search(r'(\d+)\s+værelse', link_text, re.I)
            rent_m  = re.search(r'for\s+([\d.,]+)\s+kr', link_text, re.I)
            rooms   = int(rooms_m.group(1)) if rooms_m else None
            rent    = clean_number(rent_m.group(1)) if rent_m else None

            if not rent and not size:
                continue

            # Type-mapping (inkl. "Hus/villa")
            type_key = raw_type.lower()
            type_map = {
                'lejlighed': 'lejlighed', 'etagelejlighed': 'lejlighed',
                'villa': 'villa', 'hus': 'villa', 'hus/villa': 'villa',
                'rækkehus': 'rækkehus', 'byhus': 'villa', 'dobbelthus': 'villa',
                'værelse': 'værelse', 'andel': 'andel', 'ejendom': 'lejlighed',
                'loft': 'lejlighed', 'penthouse': 'lejlighed',
            }
            property_type = type_map.get(type_key, 'lejlighed')

            zip_code = extract_zip_code(city) or city_to_zip(city)

            # Listing-ID fra tracking-URL (lease/XXXXXX) eller indholds-hash
            listing_id = None
            if url:
                id_m = re.search(r'lease/(\d+)', url)
                listing_id = id_m.group(1) if id_m else None
            if not listing_id:
                content_key = f"{city}|{rent}|{size}|{rooms}"
                listing_id = hashlib.md5(content_key.encode()).hexdigest()[:12]

            listings.append({
                'source':           self.SOURCE,
                'listing_id':       listing_id,
                'address':          None,
                'zip_code':         zip_code,
                'city':             city,
                'rent_monthly':     rent,
                'size_sqm':         size,
                'rooms':            rooms,
                'property_type':    property_type,
                'deposit':          None,
                'available_from':   None,
                'listing_url':      url,
                'email_received_at': received_at,
            })

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
            # Bred match: "[Boligtype] i [By]" – dækker alle kendte og ukendte typer.
            # Boligtype-listen er udvidet; fallback fanger alle andre "<ord(e)> i <By>"-mønstre.
            m = re.match(
                r'^(Lejlighed|Etagelejlighed|Villa|Rækkehus|Hus|Byhus|Værelse'
                r'|Andel|Andelslejlighed|Ejendom|Stuehus|Loft|Penthouse'
                r'|Bungalow|Dobbelthus|Fritidshus|Sommerhus|Tofamilieshus'
                r'|[\w\-]+(?:\s+[\w\-]+)?)'   # fallback: ét eller to ord (fx "3-værelses")
                r'\s+i\s+(.+)$',
                text, re.I
            )
            if m:
                raw_type = m.group(1).lower().strip()
                candidate_city = m.group(2).strip()
                # Afvis matches der ser ud som sætninger snarere end bynavne
                # (fx "god stand i forhold til..." – by bør ikke indeholde tal eller være >40 tegn)
                if len(candidate_city) > 40 or re.search(r'\d', candidate_city):
                    continue
                city = candidate_city
                type_map = {
                    'lejlighed': 'lejlighed', 'etagelejlighed': 'lejlighed',
                    'villa': 'villa', 'rækkehus': 'rækkehus',
                    'hus': 'villa', 'byhus': 'villa', 'dobbelthus': 'villa',
                    'tofamilieshus': 'villa', 'stuehus': 'villa', 'bungalow': 'villa',
                    'værelse': 'værelse', 'andel': 'andel', 'andelslejlighed': 'andel',
                    'ejendom': 'lejlighed', 'loft': 'lejlighed', 'penthouse': 'lejlighed',
                    'fritidshus': 'villa', 'sommerhus': 'villa',
                }
                property_type = type_map.get(raw_type, 'lejlighed')
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
            # Forsøg at udtrække numerisk ID fra tracking-URL
            id_match = re.search(r'lease/(\d+)', url)
            if id_match:
                listing_id = id_match.group(1)
            else:
                # Stabil indholds-baseret ID – hash af titel+by+leje+størrelse.
                # Bruger hashlib (deterministisk) i stedet for hash() som skifter
                # seed ved hver Python-start og dermed skaber falske duplikater.
                content_key = f"{title}|{city}|{rent}|{size}"
                listing_id = hashlib.md5(content_key.encode()).hexdigest()[:12]

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
