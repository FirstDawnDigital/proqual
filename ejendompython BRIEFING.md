# Briefing: Ejendomsinvesteringsværktøj

Læs denne fil først. Den beskriver projektets nuværende tilstand og hvad der skal laves.

---

## Projektoverblik

Et Python-baseret datapipeline-system der kører på en Mac Mini M1. Det:
1. Henter boligannoncer fra Gmail (Boligportal + Lejebolig annonce-agenter)
2. Parser HTML-emails og udtrækker: adresse, by, postnummer, leje, m², rum, boligtype
3. Gemmer rådata i en SQLite-database
4. Aggregerer huslejepriser pr. postnummer og rum-antal
5. Eksporterer til Google Sheets (tre faner)

**Workspacemappen hedder:** `Ejendomsberegner/`  
**Pythonprojektmappen:** `Ejendomsberegner/ejendomsystem/`  
**På Mac Mini køres alt fra:** `/Users/server/Documents/ejendompython/`

---

## Filstruktur

```
Ejendomsberegner/
├── BRIEFING.md                          ← denne fil
├── ARKITEKTUR.md                        ← teknisk arkitekturdokument
├── Rentabilitetsberegner.xlsx           ← eksisterende regneark (reference)
├── Boliger til salg.xlsx                ← eksisterende regneark (reference)
└── ejendomsystem/
    ├── main.py                          ← CLI entry point
    ├── database.py                      ← SQLite schema + hjælpefunktioner
    ├── config.example.env               ← skabelon for config
    ├── requirements.txt
    ├── email_parser/
    │   └── gmail_parser.py              ← Gmail IMAP + Boligportal + Lejebolig parsere
    ├── scrapers/
    │   └── boligportal_scraper.py       ← web scraper (ikke aktiv pt.)
    └── exporters/
        └── sheets_exporter.py           ← Google Sheets eksport (3 sheets)
```

På Mac Mini ligger koden i `/Users/server/Documents/ejendompython/` med `.venv` og `config.env`.

---

## Hvad virker i dag

### Pipeline
```bash
python main.py --run --days-back 7    # normal daglig kørsel (~30 sek)
python main.py --run --days-back 30   # historisk kørsel (~3 min)
python main.py --export-sheets        # eksporter til Sheets
python main.py --status               # databasestatistik
python main.py --debug-email info@boligportal.dk   # dump rå HTML fra email
python main.py --discover-senders     # find boligrelaterede afsendere i Gmail
```

### Database (seneste kørsel)
- **3.826 lejeboliger** i databasen
- **66 postnumre** med data
- **510 aggregat-kombinationer** (postnummer × rum)
- Kilder: `boligportal` (majoriteten) + `lejebolig`

### Google Sheets – tre faner
1. **"Huslejedata"** – 66 rækker, én pr. postnummer, med kr/m² lav/median/høj + månedsleje
2. **"Husleje pr. rum"** – 63 rækker, pivot: postnummer × 1/2/3/4/5 rum, median månedsleje
3. **"Rådata"** – 3.826 individuelle listings til verifikation

### Email-parsere
Begge er verificeret mod rigtige emails:
- **BoligportalEmailParser** – finder `div.class="listing-item-section"`, udtrækker 5 tekstlinjer
- **LejeboligEmailParser** – finder `table.class="mobileleasetable"`, bruger ikonbilleder til rum/m²

Postnummer slås op via `CITY_TO_ZIP`-dict (by → postnummer) da ingen af kilderne sender ZIP direkte.

### Config (på Mac Mini, ikke i repo)
```
GMAIL_ADDRESS=slejlighed@gmail.com
GMAIL_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx
BOLIGPORTAL_SENDER=info@boligportal.dk
LEJEBOLIG_SENDER=noreply@lejebolig.dk
SHEETS_RENTAL_AGGREGATES_ID=<google-sheet-id>
GOOGLE_CREDENTIALS_FILE=google_credentials.json
DB_PATH=data/ejendom.db
```

---

## Udestående opgaver (arbejd på disse)

### Opgave 1 – Huslejedata-sheet: kr/m² skal være ÅRLIG ikke månedlig

I `exporters/sheets_exporter.py`, funktionen `export_sqm_aggregates()`:

Kolonnenavnene hedder i dag "Kr/m² lav", "Kr/m² median", "Kr/m² høj" og viser **månedlig** pris.  
De skal ændres til **årsværdi** (gange 12) og hedde "Kr/m²/år lav", "Kr/m²/år median", "Kr/m²/år høj".

Konkret: på linjerne der bygger `data`-arrayet, ændr:
```python
_fmt(r["price_per_sqm_low"], 1),
_fmt(r["price_per_sqm_median"], 1),
_fmt(r["price_per_sqm_high"], 1),
```
til:
```python
_fmt(r["price_per_sqm_low"]  * 12, 0) if r["price_per_sqm_low"]  else "",
_fmt(r["price_per_sqm_median"] * 12, 0) if r["price_per_sqm_median"] else "",
_fmt(r["price_per_sqm_high"] * 12, 0) if r["price_per_sqm_high"]  else "",
```
Og opdatér header-listen tilsvarende.

---

### Opgave 2 – Rådata-sheet: gør alle URL'er klikbare

I `exporters/sheets_exporter.py`, funktionen `export_raw_listings()`:

Boligportal-URL'er er klikbare i Sheets, Lejebolig er ikke (tracking-URLs fra `click.lejebolig.dk` er for lange til auto-detection).

Fix: Wrap alle URL'er i `=HYPERLINK()`-formlen. I data-arrayet, ændr URL-kolonnen fra:
```python
r["listing_url"] or "",
```
til en hjælpefunktion:
```python
def make_hyperlink(url):
    if not url:
        return ""
    # Google Sheets HYPERLINK maks ~2000 tegn
    if len(url) > 1900:
        return url  # for lang – vis rå URL
    escaped = url.replace('"', '%22')
    return f'=HYPERLINK("{escaped}","Se annonce")'
```

---

### Opgave 3 – Undersøg listings i postnummer 8250

Postnummer 8250 er Egå ved Aarhus. Brugeren siger det er uden for deres søgning.

Undersøg ved at køre SQL på databasen `data/ejendom.db`:
```sql
SELECT address, city, zip_code, source, listing_url
FROM rental_listings
WHERE zip_code = '8250'
LIMIT 20;
```

Mulige årsager:
1. **Forkert city→zip mapping** – tjek om noget i `CITY_TO_ZIP`-dictet i `gmail_parser.py`
   matcher utilsigtet. F.eks. kan "Egå" optræde som delstreng i et andet bynavn.
2. **Boligportals søgeagent er for bred** – annoncen er faktisk fra Egå og agenten inkluderer den.
3. **Falsk postnummer fra titel** – titlen indeholder "8250" som tal (f.eks. en pris eller areal).

Find den konkrete årsag og ret enten mapping-logikken eller rapportér til brugeren at det er Boligportals søgeagent der er for bred.

Den relevante kode er `city_to_zip()` og `CITY_TO_ZIP`-dictet i:
`ejendomsystem/email_parser/gmail_parser.py`

---

## Næste større opgave (efter ovenstående)

**Koble Rentabilitetsberegneren op mod live Sheets-data.**

Den eksisterende `Rentabilitetsberegner.xlsx` har en "Generel rentabilitetsberegner"-fane der bruger VLOOKUP på et "Leje"-sheet med manuelle data. Det skal erstattes af live-data fra "Huslejedata"-sheetet i det samme Google Sheets-dokument.

Derudover mangler **"Boliger til salg"**-datapipeline (scraping af Boligsiden/Boliga).

---

## Tekniske noter

- Python 3.11 i `.venv`
- `pip install -r requirements.txt --break-system-packages` hvis pakker mangler
- Database-fil: `data/ejendom.db` (SQLite, ingen server nødvendig)
- Google credentials: `google_credentials.json` (service account, ikke i repo)
- Boligportal emails ligger i Gmail All Mail (ikke INBOX) – hentes via X-GM-RAW
- Lejebolig emails ligger i INBOX
- Begge parsere er verificeret mod rigtige email-HTML-filer
- `CITY_TO_ZIP`-dict i `gmail_parser.py` bruges til ZIP-opslag da ingen af kilderne sender postnummer direkte
