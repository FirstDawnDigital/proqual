# Briefing: Ejendomsinvesterings-webapp

Dette dokument beskriver en simpel web app der skal vise ejendomsdata fra et eksisterende Python/SQLite-system. Start med en HTML-prototype, skaler senere til Streamlit eller lignende.

---

## Kontekst og formål

Vi har bygget et datapipeline-system der:
1. Henter **lejeboliger** fra Gmail (Boligportal + Lejebolig annonce-agenter) og scraper webs
2. Scraper **salgsboliger** fra Boligsiden.dk via deres REST API
3. Gemmer alt i SQLite og beregner aggregater pr. postnummer
4. Eksporterer til Google Sheets

Formålet med web app'en er at gøre det nemt at **screene salgsboliger for investeringspotentiale** — dvs. finde boliger med et attraktivt estimeret lejeafkast relativt til salgsprisen.

---

## Datakilde

**Database:** SQLite, fil: `data/ejendom.db`  
**Sti på Mac Mini:** `/Users/server/Documents/ejendompython/data/ejendom.db`

### Tabel: `properties_for_sale`
Salgsboliger scraped fra Boligsiden.dk.

| Kolonne | Type | Beskrivelse |
|---|---|---|
| `id` | INTEGER | Primærnøgle |
| `source` | TEXT | Altid `'boligsiden'` |
| `listing_id` | TEXT | Boligsidens case-ID (UUID) |
| `address` | TEXT | F.eks. `'Ellevej 80, 3300 Frederiksværk'` |
| `zip_code` | TEXT | F.eks. `'3300'` |
| `city` | TEXT | F.eks. `'Frederiksværk'` |
| `price` | INTEGER | Kontantpris i kr, f.eks. `1850000` |
| `size_sqm` | REAL | Boligareal i m², f.eks. `98.0` |
| `rooms` | INTEGER | Antal rum, f.eks. `4` |
| `property_type` | TEXT | `villa`, `ejerlejlighed`, `rækkehus`, `villalejlighed`, `landejendom` m.fl. |
| `owner_costs_monthly` | INTEGER | Ejerudgifter pr. måned i kr |
| `energy_label` | TEXT | `A`–`G` |
| `listing_url` | TEXT | Direkte URL til annonce, f.eks. `https://www.boligsiden.dk/adresse/ellevej-80-3300-frederiksvaerk` |
| `scraped_at` | TEXT | ISO-timestamp for hvornår den blev hentet |
| `days_on_market` | INTEGER | Dage siden første annoncering (fra API, kan være NULL) |
| `price_change_count` | INTEGER | Antal prisændringer siden opslag (fra API, kan være NULL) |
| `price_change_amount` | INTEGER | Samlet prisændring i kr — negativ = reduktion (fra API, kan være NULL) |
| `latitude` | REAL | Breddegrad (fra API eller Nominatim geocoding) |
| `longitude` | REAL | Længdegrad (fra API eller Nominatim geocoding) |
| `commute_minutes` | INTEGER | Køretid i bil fra Egevangen 19 (beregnet med `--enrich-commute`, kan være NULL) |

**Antal rækker:** ~695 (seneste scrape dækkede Nordsjælland + omegn, op til 2.500.000 kr)

### Tabel: `rental_aggregates`
Aggregerede lejepriser beregnet fra `rental_listings` (lejeboliger fra emails).

Relevante kolonner:

| Kolonne | Type | Beskrivelse |
|---|---|---|
| `zip_code` | TEXT | Postnummer |
| `rooms` | INTEGER | NULL = alle rum, ellers specifikt antal |
| `property_type` | TEXT | NULL = alle typer |
| `price_per_sqm_median` | REAL | Median månedsleje pr. m² i kr |
| `rent_total_median` | REAL | Median månedsleje i alt i kr |
| `sample_size` | INTEGER | Antal datapunkter bag aggregatet |

**Antal postnumre med data:** ~96  
**Kilder:** Boligportal + Lejebolig annoncer fra Gmail, ~5.000+ listings

---

## Nøgleberegninger

### Estimeret månedsleje (to metoder)

**Metode 1 — m²-baseret** *(foretrukket, skalerer med areal)*
```
est_leje_md = price_per_sqm_median × size_sqm
```
Bruges: `rental_aggregates WHERE zip_code = X AND rooms IS NULL AND property_type IS NULL`

**Metode 2 — rum-baseret** *(supplement, baseret på faktiske udlejninger)*
```
est_leje_md = rent_total_median
```
Bruges: `rental_aggregates WHERE zip_code = X AND rooms = Y AND property_type IS NULL`

### Estimeret bruttolejeafkast
```
afkast_pct = (est_leje_md × 12) / price × 100
```

Dette er et **brutto**-afkast — det tager ikke højde for tomgang, vedligeholdelse, ejerudgifter, skat eller finansiering. Det er et screeningsværktøj, ikke en investeringsanalyse.

### Salgspris pr. m²
```
sale_kr_per_sqm = price / size_sqm
```

---

## SQL til app'en

Hoved-query der returnerer alt vi skal bruge til visning:

```sql
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
    -- Markedsdata
    p.days_on_market,
    p.price_change_count,
    p.price_change_amount,
    -- Køretid (NULL indtil --enrich-commute er kørt)
    p.commute_minutes,
    -- Salgspris pr. m²
    CASE WHEN p.size_sqm > 0
         THEN ROUND(CAST(p.price AS REAL) / p.size_sqm, 0)
         ELSE NULL END AS sale_kr_per_sqm,
    -- Estimeret leje og afkast via m²
    ra_zip.price_per_sqm_median AS leje_kr_per_sqm,
    CASE WHEN p.size_sqm > 0 AND ra_zip.price_per_sqm_median IS NOT NULL
         THEN ROUND(ra_zip.price_per_sqm_median * p.size_sqm, 0)
         ELSE NULL END AS est_leje_sqm,
    CASE WHEN p.price > 0 AND p.size_sqm > 0 AND ra_zip.price_per_sqm_median IS NOT NULL
         THEN ROUND((ra_zip.price_per_sqm_median * p.size_sqm * 12.0) / p.price * 100, 1)
         ELSE NULL END AS yield_sqm_pct,
    -- Estimeret leje og afkast via rum
    ra_rooms.rent_total_median AS est_leje_rooms,
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
ORDER BY yield_sqm_pct DESC NULLS LAST
```

---

## Hvad app'en skal kunne (MVP)

### Visning
- Tabel med alle salgsboliger, sorteret efter **afkast m² %** (højest øverst)
- Kolonner: Adresse, By, Pris, m², Rum, Type, Ejerudg./md, Energimærke, Kr/m² (salg), Est. leje/md (m²), **Afkast m² %**, Est. leje/md (rum), **Afkast rum %**, Liggetid, Prisændring, Køretid, Link
- Klikbare links til Boligsiden-annoncen
- Tomme afkast-celler (ingen lejedata for postnummeret) vises tydeligt som "–"
- `days_on_market`: vis som "X dage" — lang liggetid kan indikere svær salgbarhed
- `price_change_amount`: vis som "−150.000 kr" (rød) eller tom — prisreduktion = forhandlingsmargin
- `commute_minutes`: vis som "X min" — NULL = ikke beregnet endnu

### Filtrering
- **Postnummer** (multi-select eller fritekst)
- **Boligtype** (villa, ejerlejlighed, rækkehus, alle)
- **Maks pris** (slider eller inputfelt, f.eks. 0–2.500.000 kr)
- **Minimum afkast %** (slider, f.eks. 0–15%)
- **Antal rum** (1–6+)
- **Maks køretid** (slider, f.eks. 0–60 min) — filtrerer kun hvis commute_minutes ikke er NULL

### Sortering
- Klik på kolonneoverskrift sorterer op/ned
- Default: afkast m² % faldende

---

## Teknisk udgangspunkt

**Fase 1 — Simpel HTML-fil** (det vi starter med nu)
- Én `.html`-fil der læser data fra SQLite via et lille Python-script der dumper JSON
- Eller: Python starter en minimal HTTP-server (`http.server` eller Flask) der serverer data som JSON til en HTML/JS frontend
- Ingen build-step, ingen npm, ingen framework — bare HTML + vanilla JS (eller minimal bibliotek som Alpine.js)

**Fase 2 — Streamlit** (når HTML-MVP er valideret)
- `pip install streamlit`
- Kør med `streamlit run app.py`
- Nem filtrering med `st.sidebar`, `st.dataframe`, `st.slider` osv.

---

## Miljø

- **Mac Mini M1**, macOS
- **Python 3.11** i `.venv`
- **Database:** `/Users/server/Documents/ejendompython/data/ejendom.db`
- Ingen ekstern server, ingen API-nøgle nødvendig — ren lokal SQLite
- Eksisterende dependencies: `requests`, `beautifulsoup4`, `gspread`, `google-auth`
- Nye dependencies der må tilføjes: `flask` (hvis nødvendig), `streamlit` (fase 2)

---

## Eksempel på datarækker

| Adresse | Pris | m² | Rum | Est. leje/md (m²) | Afkast m² % |
|---|---|---|---|---|---|
| Tølløsevej 196, 4340 | 1.195.000 | 199 | 6 | 26.905 | 27% |
| Brederødvej 12, 3300 | 1.175.000 | 102 | 4 | 14.035 | 14% |
| Sdr. Jernløsevej 75, 4420 | 370.000 | 52 | 2 | 7.176 | 23% |

*(Høje afkast-% indikerer billige boliger i områder med relativt høj leje — bør valideres manuelt)*

---

## Hvad der IKKE er scope for MVP

- Brugerlogin / autentifikation
- Gemme favoritter
- Historiske prisdata
- Kort/geografisk visning
- Nettoafkast (efter omkostninger) — kun brutto
