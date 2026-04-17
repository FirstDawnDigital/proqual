# Turso + Render + GitHub Pages — trin-for-trin

**Arkitektur:**
```
Mac Mini (scraper)
  → lokal SQLite
  → sync → Turso (hosted SQLite i skyen)
              ↑
         Render (Flask API) ← browser
              ↓
         GitHub Pages (statisk HTML som fallback)
```

---

## DEL 1 — GitHub Pages (gør repo offentligt)

GitHub Pages er gratis for offentlige repos.

1. Gå til **github.com/FirstDawnDigital/proqual** → **Settings**
2. Scroll ned til **Danger Zone** → **Change repository visibility** → **Make public**
3. Gå til **Settings** → **Pages** → Source: **GitHub Actions** → Save

Siden er nu live på: **https://firstdawndigital.github.io/proqual/**

---

## DEL 2 — Turso (hosted database)

### Trin 1 — Opret konto og installer CLI

```bash
# Installer Turso CLI
curl -sSfL https://get.tur.so/install.sh | bash

# Log ind (åbner browser)
turso auth login
```

### Trin 2 — Opret database

```bash
turso db create ejendom
```

Notér output — du får en URL som:
```
libsql://ejendom-firstdawndigital.turso.io
```

### Trin 3 — Hent auth token

```bash
turso db tokens create ejendom
```

Kopiér tokenet (vises kun én gang).

### Trin 4 — Tilføj til config.env

Åbn `/Users/server/Documents/ejendompython/config.env` og tilføj:
```
TURSO_URL=libsql://ejendom-firstdawndigital.turso.io
TURSO_AUTH_TOKEN=eyJ...dit_token_her...
```

### Trin 5 — Installer Python-client og synk første gang

```bash
cd /Users/server/Documents/ejendompython
source .venv/bin/activate
pip install libsql-experimental --break-system-packages

# Synkronisér den fulde database til Turso (første gang — tager et minut)
python main.py --sync-turso
```

Du bør se:
```
✓ Turso synkroniseret
```

---

## DEL 3 — Render (Flask-app i skyen)

### Trin 1 — Opret konto

Gå til **render.com** og sign up med GitHub.

### Trin 2 — Opret ny Web Service

1. Dashboard → **New** → **Web Service**
2. Vælg **Connect a repository** → vælg **proqual**
3. Render registrerer automatisk `render.yaml` og udfylder felterne
4. Tjek at det ser rigtigt ud:
   - **Runtime**: Python
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `gunicorn webapp.app:app --bind 0.0.0.0:$PORT --workers 2 --timeout 120`

### Trin 3 — Tilføj miljøvariable

Under **Environment** → **Add Environment Variable**:

| Key | Value |
|---|---|
| `TURSO_URL` | `libsql://ejendom-firstdawndigital.turso.io` |
| `TURSO_AUTH_TOKEN` | `eyJ...dit_token...` |

### Trin 4 — Deploy

Klik **Create Web Service**. Render bygger og deployer automatisk.

Efter ~3 minutter er appen live på:
```
https://ejendompython.onrender.com
```
(eller hvad Render tildeler — kopier URL'en fra dashboardet)

### Trin 5 — Test

Åbn URL'en i browseren. Du bør se ejendomsoversigten med data fra Turso.

---

## DEL 4 — Daglig automatisk opdatering

Scheduleren i `main.py` kører allerede synk dagligt kl. 07:00:
```
Scraping → Sheets → Turso-sync → Statisk HTML
```

Når Mac Mini synker til Turso, henter Render automatisk friske data ved næste API-kald
(Turso er shared database — ingen restart af Render nødvendig).

Vil du også have Render til at redeploy dagligt (hvis koden ændrer sig):
- Render → dit service → **Settings** → **Auto-Deploy**: ON (default)
- Render rebuilder automatisk når du pusher til GitHub

---

## Hvad der nu virker

| Funktion | Løsning |
|---|---|
| Statisk visning (hurtig) | GitHub Pages → `docs/index.html` |
| Dynamisk app med live data | Render → Flask → Turso |
| Favoritter på tværs af enheder | Render/Turso (server-side annotations) |
| Daglig opdatering | Mac Mini scheduler → Turso-sync |
| Adgang udenfor hjemmenetværk | Render URL (ingen VPN, ingen port-forward) |

---

## Hurtig fejlfinding

**Render viser ingen data:**
- Tjek at TURSO_URL og TURSO_AUTH_TOKEN er sat korrekt i Render dashboard
- Kør `python main.py --sync-turso` på Mac Mini og tjek output

**Turso-sync fejler:**
- Tjek at `libsql-experimental` er installeret: `pip install libsql-experimental --break-system-packages`
- Tjek at TURSO_URL starter med `libsql://` (ikke `https://`)

**Render sover (kold start):**
- Gratis Render-tier sover efter 15 min inaktivitet → første load tager ~30 sek
- Løsning: opgrader til Render Starter ($7/md) eller brug UptimeRobot til at pinge siden hvert 10. minut (gratis)
