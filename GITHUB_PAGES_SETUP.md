# GitHub Pages setup — trin-for-trin

Resultatet: en URL som `https://esbvall.github.io/ejendompython/` der viser
din ejendomsoversigt med filtrering og sortering. Opdateres dagligt automatisk.

---

## Trin 1 — Opret et privat GitHub-repository

1. Gå til https://github.com/new
2. Navn: `ejendompython` (eller hvad du vil)
3. Sæt det til **Private** (dine boligdata skal ikke være offentlige)
4. Klik "Create repository"

> **Bemærk:** GitHub Pages virker på private repos med en gratis GitHub-konto,
> men siden selv er **offentlig** (alle med URL'en kan se den).
> Databasen og credentials er i .gitignore og pusher ALDRIG til GitHub.

---

## Trin 2 — Initialiser git og push projektet

Åbn Terminal og kør:

```bash
cd /Users/server/Documents/ejendompython

# Initialiser git (første gang)
git init
git branch -M main

# Tilføj GitHub som remote (erstat USERNAME med dit GitHub-brugernavn)
git remote add origin https://github.com/USERNAME/ejendompython.git

# Generér den statiske HTML (opretter docs/index.html)
source .venv/bin/activate
python webapp/export_static.py

# Tilføj og push (database og credentials er i .gitignore og pushes IKKE)
git add .
git commit -m "Første push med statisk ejendomsoversigt"
git push -u origin main
```

---

## Trin 3 — Aktiver GitHub Pages

1. Gå til dit repo på GitHub → **Settings** → **Pages**
2. Under "Build and deployment":
   - Source: **GitHub Actions**
3. Klik **Save**

GitHub kører nu workflowet i `.github/workflows/deploy-pages.yml` automatisk.
Efter ~1 minut er siden live på:
```
https://USERNAME.github.io/ejendompython/
```

---

## Trin 4 — Daglig automatisk opdatering (Mac Mini)

Scheduleren i `main.py` kører allerede `export_static.py` dagligt kl. 07:00.
Du skal blot sætte git op til at pushe automatisk bagefter.

Tilføj dette til din `run_pipeline.sh` (eller lav et nyt script):

```bash
#!/bin/bash
cd /Users/server/Documents/ejendompython
source .venv/bin/activate

# Generér statisk HTML
python webapp/export_static.py

# Push til GitHub (kræver at git remote er sat op, se Trin 2)
git add docs/index.html
git commit -m "Daglig opdatering $(date +%Y-%m-%d)"
git push origin main
```

Gem scriptet som `push_static.sh` og gør det eksekverbart:
```bash
chmod +x push_static.sh
```

Tilføj en cron-job der kører det kl. 07:30 (efter pipeline er færdig):
```bash
crontab -e
# Tilføj denne linje:
30 7 * * * /Users/server/Documents/ejendompython/push_static.sh >> /Users/server/Documents/ejendompython/data/push.log 2>&1
```

---

## Hvad der pushes (og hvad der IKKE gør)

| Fil/mappe | Pushes? | Forklaring |
|---|---|---|
| `docs/index.html` | ✅ Ja | Den genererede statiske HTML med bagt data |
| `webapp/`, `scrapers/`, etc. | ✅ Ja | Kildekoden |
| `data/ejendom.db` | ❌ Nej | Databasen med private data (.gitignore) |
| `config.env` | ❌ Nej | API-nøgler og passwords (.gitignore) |
| `google_credentials.json` | ❌ Nej | Google OAuth (.gitignore) |

---

## Tilføj adgangskode til siden (valgfrit, anbefales)

Siden er offentlig for alle med URL'en. Vil du beskytte den med adgangskode,
kan du tilføje et simpelt password-check i `export_static.py`:

I `patch_html()` i `export_static.py`, tilføj øverst i `<body>`:

```javascript
// Simpel klient-side adgangskode (ikke 100% sikker, men afskrækker)
const pw = localStorage.getItem("site_pw");
if (pw !== "DIT_KODEORD") {
  const input = prompt("Adgangskode:");
  if (input !== "DIT_KODEORD") { document.body.innerHTML = "Adgang nægtet."; }
  else { localStorage.setItem("site_pw", input); }
}
```

Erstat `DIT_KODEORD` med noget kun du og dine partnere kender.

---

## Hurtig test inden push

```bash
cd /Users/server/Documents/ejendompython
source .venv/bin/activate
python webapp/export_static.py
open docs/index.html   # Åbn i browser — skal virke uden Flask!
```
