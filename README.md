# Congress Bill Tracker

Scrapes bill data from the [Congress.gov API](https://api.congress.gov/) nightly,
stores it in DuckDB, and serves an interactive D3.js dashboard via GitHub Pages — all for free.

---

## Project layout

```
congress-tracker/
├── scraper.py                   ← main scraper + JSON exporter
├── local_server.py              ← dev web server for the D3 dashboard
├── environment.yml              ← Conda environment definition
├── .env.example                 ← copy to .env and add your API key
├── .gitignore
├── congress.db                  ← DuckDB file (auto-created + committed)
├── docs/
│   ├── index.html               ← D3 dashboard (served by GitHub Pages)
│   └── data/
│       ├── bills.json
│       ├── by_party.json
│       ├── by_state.json
│       ├── over_time.json
│       └── by_chamber.json
├── tests/
│   └── test_scraper.py
└── .github/
    └── workflows/
        └── scrape.yml           ← nightly GitHub Actions cron
```

---

## 1 · Get a free API key

Sign up at <https://api.congress.gov/sign-up/> — no credit card, instant approval.

---

## 2 · Local setup with Anaconda + Spyder

### Create the Conda environment

Open **Anaconda Prompt** (Windows) or a terminal (Mac/Linux):

```bash
# Navigate to the project folder
cd path/to/congress-tracker

# Create the environment from the provided file
conda env create -f environment.yml

# Activate it
conda activate congress-tracker
```

### Set your API key locally

```bash
# Copy the example file
cp .env.example .env

# Edit .env and replace your_key_here with your real key
```

On **Windows** use Notepad or any text editor to edit `.env`.

### Run the scraper from Spyder

1. Open Spyder, then open `scraper.py`
2. In **Spyder → Preferences → Python interpreter**, choose the
   `congress-tracker` conda environment you just created
3. In the top-right of Spyder, make sure the working directory is the
   project root (where `scraper.py` lives)
4. Run the script with **F5** or the green play button

Alternatively, from the **IPython console** pane at the bottom of Spyder:

```python
# Make sure your key is loaded — python-dotenv picks up .env automatically
import os
os.environ["CONGRESS_API_KEY"] = "your_key_here"  # or use .env

%run scraper.py
```

### Preview the dashboard locally

After the scraper has run and exported JSON, start the dev server:

```bash
# From a terminal (or Spyder's IPython console)
python local_server.py
```

This opens <http://localhost:8000> in your browser automatically.
The server is needed because browsers block `file://` AJAX requests (CORS).

### Run tests

```bash
pytest tests/
```

---

## 3 · Deploy to GitHub

### First-time setup

```bash
git init
git add .
git commit -m "init: congress tracker"

# Create a repo on GitHub, then:
git remote add origin https://github.com/YOUR_USERNAME/congress-tracker.git
git branch -M main
git push -u origin main
```

### Add your API key as a GitHub secret

1. GitHub repo → **Settings → Secrets and variables → Actions**
2. Click **New repository secret**
3. Name: `CONGRESS_API_KEY`   Value: your key

### Enable GitHub Pages

1. GitHub repo → **Settings → Pages**
2. Source: **Deploy from a branch**
3. Branch: `main`   Folder: `/docs`
4. Save — your dashboard will be live at
   `https://YOUR_USERNAME.github.io/congress-tracker/`

### Trigger the first scrape manually

GitHub → **Actions tab** → **Nightly Congress Scrape** → **Run workflow**

After it finishes, the DB and JSON files will be committed automatically,
and GitHub Pages will redeploy the dashboard within ~60 seconds.

---

## 4 · Hosting cost summary

| Component | Where | Cost |
|-----------|-------|------|
| Nightly scraper | GitHub Actions (2,000 min/month free) | **Free** |
| DB + JSON storage | Git repo | **Free** |
| D3 dashboard | GitHub Pages | **Free** |
| Live query API (optional) | Railway / Fly.io free tier | Free–$3/mo |

---

## 5 · Customising the scraper

Edit the constants at the top of `scraper.py`:

| Variable | Default | Description |
|----------|---------|-------------|
| `CONGRESS` | `119` | Which congress to scrape |
| `MAX_PAGES` | `20` | Max pages per run (250 bills/page) |
| `DB_PATH` | `congress.db` | Path to the DuckDB file |
| `DATA_DIR` | `docs/data/` | Where JSON exports land |

To scrape a different bill type (Senate resolutions, joint resolutions, etc.)
edit the `fetch_bills` function's URL path:
```python
# e.g. Senate bills only:
f"{BASE_URL}/bill/{congress}/s"
```

---

## 6 · Querying the DB directly in Spyder

```python
import duckdb
con = duckdb.connect("congress.db")

# All Democratic bills from Minnesota
con.execute("""
    SELECT title, introduced_date, latest_action
    FROM bills
    WHERE sponsor_party = 'D' AND sponsor_state = 'MN'
    ORDER BY introduced_date DESC
    LIMIT 20
""").fetchdf()

# Monthly bill counts
con.execute("""
    SELECT strftime(introduced_date, '%Y-%m') AS month, COUNT(*) AS n
    FROM bills GROUP BY month ORDER BY month
""").fetchdf()
```
