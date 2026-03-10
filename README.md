# TUNI TREPO downloads scraper

This project scrapes recent submissions from TREPO, stores publication metadata and download counts in SQLite, prints summary reports, and serves a small Flask dashboard.

Download counts are fetched from TREPO's JSON statistics endpoint at `/simplestats/rest?handle=10024/234747`, using the handle identifier from each publication URL.

## Setup

PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pip install -e .
```

## Usage

Scrape from the saved offset:

```powershell
trepo-scraper scrape --delay 1.0
```

If you already scraped pages before the JSON stats endpoint was wired in, force a backfill of download counts:

```powershell
trepo-scraper scrape --start-offset 0 --refresh-downloads --delay 1.0
```

Limit a test run to 2 listing pages:

```powershell
trepo-scraper scrape --limit-pages 2 --delay 1.0
```

Print reports:

```powershell
trepo-scraper report
```

Export current data to JSON:

```powershell
trepo-scraper export-json --output data/publications.json
```

Reset progress back to the beginning:

```powershell
trepo-scraper reset-progress --offset 0
```

Start the dashboard:

```powershell
trepo-scraper serve --host 127.0.0.1 --port 5000
```

## Stored data

The SQLite database lives at `data/trepo_scraper.db` by default.

It stores:

- scraper progress by listing offset
- publication title
- author
- year
- type
- handle URL
- download count when present

The scraper resumes from the last completed listing page. If a run stops midway through a page, rerunning is safe because publication rows are upserted.
