# Index Tracker — Claude Code Instructions

## Project Overview

Flask web app that tracks the composition of 32 financial indices (S&P 500, FTSE 100, DAX, Forbes Global 2000, etc.). Built for Kernel internal use.

- **Entry point**: `app.py` (Flask server)
- **Scraper module**: `scrapers.py` — all scraping logic lives here
- **Data persistence**: `data/*.json` (gitignored)
- **Templates**: `templates/index.html` — single-page app
- **Deployment**: Render (`render.yaml`)

**Local dev**: `source venv/bin/activate && python app.py` → `localhost:5000`

---

## Scraper Architecture

All 32 indices are defined in `scrapers.py::INDICES_CONFIG`. Each entry specifies a `source` type that maps to a scraper function.

| Source | Function | Notes |
|--------|----------|-------|
| `wikipedia` | `scrape_wikipedia()` | Reliable for most indices. Detects table automatically. |
| `ishares` | `scrape_ishares()` | iShares ETF holdings CSV download. Includes per-company country. |
| `nasdaq` | `scrape_nasdaq()` | Nasdaq screener CSV API. ~3,500 stocks with country. |
| `fortune` | `scrape_fortune()` | Fortune franchise API → Playwright fallback. **Brittle.** |
| `forbes` | `scrape_forbes()` | Forbes JSON API → Playwright fallback. **Year-sensitive.** |

---

## Index Status & Known Blockers

### Working (Wikipedia — reliable)
OMXC25, OMXH25, OBX, TA-35, SMI, AEX, BEL 20, OMX Stockholm 30, IBEX 35, NIFTY 50, Nikkei 225, Euro STOXX 50, DJI, CAC 40, CAC Next 20, Nasdaq 100, DAX, TecDAX, FTSE 100, FTSE 250, MDAX, SDAX, S&P 500

### Working (iShares CSV — reliable but URL-sensitive)
STOXX Europe 600, STOXX Global 1800, MSCI World, S&P Global 1200
- If a 403 is returned, the iShares CDN URL may have changed. Use `scrape_as_markdown` on the iShares product page to find the new CSV download link.

### Working (Nasdaq API)
Nasdaq Composite — returns full universe ~3,500 stocks with country column.

### Fragile — use Brightdata MCP to fix/test
- **Fortune 500 / Fortune 1000**: `fortune.com/franchise-api/v1/items/fortune500` changes format periodically. Playwright fallback requires a headless browser (not available on Render). Use `scrape_as_markdown("https://fortune.com/ranking/fortune500/")` to diagnose.
- **Forbes Global 2000**: `forbesapi` endpoint is year-sensitive — the 2026 list is not published yet (published ~May/June). Hardcode year to 2025 until then. Use `scrape_as_markdown("https://www.forbes.com/lists/global2000/")` to inspect current page.
- **FTSE 350**: Wikipedia has no component table. Use `search_engine("FTSE 350 index constituents full list 2025")` to find an authoritative source (ftserussell.com, londonstockexchange.com, or a reliable ETF provider).

---

## Brightdata MCP

**Registered as**: `brightdata` MCP (SSE transport)
**Token**: stored in `.env` as `BRIGHTDATA_API_KEY`
**Free tier**: 5,000 requests/month

### Available Tools
| Tool | Use case |
|------|----------|
| `scrape_as_markdown` | Fetch any URL and return clean markdown — bypasses JS, paywalls, bot detection |
| `search_engine` | Web search returning structured results |
| `scrape_batch` | Scrape multiple URLs in one call |
| `search_engine_batch` | Batch web search |

### When to use Brightdata MCP
- A scraper returns 0 results or throws an error
- A site requires JavaScript rendering (Fortune, Forbes, TradingView)
- You need to find an alternative data source for a broken scraper
- You need to inspect live page HTML/structure to debug a parser
- Finding FTSE 350, or any index not on Wikipedia

### When NOT to use Brightdata MCP (save quota)
- Any index with a working Wikipedia scraper — use `scrape_wikipedia()` instead
- Any index with a working iShares CSV URL — use `scrape_ishares()` instead
- Nasdaq Composite — the screener API works fine

### Example: Fix Fortune 500 scraper
```
# 1. Inspect the live page
scrape_as_markdown("https://fortune.com/ranking/fortune500/")
# → parse the markdown for company names/ranks, update scrape_fortune() parser

# 2. If the franchise API is down, find alternatives
search_engine("Fortune 500 2025 full list CSV download")
```

### Example: Find FTSE 350 source
```
search_engine("FTSE 350 constituents complete list 2025 site:ftserussell.com OR site:londonstockexchange.com")
# → scrape_as_markdown(best_result_url)
```

---

## Adding a New Index

1. Add entry to `INDICES_CONFIG` in `scrapers.py`
2. Choose source: `wikipedia` (preferred), `ishares`, or add a new scraper function
3. For manual/gated sources: set `'manual': True` in config and use CSV upload in the UI
4. Test locally with `python app.py`, click Refresh in the UI

## Deployment Notes

- **Playwright on Render** — works; requires `--no-sandbox --disable-dev-shm-usage --disable-gpu` launch args (already set in all scrapers). Build command installs chromium via `playwright install chromium --with-deps`
- `enrich_urls_with_kernel()` only runs locally (Kernel CLI not on Render)
- `data/` directory is ephemeral on Render — data resets on each deploy
