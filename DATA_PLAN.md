# Index Tracker — Data Sources Plan

This document covers all 32 indices: what data source is used, what fields are available, current status, and what's needed to close remaining gaps.

**Fields collected per company**: `name`, `ticker`, `country`, `url`

---

## Status Legend

| Symbol | Meaning |
|--------|---------|
| ✅ | Working, exact index data |
| 🟡 | Working, but proxy index (similar but not identical) |
| ⚠️ | Working, known limitation |
| ❌ | Broken / no data |

---

## Indices by Source

### Wikipedia — `scrape_wikipedia()` (21 indices)

Reads constituent tables directly from Wikipedia. Reliable for most indices. All working as of April 2026.

| Index | Key | Companies | Ticker | Country | Notes |
|-------|-----|-----------|--------|---------|-------|
| ✅ OMXC25 | `omcx_25` | 25 | ✅ | Denmark (default) | |
| ✅ OMXH25 | `omxh_25` | 25 | ✅ | Finland (default) | |
| ✅ OBX | `obx` | 25 | ✅ | Norway (default) | |
| ✅ TA-35 | `ta_35` | 35 | ✅ | Per row | Multi-row header fixed |
| ✅ SMI | `smi` | 20 | ✅ | Switzerland (default) | |
| ✅ AEX | `aex` | 25 | ✅ | Netherlands (default) | |
| ✅ BEL 20 | `bel_20` | 20 | ✅ | Belgium (default) | Tickers normalised from `Euronext:TICK` format |
| ✅ OMX Stockholm 30 | `omx_stockholm_30` | 30 | ✅ | Sweden (default) | |
| ✅ IBEX 35 | `ibex_35` | 35 | ✅ | Spain (default) | |
| ✅ NIFTY 50 | `nifty_50` | 50 | ✅ | India (default) | |
| ✅ Euro STOXX 50 | `euro_stoxx_50` | 50 | ✅ | Per row | Country from "Registered office" column |
| ✅ Dow Jones | `dji` | 30 | ✅ | United States (default) | |
| ✅ CAC 40 | `cac_40` | 40 | ✅ | France (default) | |
| ✅ CAC Next 20 | `cac_next_20` | 20 | ✅ | France (default) | |
| ✅ Nasdaq 100 | `nasdaq_100` | 101 | ✅ | United States (default) | Wikipedia lists 101 (includes 1 non-US) |
| ✅ DAX | `dax` | 40 | ✅ | Germany (default) | |
| ✅ S&P 500 | `s_p_500` | 503 | ✅ | United States (default) | Wikipedia lists 503 (multi-class shares) |
| ✅ MDAX | `mdax` | 50 | ✅ | Germany (default) | |
| ✅ SDAX | `sdax` | ~69 | ⚠️ | Germany (default) | Wikipedia has ~69; some tickers missing |
| ✅ FTSE 100 | `ftse_100` | 100 | ✅ | United Kingdom (default) | |
| ✅ FTSE 250 | `ftse_250` | 250 | ✅ | United Kingdom (default) | |

**To fix SDAX missing tickers**: a few companies link to Wikipedia pages where the "Traded as" section only lists the index name (SDAX), not a ticker code. Accept as-is or look up tickers manually.

---

### Wikipedia Navbox — `scrape_wikipedia_navbox()` (2 indices)

These indices don't have a constituent wikitable. The constituent list is in a navigation box (navbox) at the bottom of the page. The scraper fetches each company's Wikipedia page individually to find the ticker.

⚠️ **Performance warning**: These scrapers make one HTTP request per company (30 for TecDAX, 225 for Nikkei 225). Expect ~30s and ~3–4 minutes respectively.

| Index | Key | Companies | Ticker coverage | Country | Notes |
|-------|-----|-----------|-----------------|---------|-------|
| ✅ TecDAX | `tecdax` | 30 | 24/30 (80%) | Germany (default) | 6 companies' Wikipedia pages don't list a ticker |
| ✅ Nikkei 225 | `nikkei_225` | 225 | ~70% | Japan (default) | Many Japanese companies list only exchange codes; some pages lack ticker data |

**TecDAX missing tickers** (6): Atoss, Cancom, CompuGroup Medical, Energiekontor, MorphoSys, Nagarro. Their Wikipedia "Traded as" sections only name the index (SDAX/MDAX), not the ticker code.

**To improve coverage**: Look up these tickers from Deutsche Börse or STOXX directly and hard-code them in `INDICES_CONFIG` as a `ticker_overrides` dict, or use Brightdata to scrape a structured source like stockanalysis.com or Deutsche Börse.

---

### Derived — combines sub-indices (1 index)

| Index | Key | Companies | Source | Notes |
|-------|-----|-----------|--------|-------|
| ✅ FTSE 350 | `ftse_350` | 350 | FTSE 100 + FTSE 250 | No dedicated Wikipedia page; mathematically exact |

---

### iShares ETF CSV — `scrape_ishares()` (4 indices)

iShares ETF holdings CSVs are used as a proxy for large global indices. The original iShares UK URLs all returned 404 (BlackRock changed their CDN). Now using iShares US ETF equivalents.

Country is per company from the iShares "Location" column (country of domicile).

| Index | Key | Source ETF | Companies | Exact? | Notes |
|-------|-----|------------|-----------|--------|-------|
| ✅ MSCI World | `msci_world` | URTH (iShares MSCI World ETF) | ~1,320 | ✅ Exact | Tracks the same MSCI World index |
| 🟡 STOXX Europe 600 | `stoxx_eu_600` | IEUR (iShares Core MSCI Europe ETF) | ~1,015 | ❌ Proxy | MSCI Europe ≠ STOXX 600: different methodology, ~440 large/mid vs 600; some overlap with small-cap |
| 🟡 STOXX Global 1800 | `stoxx_global_1800` | ACWI (iShares MSCI ACWI ETF) | ~2,272 | ❌ Proxy | MSCI ACWI includes emerging markets; STOXX 1800 does not. Broader universe |
| 🟡 S&P Global 1200 | `s_p_global_1200` | URTH (iShares MSCI World ETF) | ~1,320 | ❌ Proxy | S&P Global 1200 = top 1,200 global large-caps; MSCI World is ~95% overlap |

**To get exact data for STOXX 600 / STOXX 1800**: The STOXX website (stoxx.com) and the original iShares UK ETF pages require JavaScript. Use Brightdata MCP (`scrape_as_markdown`) to access the iShares UK product pages and find the current CSV download URL.

```
# In a new Claude Code session (Brightdata loads at session start):
scrape_as_markdown("https://www.ishares.com/uk/individual/en/products/273134/")
# Look for the CSV download link → update INDICES_CONFIG url
```

**S&P Global 1200 alternative**: The S&P website has a constituent list but requires registration. A better proxy might be iShares S&P Global 100 ETF (IOO) for top 100, or combine IVV (S&P 500) + IEUR + other regional ETFs.

---

### Nasdaq Screener API — `scrape_nasdaq()` (1 index)

| Index | Key | Companies | Ticker | Country | Notes |
|-------|-----|-----------|--------|---------|-------|
| ✅ Nasdaq Composite | `nasdaq_composite` | ~4,000 | ✅ | Per row | Full universe of Nasdaq-listed stocks. Returns JSON (not CSV). |

---

### Fortune Franchise API — `scrape_fortune()` (2 indices)

| Index | Key | Companies | Ticker | Country | Notes |
|-------|-----|-----------|--------|---------|-------|
| ✅ Fortune 500 | `fortune_500` | 500 | ❌ None | United States (default) | Fortune API doesn't include tickers |
| ⚠️ Fortune 1000 | `fortune_1000` | 500 | ❌ None | United States (default) | **API hard cap at 500**; ranks 501–1000 not available via API |

**Fortune 1000 gap**: The Fortune franchise API returns HTTP 500 for `count > 500`. Fortune's public-facing fortune1000 page (fortune.com/ranking/fortune1000) returns 404 — the page may be behind a paywall or login. To get the full 1000:

- **Option A**: Use Playwright (local only, not on Render) which can load the interactive ranking page
- **Option B**: Use Brightdata `scrape_as_markdown("https://fortune.com/ranking/fortune1000/")` — may bypass the paywall
- **Option C**: Accept 500 companies and rename this index to "Fortune 500" in the UI

---

### Forbes API — `scrape_forbes()` (1 index)

| Index | Key | Companies | Ticker | Country | Notes |
|-------|-----|-----------|--------|---------|-------|
| ✅ Forbes Global 2000 | `forbes_2000` | 2,000 | ❌ None | Per row | Uses 2025 list (2026 not published until May/June) |

Forbes publishes the Global 2000 in May/June annually. The scraper automatically falls back to the previous year's list when the current year isn't available yet.

---

## Summary Table

| Status | Count | Indices |
|--------|-------|---------|
| ✅ Exact data, working | 27 | All Wikipedia, Nasdaq, Fortune 500, Forbes, MSCI World, FTSE 350 |
| 🟡 Proxy data, working | 3 | STOXX EU 600, STOXX Global 1800, S&P Global 1200 |
| ⚠️ Partial data | 2 | Fortune 1000 (500/1000), TecDAX (24/30 tickers) |
| ❌ Not working | 0 | — |

---

## Remaining Work

### Priority 1 — Fix proxy indices (requires Brightdata)

Find the current iShares UK CSV download URLs for EXSA, ISWD, ISSP. These return 404 due to a BlackRock URL change. Brightdata's `scrape_as_markdown` can bypass their JS to find the new download link.

```python
# Open a new Claude Code session (Brightdata tools load at session start)
# Then use:
scrape_as_markdown("https://www.ishares.com/uk/individual/en/products/273134/")
# → find CSV download URL → update scrapers.py INDICES_CONFIG
```

### Priority 2 — Fortune 1000 (ranks 501–1000)

Use Brightdata or Playwright to access the full Fortune 1000 list. The Fortune franchise API hard-caps at 500.

### Priority 3 — Add tickers to Forbes / Fortune

Neither Forbes nor Fortune include ticker symbols in their APIs. Options:
- Match company names to a ticker database (e.g. Nasdaq screener or yfinance)
- Use Brightdata to scrape each company's page for ticker info (expensive: 500–2000 requests)
- Accept no tickers for these lists (they're primarily revenue/market-cap rankings, not equity indices)

### Priority 4 — Nikkei 225 ticker coverage

~30% of Nikkei 225 companies have missing or incomplete ticker data on their Wikipedia pages. Japanese companies use 4-digit codes (e.g. 7203 for Toyota). Consider using the official JPX/TSE data or a structured financial data API.

---

## Field Coverage by Index

| Index | Name | Ticker | Country | URL |
|-------|------|--------|---------|-----|
| Wikipedia indices | ✅ | ✅ (mostly) | ✅ or default | ✅ Wikipedia link |
| TecDAX / Nikkei 225 | ✅ | ⚠️ ~70–80% | ✅ default | ✅ Wikipedia link |
| iShares indices | ✅ | ✅ (exchange ticker) | ✅ per company | ❌ empty |
| Nasdaq Composite | ✅ | ✅ | ✅ per company | ❌ empty |
| Fortune 500/1000 | ✅ | ❌ | ✅ default US | ✅ Forbes URL |
| Forbes Global 2000 | ✅ | ❌ | ✅ per company | ✅ Forbes URL |
