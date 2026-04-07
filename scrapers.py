"""
scrapers.py — Index composition scrapers for each data source.

Supported sources:
  - wikipedia    : requests + BeautifulSoup table parsing
  - ishares      : iShares ETF holdings CSV (static download, no JS needed)
  - nasdaq       : Nasdaq stock screener CSV API (Nasdaq Composite)
  - marketscreener: requests + BeautifulSoup with pagination (fallback)
  - euronext     : requests + BeautifulSoup (fallback)
  - fortune      : Fortune.com franchise API + Playwright fallback
  - forbes       : Forbes.com forbesapi + Playwright fallback
"""

import requests
from bs4 import BeautifulSoup
import time
import re
import subprocess
import json
import os
from typing import List, Optional

# ---------------------------------------------------------------------------
# Index Configuration
# ---------------------------------------------------------------------------

INDICES_CONFIG = {
    'omcx_25': {
        'name': 'OMXC25',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/OMX_Copenhagen_25',
        'country_default': 'Denmark',
        'expected_count': 25,
        'data_notes': 'Wikipedia. May lag rebalancing by days.',
        'rebalance_schedule': 'Semi-annual (Jan/Jul)',
        'stale_days': 200,
    },
    'omxh_25': {
        'name': 'OMXH25',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/OMX_Helsinki_25',
        'country_default': 'Finland',
        'expected_count': 25,
        'data_notes': 'Wikipedia. May lag rebalancing by days.',
        'rebalance_schedule': 'Semi-annual (Jan/Jul)',
        'stale_days': 200,
    },
    'obx': {
        'name': 'OBX',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/OBX_Index',
        'country_default': 'Norway',
        'expected_count': 25,
        'data_notes': 'Wikipedia. May lag rebalancing by days.',
        'rebalance_schedule': 'Semi-annual (Jun/Dec)',
        'stale_days': 200,
    },
    'ta_35': {
        'name': 'TA-35',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/TA-35_Index',
        'country_default': 'Israel',
        'expected_count': 35,
        'data_notes': 'Wikipedia. May lag rebalancing by days.',
        'rebalance_schedule': 'Monthly review',
        'stale_days': 45,
    },
    'smi': {
        'name': 'SMI',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/Swiss_Market_Index',
        'country_default': 'Switzerland',
        'expected_count': 20,
        'data_notes': 'Wikipedia. May lag rebalancing by days.',
        'rebalance_schedule': 'Annual (Sep)',
        'stale_days': 400,
    },
    'aex': {
        'name': 'AEX',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/AEX_index',
        'table_index': 2,
        'country_default': 'Netherlands',
        'expected_count': 25,
        'data_notes': 'Wikipedia. May lag rebalancing by days.',
        'rebalance_schedule': 'Annual (Mar)',
        'stale_days': 400,
    },
    'bel_20': {
        'name': 'BEL 20',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/BEL20',
        'table_index': 1,
        'country_default': 'Belgium',
        'expected_count': 20,
        'data_notes': 'Wikipedia. May lag rebalancing by days.',
        'rebalance_schedule': 'Annual',
        'stale_days': 400,
        'alt_source': 'investing',
        'alt_url': 'https://www.investing.com/indices/bel-20-components',
    },
    'omx_stockholm_30': {
        'name': 'OMX Stockholm 30',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/OMX_Stockholm_30',
        'country_default': 'Sweden',
        'expected_count': 30,
        'data_notes': 'Wikipedia. May lag rebalancing by days.',
        'rebalance_schedule': 'Semi-annual (Jan/Jul)',
        'stale_days': 200,
        'alt_source': 'investing',
        'alt_url': 'https://www.investing.com/indices/omx-stockholm-30-components',
    },
    'ibex_35': {
        'name': 'IBEX 35',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/IBEX_35',
        'table_index': 1,
        'country_default': 'Spain',
        'expected_count': 35,
        'data_notes': 'Wikipedia. May lag rebalancing by days.',
        'rebalance_schedule': 'Semi-annual (Jan/Jul)',
        'stale_days': 200,
        'alt_source': 'investing',
        'alt_url': 'https://www.investing.com/indices/spain-35-components',
    },
    'nifty_50': {
        'name': 'NIFTY 50',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/NIFTY_50',
        'country_default': 'India',
        'expected_count': 50,
        'data_notes': 'Wikipedia. May lag rebalancing by days.',
        'rebalance_schedule': 'Semi-annual (Mar/Sep)',
        'stale_days': 200,
    },
    'nikkei_225': {
        'name': 'Nikkei 225',
        'source': 'wikipedia_navbox',
        'url': 'https://en.wikipedia.org/wiki/Nikkei_225',
        'country_default': 'Japan',
        'expected_count': 225,
        'data_notes': 'Wikipedia navbox. May lag rebalancing by days.',
        'rebalance_schedule': 'Annual (Oct)',
        'stale_days': 400,
        'alt_source': 'investing',
        'alt_url': 'https://www.investing.com/indices/japan-ni225-components',
    },
    'euro_stoxx_50': {
        'name': 'Euro STOXX 50',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/Euro_Stoxx_50',
        'table_index': 2,
        'expected_count': 50,
        'data_notes': 'Wikipedia. May lag rebalancing by days.',
        'rebalance_schedule': 'Quarterly (Mar/Jun/Sep/Dec)',
        'stale_days': 100,
        'alt_source': 'investing',
        'alt_url': 'https://www.investing.com/indices/eu-stoxx50-components',
    },
    'dji': {
        'name': 'Dow Jones Industrial Average',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average',
        'table_index': 0,
        'country_default': 'United States',
        'expected_count': 30,
        'data_notes': 'Wikipedia. May lag rebalancing by days.',
        'rebalance_schedule': 'Ad hoc',
        'stale_days': 400,
        'alt_source': 'investing',
        'alt_url': 'https://www.investing.com/indices/us-30-components',
    },
    'stoxx_eu_600': {
        'name': 'STOXX Europe 600',
        'source': 'ishares',
        # iShares STOXX Europe 600 UCITS ETF (EXSA) — real STOXX Europe 600 ETF via iShares CH
        'url': 'https://www.ishares.com/ch/individual/en/products/251931/ishares-stoxx-europe-600-ucits-etf-de-fund/1495092304805.ajax?fileType=csv&fileName=EXSA_holdings&dataType=fund',
        'expected_count': 600,
        'data_notes': 'iShares EXSA ETF (real STOXX ETF). Updated daily by BlackRock. High confidence.',
        'rebalance_schedule': 'Quarterly (Mar/Jun/Sep/Dec)',
        'stale_days': 100,
    },
    'stoxx_global_1800': {
        'name': 'STOXX Global 1800',
        'source': 'ishares',
        # iShares MSCI ACWI ETF — best available proxy for STOXX Global 1800
        # UK iShares URL (ISWD) no longer accessible; MSCI ACWI covers same global developed + EM universe
        'url': 'https://www.ishares.com/us/products/239600/ishares-msci-acwi-etf/1467271812596.ajax?fileType=csv&fileName=ACWI_holdings&dataType=fund',
        'expected_count': 1800,
        'data_notes': '\u26a0 PROXY: Uses iShares MSCI ACWI ETF (~2,270 holdings). No iShares STOXX Global 1800 ETF exists. MSCI ACWI covers the same universe (developed + EM) but is a different index. Count will exceed 1,800. Aug 2025 snapshot (exact 1,800 constituents) stored in data/snapshots/ for QA reference.',
        'rebalance_schedule': 'Quarterly (Mar/Jun/Sep/Dec)',
        'stale_days': 100,
    },
    'msci_world': {
        'name': 'MSCI World',
        'source': 'ishares',
        # iShares MSCI World ETF (URTH) — exact MSCI World index
        # UK iShares URL (SWRD) no longer accessible; using iShares US URTH which tracks same index
        'url': 'https://www.ishares.com/us/products/239696/ISHARES-MSCI-WORLD-ETF/1467271812596.ajax?fileType=csv&fileName=URTH_holdings&dataType=fund',
        'expected_count': 1400,
        'data_notes': 'iShares URTH ETF (exact MSCI World). Updated daily. High confidence. ~1,400 developed-market large/mid-cap companies.',
        'rebalance_schedule': 'Quarterly (Feb/May/Aug/Nov)',
        'stale_days': 100,
    },
    's_p_global_1200': {
        'name': 'S&P Global 1200',
        'source': 'ishares',
        # iShares MSCI World ETF (URTH) — closest available proxy for S&P Global 1200
        # UK iShares URL (ISSP) no longer accessible; MSCI World overlaps ~95% with S&P Global 1200
        'url': 'https://www.ishares.com/us/products/239696/ISHARES-MSCI-WORLD-ETF/1467271812596.ajax?fileType=csv&fileName=URTH_holdings&dataType=fund',
        'expected_count': 1200,
        'data_notes': '\u26a0 PROXY: Uses iShares MSCI World ETF. No accessible iShares S&P Global 1200 CSV. MSCI World overlaps ~95% with S&P Global 1200 but omits some EM names. Aug 2025 snapshot (exact 1,200 constituents) stored in data/snapshots/ for QA reference.',
        'rebalance_schedule': 'Quarterly (Mar/Jun/Sep/Dec)',
        'stale_days': 100,
    },
    'cac_40': {
        'name': 'CAC 40',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/CAC_40',
        'table_index': 3,
        'country_default': 'France',
        'expected_count': 40,
        'data_notes': 'Wikipedia. May lag rebalancing by days.',
        'rebalance_schedule': 'Quarterly (Mar/Jun/Sep/Dec)',
        'stale_days': 100,
        'alt_source': 'investing',
        'alt_url': 'https://www.investing.com/indices/france-40-components',
    },
    'cac_next_20': {
        'name': 'CAC Next 20',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/CAC_Next_20',
        'country_default': 'France',
        'expected_count': 20,
        'data_notes': 'Wikipedia. May lag rebalancing by days.',
        'rebalance_schedule': 'Quarterly (Mar/Jun/Sep/Dec)',
        'stale_days': 100,
        'alt_source': 'investing',
        'alt_url': 'https://www.investing.com/indices/cac-next-20-components',
    },
    'nasdaq_100': {
        'name': 'Nasdaq 100',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/Nasdaq-100',
        'table_index': 3,
        'country_default': 'United States',
        'expected_count': 100,
        'data_notes': 'Wikipedia. May lag rebalancing by days.',
        'rebalance_schedule': 'Annual (Dec) + quarterly review',
        'stale_days': 100,
        'alt_source': 'investing',
        'alt_url': 'https://www.investing.com/indices/nq-100-components',
    },
    'dax': {
        'name': 'DAX',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/DAX',
        'table_index': 3,
        'country_default': 'Germany',
        'expected_count': 40,
        'data_notes': 'Wikipedia. May lag rebalancing by days.',
        'rebalance_schedule': 'Quarterly (Mar/Jun/Sep/Dec)',
        'stale_days': 100,
    },
    'fortune_500': {
        'name': 'Fortune 500',
        'source': 'fortune',
        'url': 'https://fortune.com/ranking/fortune500/',
        'limit': 500,
        'country_default': 'United States',
        'expected_count': 500,
        'data_notes': 'us500.com via Playwright (Fortune 500, Walmart #1, official 2025 list).',
        'rebalance_schedule': 'Annual (May)',
        'stale_days': 400,
    },
    'fortune_1000': {
        'name': 'Fortune 1000',
        'source': 'fortune',
        'url': 'https://fortune.com/ranking/fortune500/',
        'limit': 1000,
        'country_default': 'United States',
        'expected_count': 1000,
        'data_notes': 'us500.com via Playwright (all 1,000 companies). Walmart #1 (official 2025 list, FY2024 revenue). 4 rank numbers skipped due to Fortune ties (498×2, 665×2, 667×2, 759×2).',
        'rebalance_schedule': 'Annual (May)',
        'stale_days': 400,
    },
    's_p_500': {
        'name': 'S&P 500',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
        'table_index': 0,
        'country_default': 'United States',
        'expected_count': 500,
        'data_notes': 'Wikipedia. May lag S&P rebalancing. Note: dual-class shares (Alphabet, Fox) are deduplicated to one entry each.',
        'rebalance_schedule': 'Quarterly (Mar/Jun/Sep/Dec)',
        'stale_days': 100,
    },
    'forbes_2000': {
        'name': 'Forbes Global 2000',
        'source': 'forbes',
        'url': 'https://www.forbes.com/lists/global2000/',
        'expected_count': 2000,
        'data_notes': 'Forbes JSON API. Year-sensitive \u2014 published annually ~May/June. Hardcoded to 2025 list.',
        'rebalance_schedule': 'Annual (May/Jun)',
        'stale_days': 400,
    },
    'tecdax': {
        'name': 'TecDAX',
        'source': 'investing',
        'url': 'https://www.investing.com/indices/tecdax-components',
        'country_default': 'Germany',
        'expected_count': 30,
        'data_notes': 'investing.com components page. More current than Wikipedia. ⚠ QIAGEN (Dutch/US dual-listed) missing from investing.com data — actual TecDAX has 30 members.',
        'rebalance_schedule': 'Quarterly (Mar/Jun/Sep/Dec)',
        'stale_days': 100,
    },
    'ftse_100': {
        'name': 'FTSE 100',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/FTSE_100',
        'table_index': 3,
        'country_default': 'United Kingdom',
        'expected_count': 100,
        'data_notes': 'Wikipedia. May lag FTSE Russell rebalancing by days.',
        'rebalance_schedule': 'Quarterly (Mar/Jun/Sep/Dec)',
        'stale_days': 100,
    },
    'ftse_250': {
        'name': 'FTSE 250',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/FTSE_250_Index',
        'table_index': 2,
        'country_default': 'United Kingdom',
        'expected_count': 250,
        'data_notes': 'Wikipedia. May lag FTSE Russell rebalancing by days.',
        'rebalance_schedule': 'Quarterly (Mar/Jun/Sep/Dec)',
        'stale_days': 100,
    },
    'ftse_350': {
        'name': 'FTSE 350',
        # FTSE 350 = FTSE 100 + FTSE 250; no dedicated Wikipedia constituent table exists.
        # The 'derived' source combines the two sub-indices automatically.
        'source': 'derived',
        'components': ['ftse_100', 'ftse_250'],
        'expected_count': 350,
        'data_notes': 'Derived: FTSE 100 + FTSE 250 combined. Inherits Wikipedia limitations of both.',
        'rebalance_schedule': 'Quarterly (Mar/Jun/Sep/Dec)',
        'stale_days': 100,
    },
    'mdax': {
        'name': 'MDAX',
        'source': 'investing',
        'url': 'https://www.investing.com/indices/mdaxi-components',
        'country_default': 'Germany',
        'expected_count': 50,
        'data_notes': 'investing.com components page. More current than Wikipedia.',
        'rebalance_schedule': 'Quarterly (Mar/Jun/Sep/Dec)',
        'stale_days': 100,
    },
    'sdax': {
        'name': 'SDAX',
        'source': 'investing',
        'url': 'https://www.investing.com/indices/sdaxi-components',
        'country_default': 'Germany',
        'expected_count': 70,
        'data_notes': 'investing.com components page. More current than Wikipedia.',
        'rebalance_schedule': 'Quarterly (Mar/Jun/Sep/Dec)',
        'stale_days': 100,
    },
    'nasdaq_composite': {
        'name': 'Nasdaq Composite',
        'source': 'nasdaq',
        'url': 'https://api.nasdaq.com/api/screener/stocks?tableonly=true&exchange=nasdaq&download=true',
        'country_default': 'United States',
        'expected_count': 3300,
        'data_notes': 'Official Nasdaq screener API. Returns all Nasdaq-listed stocks (~3,300\u20133,500). High confidence. Count varies as companies list/delist.',
        'rebalance_schedule': 'Continuous (daily)',
        'stale_days': 14,
    },
}


# ---------------------------------------------------------------------------
# URL enrichment via Kernel CLI (local only, silently skipped on Render)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Post-processing: deduplication, URL sanitization, ticker cleanup
# ---------------------------------------------------------------------------

# URLs from these domains are source sites, not company websites
_SOURCE_DOMAINS = frozenset({
    'en.wikipedia.org', 'wikipedia.org',
    'fortune.com',
    'forbes.com',
    'marketscreener.com',
    'ishares.com',
})


def _is_source_url(url: str) -> bool:
    """Return True if url belongs to a scraping-source domain (not a company website)."""
    if not url:
        return False
    try:
        from urllib.parse import urlparse
        host = (urlparse(url).hostname or '').lstrip('www.')
        return any(host == d or host.endswith('.' + d) for d in _SOURCE_DOMAINS)
    except Exception:
        return False


def _dedup_key(name: str) -> str:
    """
    Normalise a company name to a deduplication key.
    Strips share-class suffixes so that e.g. 'Alphabet Inc.' (Class A) and
    'Alphabet Inc.' (Class C) collapse to the same key.
    """
    key = name.lower().strip()
    # Strip parenthesised class descriptors: "(Class A)", "(Class C)" etc.
    key = re.sub(r'\s*\([^)]*\)\s*$', '', key)
    # Strip trailing share-class words
    key = re.sub(
        r'\s+(?:class\s+[a-d]|cl\.?\s*[a-d]|series\s+[a-d]|[a-d]\s+share[s]?'
        r'|preferred?|ordinary|voting|non[\s\-]?voting)$',
        '', key, flags=re.IGNORECASE,
    )
    # Remove punctuation for comparison (keep letters, digits, space)
    key = re.sub(r'[^\w\s]', ' ', key)
    # Collapse whitespace
    key = re.sub(r'\s+', ' ', key).strip()
    return key


def _clean_ticker(ticker: str) -> str:
    """Strip exchange prefix from ticker (e.g. 'NYSE:AAPL' → 'AAPL')."""
    if ticker and ':' in ticker:
        ticker = ticker.split(':')[-1]
    return ticker.strip()


def _post_process(companies: List[dict]) -> List[dict]:
    """
    Post-process a raw list of scraped companies:
      1. Clear URLs that are scraping-source domains (not the company's own site).
      2. Clean ticker formats (strip exchange prefix).
      3. Deduplicate by normalised company name — keeps first occurrence (lowest rank).
    """
    seen: dict = {}   # dedup_key → index in result
    result: List[dict] = []

    for c in companies:
        # 1. URL sanitation — drop source-site URLs
        url = c.get('url') or ''
        cleaned_url = '' if _is_source_url(url) else url

        # 2. Ticker cleanup
        cleaned_ticker = _clean_ticker(c.get('ticker') or '')

        # Build cleaned copy only if something changed (avoid unnecessary dict copies)
        if cleaned_url != url or cleaned_ticker != (c.get('ticker') or ''):
            c = {**c, 'url': cleaned_url, 'ticker': cleaned_ticker}

        # 3. Deduplication
        key = _dedup_key(c.get('name') or '')
        if key and key not in seen:
            seen[key] = len(result)
            result.append(c)
        # Else: duplicate — silently drop

    return result


_KERNEL_BIN = os.path.expanduser('~/.local/bin/kernel')


def _kernel_available() -> bool:
    return os.path.isfile(_KERNEL_BIN)


def enrich_urls_with_kernel(companies: List[dict]) -> List[dict]:
    """
    Use the Kernel identity lookup CLI to infer company website URLs.

    Only runs when the kernel CLI is available (i.e., on local dev machines).
    Silently skips on Render where the CLI isn't installed.

    Sends companies in batches of 50 to avoid CLI limits.
    Populates company['url'] where Kernel returns a match.
    """
    if not _kernel_available():
        return companies

    # Only enrich companies that don't already have a website URL
    needs_url = [
        (i, c) for i, c in enumerate(companies)
        if not (c.get('url') or '').startswith('http')
        or 'wikipedia.org' in (c.get('url') or '')
    ]

    if not needs_url:
        return companies

    batch_size = 50
    for batch_start in range(0, len(needs_url), batch_size):
        batch = needs_url[batch_start:batch_start + batch_size]
        lookup_input = [
            {'name': companies[i]['name'], 'url': ''}
            for i, _ in batch
        ]
        try:
            result = subprocess.run(
                [_KERNEL_BIN, 'identity', 'lookup', '--json',
                 json.dumps(lookup_input)],
                capture_output=True, text=True, timeout=120,
            )
            if result.returncode == 0 and result.stdout.strip():
                matches = json.loads(result.stdout)
                for j, (orig_idx, _) in enumerate(batch):
                    if j < len(matches) and matches[j]:
                        url = (matches[j].get('account_url') or
                               matches[j].get('url') or
                               matches[j].get('website') or '')
                        if url:
                            companies[orig_idx]['url'] = url
        except Exception:
            pass  # Kernel unavailable or failed — silently continue

    return companies


# ---------------------------------------------------------------------------
# Wikipedia scraper
# ---------------------------------------------------------------------------

def scrape_wikipedia(
    url: str,
    table_index: int = 0,
    country_default: str = '',
) -> List[dict]:
    """
    Scrape index components from a Wikipedia article.

    Strategy:
    1. Collect all wikitables on the page.
    2. Score each table: prefer tables whose headers mention 'company',
       'constituent', 'ticker', 'symbol', 'stock', 'name', 'issuer'.
    3. Parse the best-scored table with ≥3 data rows.
    4. Detect ticker column and (if present) country column automatically.
    5. Apply country_default as fallback when no country column exists.

    Returns a list of dicts: {rank, name, ticker, url, country}
    """
    headers = {
        'User-Agent': (
            'Mozilla/5.0 (compatible; IndexTracker/1.0; '
            '+https://data-catalogue.onrender.com)'
        )
    }
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, 'lxml')
    tables = soup.find_all('table', class_=re.compile(r'wikitable'))

    if not tables:
        raise Exception(f'No wikitable found on Wikipedia page: {url}')

    COMPANY_KW = {'company', 'constituent', 'name', 'stock', 'security',
                  'issuer', 'member', 'company name', 'corp'}
    TICKER_KW  = {'ticker', 'symbol', 'code', 'abbr'}
    COUNTRY_KW = {'country', 'nation', 'nationality', 'domicile',
                  'incorporated', 'registered'}

    def score_table(table) -> int:
        rows = table.find_all('tr')
        if len(rows) < 3:
            return 0
        header_cells = rows[0].find_all(['th', 'td'])
        headers_text = [c.get_text(strip=True).lower() for c in header_cells]
        score = len(rows)
        for h in headers_text:
            if any(kw in h for kw in COMPANY_KW):
                score += 200
        return score

    def parse_table(table, default_country: str) -> List[dict]:
        rows = table.find_all('tr')
        if len(rows) < 2:
            return []

        # Skip spanning title rows (e.g. TA-35: row[0] is "TA-35 Components"
        # spanning all columns; actual column headers are in row[1])
        header_row_idx = 0
        first_cells = rows[0].find_all(['th', 'td'])
        if (len(first_cells) == 1 and
                first_cells[0].get('colspan') and
                len(rows) > 2):
            header_row_idx = 1

        header_cells = rows[header_row_idx].find_all(['th', 'td'])
        headers_text = [c.get_text(strip=True).lower() for c in header_cells]

        # Detect column positions
        name_col = next(
            (i for i, h in enumerate(headers_text)
             if any(kw in h for kw in COMPANY_KW)),
            0,
        )
        ticker_col = next(
            (i for i, h in enumerate(headers_text)
             if any(kw in h for kw in TICKER_KW)),
            None,
        )
        country_col = next(
            (i for i, h in enumerate(headers_text)
             if any(kw in h for kw in COUNTRY_KW)),
            None,
        )

        companies = []
        for rank, row in enumerate(rows[header_row_idx + 1:], start=1):
            cells = row.find_all(['td', 'th'])
            if len(cells) <= name_col:
                continue

            name_cell = cells[name_col]
            link = name_cell.find('a')
            name = (link.get_text(strip=True) if link
                    else name_cell.get_text(strip=True))

            # Skip rows that look like sub-headers
            if not name or name.lower() in COMPANY_KW:
                continue

            ticker = ''
            if ticker_col is not None and ticker_col < len(cells):
                ticker = cells[ticker_col].get_text(strip=True)
                # Normalize "Exchange:TICK" → "TICK" (common on European Wikipedia pages)
                if ':' in ticker:
                    ticker = ticker.split(':')[-1]

            country = default_country
            if country_col is not None and country_col < len(cells):
                country = cells[country_col].get_text(strip=True) or default_country

            # Extract Wikipedia article URL if available
            wiki_url = ''
            if link and link.get('href', '').startswith('/wiki/'):
                wiki_url = 'https://en.wikipedia.org' + link['href']

            companies.append({
                'rank': rank,
                'name': name,
                'ticker': ticker,
                'url': wiki_url,
                'country': country,
            })

        return companies

    # Score tables; try the highest-scored tables first
    scored = sorted(
        enumerate(tables),
        key=lambda x: score_table(x[1]),
        reverse=True,
    )

    # Make sure we try table_index first when explicitly specified
    if table_index < len(tables):
        hint_result = parse_table(tables[table_index], country_default)
        if len(hint_result) >= 3:
            return hint_result

    for _, table in scored:
        result = parse_table(table, country_default)
        if len(result) >= 3:
            return result

    raise Exception(
        f'Could not find a valid company table on Wikipedia page: {url}. '
        f'Tried {len(tables)} tables.'
    )


# ---------------------------------------------------------------------------
# Wikipedia navbox scraper (for indices like TecDAX and Nikkei 225 where the
# constituent list is in a navigation box rather than a wikitable)
# ---------------------------------------------------------------------------

def _extract_ticker_from_wikipedia(wiki_url: str, session: requests.Session) -> str:
    """
    Fetch a company's Wikipedia page and extract its stock ticker from the infobox.
    Returns the ticker string, or '' if not found.
    """
    try:
        resp = session.get(wiki_url, timeout=15)
        resp.raise_for_status()
    except Exception:
        return ''

    soup = BeautifulSoup(resp.text, 'lxml')
    infobox = soup.find('table', class_=re.compile(r'\binfobox\b'))
    if not infobox:
        return ''

    TRADE_KW = {'traded', 'ticker', 'symbol', 'stock symbol', 'listed'}
    for row in infobox.find_all('tr'):
        header = row.find('th')
        cell = row.find('td')
        if not header or not cell:
            continue
        if not any(kw in header.get_text(strip=True).lower() for kw in TRADE_KW):
            continue

        # Prefer external links (exchange listing pages) — their text is the ticker
        ext_links = [
            a.get_text(strip=True)
            for a in cell.find_all('a')
            if a.get('href', '').startswith('http') and a.get_text(strip=True)
        ]
        if ext_links:
            return ext_links[0]

        # Look for EXCHANGE:TICKER pattern in the cell text
        text = cell.get_text(separator=' ', strip=True)
        m = re.search(r'\b[A-Z]{2,6}:\s*([A-Z0-9]{1,8})\b', text)
        if m:
            return m.group(1)

        # Fallback: if the cell text looks like a ticker code (short alphanum,
        # not a known index name), treat it as the ticker
        KNOWN_INDICES = {'DAX', 'MDAX', 'SDAX', 'TECDAX', 'ATX', 'FTSE', 'SMI',
                         'CAC', 'AEX', 'BEL', 'IBEX', 'DJIA', 'SP500', 'NYSE',
                         'NASDAQ', 'LSE', 'TSX', 'ASX', 'TOPIX'}
        plain = text.strip()
        if (plain
                and len(plain) <= 10
                and re.match(r'^[A-Za-z0-9&]+$', plain)
                and plain.upper() not in KNOWN_INDICES):
            return plain.upper()

    return ''


def scrape_wikipedia_navbox(
    url: str,
    country_default: str = '',
) -> List[dict]:
    """
    Scrape index constituents from a Wikipedia navbox (navigation table).

    Used for indices like TecDAX and Nikkei 225 where the constituent list
    is in a navbox rather than a wikitable. Fetches each company's Wikipedia
    page individually to obtain its stock ticker.

    Returns a list of dicts: {rank, name, ticker, url, country}
    """
    wiki_headers = {
        'User-Agent': (
            'Mozilla/5.0 (compatible; IndexTracker/1.0; '
            '+https://data-catalogue.onrender.com)'
        )
    }
    session = requests.Session()
    session.headers.update(wiki_headers)

    resp = session.get(url, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, 'lxml')

    # Find the navbox that lists constituents: look for a table whose text
    # contains many internal Wikipedia links to company articles.
    navbox = None
    for table in soup.find_all('table'):
        classes = ' '.join(table.get('class', []))
        if 'navbox' not in classes and 'collapsible' not in classes:
            continue
        links = [
            a for a in table.find_all('a')
            if a.get('href', '').startswith('/wiki/')
            and a.get_text(strip=True)
            and a.get_text(strip=True) not in ('v', 't', 'e')
        ]
        if len(links) >= 10:
            navbox = table
            break

    if navbox is None:
        raise Exception(
            f'Could not find a navbox with index constituents on: {url}'
        )

    # Extract unique company links from list cells only (skip title/header cells
    # which contain geography links like "Japan" or "Germany")
    list_cells = navbox.find_all(
        ['td', 'li'],
        class_=re.compile(r'navbox-list|hlist'),
    )
    link_elements = []
    if list_cells:
        for cell in list_cells:
            link_elements.extend(cell.find_all('a'))
    else:
        # Fallback: all links in the navbox
        link_elements = navbox.find_all('a')

    seen = set()
    entries = []
    for a in link_elements:
        href = a.get('href', '')
        name = a.get_text(strip=True)
        if (not href.startswith('/wiki/')
                or not name
                or name in ('v', 't', 'e')
                or href in seen):
            continue
        seen.add(href)
        entries.append((name, 'https://en.wikipedia.org' + href))

    if not entries:
        raise Exception(f'No company links found in navbox on: {url}')

    companies = []
    for rank, (name, wiki_url) in enumerate(entries, start=1):
        ticker = _extract_ticker_from_wikipedia(wiki_url, session)
        companies.append({
            'rank': rank,
            'name': name,
            'ticker': ticker,
            'url': wiki_url,
            'country': country_default,
        })
        time.sleep(0.3)  # be polite to Wikipedia

    return companies


# ---------------------------------------------------------------------------
# iShares ETF holdings CSV scraper
# ---------------------------------------------------------------------------

def scrape_ishares(url: str) -> List[dict]:
    """
    Fetch an iShares ETF holdings CSV and extract constituent companies.

    iShares CSV files have several metadata rows at the top before the
    actual data header row. We scan for the header row by looking for
    a line containing 'Name' or 'Issuer', then parse from there.

    Returns a list of dicts: {rank, name, ticker, url, country}
    """
    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/120.0.0.0 Safari/537.36'
        ),
        'Referer': 'https://www.ishares.com/',
        'Accept': 'text/csv,application/csv,text/plain,*/*',
    }

    try:
        resp = requests.get(url, headers=headers, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise Exception(f'iShares request failed: {e}')

    import csv as csv_module
    import io as io_module

    lines = resp.text.splitlines()

    # Find the header row — iShares CSVs have a few metadata lines first
    header_idx = None
    for i, line in enumerate(lines):
        if re.search(r'\bName\b', line) and re.search(
                r'\bTicker\b|\bAsset Class\b|\bISIN\b', line):
            header_idx = i
            break

    if header_idx is None:
        raise Exception(
            f'Could not find data header row in iShares CSV from {url}. '
            'The file format may have changed.'
        )

    reader = csv_module.DictReader(
        io_module.StringIO('\n'.join(lines[header_idx:]))
    )

    companies = []
    rank = 1
    skip_asset_classes = {
        'cash', 'money market', 'future', 'futures', 'forward',
        'option', 'swap', 'other', 'fx',
    }

    # Normalise fieldnames once
    fieldnames = reader.fieldnames or []
    field_lower = {f: f.lower().strip() for f in fieldnames}

    # Try to detect the country column (iShares uses various names)
    country_field = next(
        (f for f, fl in field_lower.items()
         if any(kw in fl for kw in
                ('country of incorporation', 'location of incorporation',
                 'location', 'country'))),
        None,
    )

    for row in reader:
        name = (row.get('Name') or row.get('Issuer') or '').strip()
        ticker = (row.get('Ticker') or '').strip()
        asset_class = (row.get('Asset Class') or '').strip().lower()
        country = ((row.get(country_field) or '') if country_field else '').strip()

        if not name or name in ('-', '', 'N/A'):
            continue
        if asset_class in skip_asset_classes:
            continue
        if name.lower().startswith('total') or name.lower() == 'name':
            continue

        companies.append({
            'rank': rank,
            'name': name,
            'ticker': ticker,
            'url': '',
            'country': country,
        })
        rank += 1

    if not companies:
        raise Exception(
            f'iShares scraper found no equity holdings in the CSV from {url}. '
            'Check the URL is correct and still active.'
        )

    return companies


# ---------------------------------------------------------------------------
# Nasdaq Composite scraper (Nasdaq stock screener API)
# ---------------------------------------------------------------------------

def scrape_nasdaq(url: str, country_default: str = 'United States') -> List[dict]:
    """
    Fetch all NASDAQ-listed stocks from the Nasdaq screener API.

    The Nasdaq screener CSV returns all ~3,000+ stocks on the NASDAQ exchange.
    Columns include: Symbol, Name, LastSale, NetChange, %Change, MarketCap,
                     Country, IPOyear, Volume, Sector, Industry, URL

    Returns a list of dicts: {rank, name, ticker, url, country}
    """
    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/120.0.0.0 Safari/537.36'
        ),
        'Accept': 'text/csv,application/csv,*/*',
    }

    try:
        resp = requests.get(url, headers=headers, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise Exception(f'Nasdaq screener request failed: {e}')

    try:
        data = resp.json()
    except ValueError as e:
        raise Exception(f'Nasdaq screener returned non-JSON response: {e}')

    try:
        rows = data['data']['rows']
    except (KeyError, TypeError) as e:
        raise Exception(f'Nasdaq screener JSON structure changed: {e}')

    companies = []
    rank = 1

    for row in rows:
        name = (row.get('name') or '').strip()
        ticker = (row.get('symbol') or '').strip()
        country = (row.get('country') or country_default).strip()

        if not name or not ticker:
            continue

        companies.append({
            'rank': rank,
            'name': name,
            'ticker': ticker,
            'url': '',
            'country': country,
        })
        rank += 1

    if not companies:
        raise Exception(
            f'Nasdaq screener returned no data from {url}. '
            'The API endpoint may have changed.'
        )

    return companies


# ---------------------------------------------------------------------------
# MarketScreener scraper (kept as fallback)
# ---------------------------------------------------------------------------

def scrape_marketscreener(url: str) -> List[dict]:
    """
    Scrape index components from MarketScreener (paginated, ~50 rows/page).
    Returns a list of dicts: {rank, name, ticker, url, country}
    """
    session = requests.Session()
    session.headers.update({
        'User-Agent': (
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/120.0.0.0 Safari/537.36'
        ),
        'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.9',
        'Referer': 'https://uk.marketscreener.com/',
    })

    companies = []
    rank = 1
    page = 1

    while True:
        page_url = url if page == 1 else f'{url.rstrip("/")}/?p={page}'
        try:
            resp = session.get(page_url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            if companies:
                break
            raise Exception(f'MarketScreener request failed: {e}')

        soup = BeautifulSoup(resp.text, 'lxml')
        table = (
            soup.find('table', class_=re.compile(
                r'ComponentsTable|constituents|index-components', re.I))
            or soup.find('table', class_='table')
            or soup.find('table')
        )

        if not table:
            break

        rows = table.find_all('tr')
        page_companies = []

        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 2:
                continue
            name_cell = cells[0]
            link = name_cell.find('a')
            name = (link.get_text(strip=True) if link
                    else name_cell.get_text(strip=True))
            if not name or name.lower() in ('name', 'company', 'stock'):
                continue
            ticker = cells[1].get_text(strip=True) if len(cells) > 1 else ''
            company_url = ''
            if link and link.get('href', ''):
                href = link['href']
                if href.startswith('http'):
                    company_url = href
                elif href.startswith('/'):
                    company_url = 'https://uk.marketscreener.com' + href
            page_companies.append({
                'rank': rank, 'name': name, 'ticker': ticker,
                'url': company_url, 'country': '',
            })
            rank += 1

        if not page_companies:
            break

        companies.extend(page_companies)
        page += 1
        time.sleep(1)
        if page > 50:
            break

    if not companies:
        raise Exception(
            f'MarketScreener scraper returned no data for {url}.'
        )
    return companies


# ---------------------------------------------------------------------------
# Fortune 500 / 1000 scraper
# ---------------------------------------------------------------------------

def _scrape_fortune_50pros(limit: int, country_default: str) -> List[dict]:
    """
    Scrape 50pros.com/fortune500 — server-side rendered, no JS needed.
    Returns up to 500 companies (Fortune 500 only; ranks 501-1000 not available here).
    """
    from bs4 import BeautifulSoup
    resp = requests.get(
        'https://www.50pros.com/fortune500',
        headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'},
        timeout=30,
    )
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    table = soup.find('table')
    if not table:
        raise Exception('50pros.com: no table found')
    companies = []
    for row in table.find_all('tr')[1:]:  # skip header
        cells = row.find_all('td')
        if len(cells) < 2:
            continue
        rank_str = cells[0].get_text(strip=True)
        if not rank_str.isdigit():
            continue
        rank_val = int(rank_str)
        if rank_val > limit:
            break
        name = cells[1].get_text(strip=True)
        link_tag = cells[1].find('a')
        company_url = link_tag['href'] if link_tag and link_tag.get('href') else ''
        if name:
            companies.append({
                'rank': rank_val,
                'name': name,
                'ticker': '',
                'url': company_url,
                'country': country_default,
            })
    return companies


def _scrape_fortune_us500_playwright(limit: int, country_default: str) -> List[dict]:
    """
    Scrape us500.com/fortune-1000-companies via Playwright (requires headless browser).
    Returns up to `limit` companies. Handles Fortune's tied ranks (same rank number
    for multiple companies; the following rank number is intentionally skipped).

    NOTE: Playwright is only available locally, not on Render.
    """
    import time
    from playwright.sync_api import sync_playwright

    FIELDS_PER_ROW = 10

    def parse_rows(lines):
        companies = []
        i = 0
        while i < len(lines):
            if lines[i].isdigit():
                rank = int(lines[i])
                if 1 <= rank <= limit and i + FIELDS_PER_ROW <= len(lines):
                    companies.append({
                        'rank': rank,
                        'name': lines[i + 1],
                        'ticker': '',
                        'url': '',
                        'country': country_default,
                    })
                    i += FIELDS_PER_ROW
                    continue
            i += 1
        return companies

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
        )
        page = browser.new_page()
        page.goto('https://us500.com/fortune-1000-companies', wait_until='networkidle', timeout=60000)
        time.sleep(2)

        scroll_el = page.query_selector('div.overflow-auto')
        if not scroll_el:
            browser.close()
            raise Exception('us500.com: could not find scroll container')

        all_companies: dict = {}  # key=rank_name to capture ties
        scroll_pos = 0
        scroll_step = 600
        stale_count = 0

        scroll_el.evaluate('el => el.scrollTop = 0')
        time.sleep(1)

        for _ in range(300):
            text = scroll_el.inner_text()
            lines = [l.strip() for l in text.split('\n') if l.strip()]
            batch = parse_rows(lines)
            before = len(all_companies)
            for c in batch:
                all_companies[f"{c['rank']}_{c['name']}"] = c
            added = len(all_companies) - before

            max_rank = max(c['rank'] for c in all_companies.values()) if all_companies else 0
            if max_rank >= limit and added == 0:
                stale_count += 1
                if stale_count >= 8:
                    break
            else:
                stale_count = 0

            scroll_pos += scroll_step
            scroll_el.evaluate(f'el => el.scrollTop = {scroll_pos}')
            time.sleep(0.4)

        browser.close()

    companies = sorted(all_companies.values(), key=lambda x: (x['rank'], x['name']))
    companies = [c for c in companies if c['rank'] <= limit]
    if not companies:
        raise Exception('us500.com: no companies scraped')
    return companies


def scrape_fortune(
    url: str,
    limit: int = 500,
    country_default: str = 'United States',
) -> List[dict]:
    """
    Scrape the Fortune 500/1000.
    Strategy:
      1. Try us500.com via Playwright (full 1000, requires local headless browser).
      2. Try fortune.com franchise API (fast, but intermittently broken).
      3. Fall back to 50pros.com (server-side rendered, covers Fortune 500 only).
    Returns a list of dicts: {rank, name, ticker, url, country}
    """
    # 1. Try us500.com via Playwright (requires chromium; installed via render.yaml build command)
    try:
        companies = _scrape_fortune_us500_playwright(limit, country_default)
        if companies:
            return companies
    except Exception:
        pass

    # 2. Try fortune.com franchise API
    api_url = (
        'https://fortune.com/franchise-api/v1/items/fortune500'
        f'?genre=fortune500&count={limit}&field=rank&field=company'
        '&field=url&field=revenues&field=title'
    )
    try:
        resp = requests.get(
            api_url,
            headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://fortune.com/'},
            timeout=30,
        )
        if resp.status_code == 200:
            data = resp.json()
            items = data.get('items', data) if isinstance(data, dict) else data
            companies = []
            for item in items[:limit]:
                title = (
                    item.get('company') or item.get('title') or
                    item.get('fields', {}).get('company') or ''
                ).strip()
                rank_val = (
                    item.get('rank') or
                    item.get('fields', {}).get('rank') or
                    len(companies) + 1
                )
                company_url = (
                    item.get('url') or item.get('fields', {}).get('url') or ''
                )
                if title:
                    companies.append({
                        'rank': int(rank_val) if str(rank_val).isdigit()
                               else len(companies) + 1,
                        'name': title,
                        'ticker': '',
                        'url': company_url,
                        'country': country_default,
                    })
            if companies:
                return companies
    except Exception:
        pass

    # 3. Fall back to 50pros.com (covers ranks 1-500 only)
    companies = _scrape_fortune_50pros(min(limit, 500), country_default)
    if not companies:
        raise Exception('Fortune scraper: all sources failed.')
    return companies


# ---------------------------------------------------------------------------
# Forbes Global 2000 scraper
# ---------------------------------------------------------------------------

def scrape_forbes(url: str) -> List[dict]:
    """
    Scrape the Forbes Global 2000 from forbes.com.
    Tries the forbesapi first; falls back to Playwright.
    Returns a list of dicts: {rank, name, ticker, url, country}
    """
    import datetime
    current_year = datetime.datetime.utcnow().year
    # Forbes publishes the Global 2000 list in May/June — try current year first,
    # then fall back to previous year if the list hasn't been published yet.
    items = []
    for year in (current_year, current_year - 1):
        api_url = (
            f'https://www.forbes.com/forbesapi/org/global2000/{year}'
            '/position/true.json?fields=rank,organizationName,country,publicStatus,uri&limit=2000'
        )
        try:
            resp = requests.get(
                api_url,
                headers={'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.forbes.com/'},
                timeout=30,
            )
            if resp.status_code == 200:
                data = resp.json()
                items = data.get('organizationList', {}).get('organizationsLists', [])
                if not items:
                    items = data if isinstance(data, list) else []
                if items:
                    break  # Found data for this year — stop trying
        except Exception:
            pass
    try:
        if items:
            companies = []
            for item in items:
                name = (item.get('organizationName') or item.get('name') or '').strip()
                rank_val = item.get('rank') or len(companies) + 1
                uri = item.get('uri') or ''
                if uri.startswith('/'):
                    company_url = f'https://www.forbes.com{uri}'
                elif uri:
                    company_url = f'https://www.forbes.com/companies/{uri}/'
                else:
                    company_url = ''
                country = (item.get('country') or '').strip()
                if name:
                    companies.append({
                        'rank': int(rank_val) if str(rank_val).isdigit()
                               else len(companies) + 1,
                        'name': name,
                        'ticker': '',
                        'url': company_url,
                        'country': country,
                    })
            if companies:
                return companies
    except Exception:
        pass

    # Playwright fallback
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

    companies = []
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage'],
        )
        try:
            page = browser.new_page(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            )
            page.goto(url, wait_until='networkidle', timeout=60000)
            try:
                page.wait_for_selector(
                    'table tr, [class*="listItem"], [class*="row"]',
                    timeout=15000,
                )
            except PlaywrightTimeout:
                pass
            page.wait_for_timeout(3000)

            rows = page.evaluate("""
                () => {
                    const results = [];
                    document.querySelectorAll(
                        'table tr, [class*="listItem"], [class*="rankRow"]'
                    ).forEach((row, idx) => {
                        const nameEl = row.querySelector(
                            '[class*="name"], [class*="org"], td:nth-child(2), a'
                        );
                        if (nameEl) {
                            const name = nameEl.textContent.trim();
                            const link = nameEl.closest('a') || nameEl.querySelector('a');
                            if (name && name.length > 1 && !/^\\d+$/.test(name)) {
                                results.push({
                                    rank: idx + 1, name, url: link ? link.href : ''
                                });
                            }
                        }
                    });
                    return results;
                }
            """)

            for i, r in enumerate(rows):
                companies.append({
                    'rank': r.get('rank', i + 1),
                    'name': r.get('name', '').strip(),
                    'ticker': '',
                    'url': r.get('url', ''),
                    'country': '',
                })
        finally:
            browser.close()

    if not companies:
        raise Exception(
            f'Forbes scraper returned no data from {url}.'
        )
    return companies


# ---------------------------------------------------------------------------
# Investing.com components scraper
#
# investing.com serves its index components tables in server-rendered HTML —
# no JS or login required. URL pattern:
#   https://www.investing.com/indices/{slug}-components
# The components are in the second <table> on the page (30 rows for TecDAX).
# Names have a "derived" suffix appended by the site — stripped on parse.
# ---------------------------------------------------------------------------

def scrape_investing(url: str, country_default: str = '') -> List[dict]:
    """
    Scrape index components from an investing.com components page.

    URL must end in '-components', e.g.:
      https://www.investing.com/indices/tecdax-components
      https://www.investing.com/indices/mdax-components

    Returns a list of dicts: {rank, name, ticker, url, country}
    """
    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/120.0.0.0 Safari/537.36'
        ),
        'Accept': 'text/html,application/xhtml+xml,*/*;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.investing.com/',
    }

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise Exception(f'investing.com request failed: {e}')

    soup = BeautifulSoup(resp.text, 'lxml')
    tables = soup.find_all('table')

    # Find the components table: has 'Name' header and 20+ rows
    comp_table = None
    for table in tables:
        header_cells = [th.get_text(strip=True) for th in table.find_all('th')]
        rows = table.find_all('tr')
        if 'Name' in header_cells and len(rows) > 10:
            comp_table = table
            break

    if comp_table is None:
        raise Exception(
            f'investing.com: could not find components table at {url}. '
            'Page structure may have changed.'
        )

    companies = []
    for rank, row in enumerate(comp_table.find_all('tr')[1:], start=1):
        cells = row.find_all('td')
        if len(cells) < 2:
            continue

        # Name is in the second cell; investing.com appends "derived" to names.
        # Some names with special chars (e.g. "1&1") lose their ampersand in
        # investing.com's HTML — fall back to the checkbox input value which
        # has the same issue, so we apply a known-corrections map afterwards.
        raw_name = cells[1].get_text(strip=True)
        name = re.sub(r'derived$', '', raw_name).strip()
        if not name:
            continue

        # Known name corrections for investing.com encoding issues
        _NAME_FIXES = {'11 AG': '1&1 AG'}
        name = _NAME_FIXES.get(name, name)

        # Extract the investing.com detail URL from the link in the name cell
        link = cells[1].find('a')
        detail_url = ''
        if link and link.get('href', '').startswith('/'):
            detail_url = 'https://www.investing.com' + link['href']

        companies.append({
            'rank': rank,
            'name': name,
            'ticker': '',
            'url': detail_url,
            'country': country_default,
        })

    if not companies:
        raise Exception(
            f'investing.com: no companies parsed from {url}.'
        )

    return companies


# ---------------------------------------------------------------------------
# TradingView scraper (Playwright — kept as reference / manual fallback)
# NOTE: TradingView's components pages require a logged-in account to show
# more than ~10 rows ("Log in or create a free account to see all components").
# The scanner API (scanner.tradingview.com) does not support index-membership
# filtering. All indices should use Wikipedia or iShares CSV sources instead.
# ---------------------------------------------------------------------------

def scrape_tradingview(url: str) -> List[dict]:
    """
    Scrape index components from a TradingView components page.
    NOTE: Due to virtual scrolling limitations, typically returns ≤10 rows.
    Prefer Wikipedia or iShares sources for reliable full-index data.
    Returns a list of dicts: {rank, name, ticker, url, country}
    """
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

    seen_keys: set = set()
    companies: List[dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu'],
        )
        try:
            page = browser.new_page(
                user_agent=(
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/120.0.0.0 Safari/537.36'
                )
            )
            page.goto(url, wait_until='networkidle', timeout=60000)
            page.set_viewport_size({'width': 1920, 'height': 4000})
            page.wait_for_timeout(1000)

            try:
                page.wait_for_selector('tr[data-rowkey]', timeout=20000)
            except PlaywrightTimeout:
                raise Exception(
                    f'TradingView page loaded but no data rows appeared at {url}.'
                )

            page.wait_for_timeout(2000)

            for _ in range(80):
                batch = page.evaluate("""
                    () => {
                        const rows = document.querySelectorAll('tr[data-rowkey]');
                        const results = [];
                        rows.forEach(row => {
                            const rowKey = row.getAttribute('data-rowkey') || '';
                            const ticker = rowKey.includes(':')
                                ? rowKey.split(':').pop() : rowKey;
                            const descEl = row.querySelector('[class*="tickerDescription"]');
                            let name = descEl ? descEl.textContent.trim() : '';
                            if (!name) {
                                const titleEl = row.querySelector('[title*=" \u2212 "]');
                                if (titleEl) {
                                    const parts = titleEl.getAttribute('title').split(' \u2212 ');
                                    name = parts.length > 1 ? parts[1].trim() : '';
                                }
                            }
                            if (name) results.push({ key: rowKey, ticker, name });
                        });
                        return results;
                    }
                """)

                new_found = False
                for row in batch:
                    if row['key'] not in seen_keys:
                        seen_keys.add(row['key'])
                        companies.append({
                            'rank': len(companies) + 1,
                            'name': row['name'],
                            'ticker': row['ticker'],
                            'url': '',
                            'country': '',
                        })
                        new_found = True

                if not new_found:
                    break

                page.evaluate("""
                    () => {
                        document.querySelectorAll('*').forEach(el => {
                            try {
                                if (el.scrollHeight > el.clientHeight + 10
                                        && getComputedStyle(el).overflow !== 'visible') {
                                    el.scrollTop += 800;
                                }
                            } catch(e) {}
                        });
                        window.scrollBy(0, 800);
                    }
                """)
                page.wait_for_timeout(500)

        finally:
            browser.close()

    if not companies:
        raise Exception(
            f'TradingView scraper returned no data for {url}.'
        )
    return companies


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def scrape_index(index_id: str, config: dict) -> List[dict]:
    """
    Route to the correct scraper based on config['source'].
    After scraping, optionally enriches URLs via Kernel CLI (local only).

    Returns a list of company dicts: {rank, name, ticker, url, country}
    Raises Exception on failure (callers should catch and report to UI).
    """
    source = config.get('source')
    country_default = config.get('country_default', '')

    if source == 'manual' or config.get('manual'):
        raise Exception(
            f'Index "{config.get("name", index_id)}" requires manual CSV upload. '
            'Use the Upload CSV button to populate this index.'
        )

    if source == 'wikipedia':
        companies = scrape_wikipedia(
            config['url'],
            config.get('table_index', 0),
            country_default,
        )

    elif source == 'ishares':
        companies = scrape_ishares(config['url'])
        # Apply country_default as fallback for companies with no country
        if country_default:
            for c in companies:
                if not c.get('country'):
                    c['country'] = country_default

    elif source == 'nasdaq':
        companies = scrape_nasdaq(config['url'], country_default)

    elif source == 'marketscreener':
        companies = scrape_marketscreener(config['url'])

    elif source == 'fortune':
        companies = scrape_fortune(
            config['url'], config.get('limit', 500), country_default
        )

    elif source == 'forbes':
        companies = scrape_forbes(config['url'])

    elif source == 'investing':
        companies = scrape_investing(config['url'], country_default)

    elif source == 'tradingview':
        companies = scrape_tradingview(config['url'])

    elif source == 'wikipedia_navbox':
        companies = scrape_wikipedia_navbox(config['url'], country_default)

    elif source == 'derived':
        # Combine multiple sub-indices into one (e.g. FTSE 350 = FTSE 100 + FTSE 250)
        companies = []
        for component_id in config.get('components', []):
            component_cfg = INDICES_CONFIG.get(component_id)
            if not component_cfg:
                raise Exception(
                    f'Derived index "{index_id}" references unknown component "{component_id}".'
                )
            part = scrape_index(component_id, component_cfg)
            start_rank = len(companies) + 1
            for c in part:
                companies.append({**c, 'rank': start_rank + c['rank'] - 1})

    else:
        raise Exception(f'Unknown source "{source}" for index "{index_id}".')

    # Sanitize URLs, clean tickers, and deduplicate by company name
    companies = _post_process(companies)

    # Optional: enrich missing website URLs via Kernel CLI (no-op on Render)
    companies = enrich_urls_with_kernel(companies)

    return companies


def validate_against_alt(index_id: str, config: dict, primary_companies: list) -> dict:
    """
    Scrape the alt source (if configured) and compare count + name overlap
    against the primary scrape result. Returns a validation dict.
    """
    alt_url = config.get('alt_url')
    alt_source = config.get('alt_source')
    if not alt_url or not alt_source:
        return {}

    from datetime import datetime
    validated_at = datetime.utcnow().isoformat() + 'Z'

    try:
        if alt_source == 'investing':
            alt_companies = scrape_investing(alt_url, config.get('country_default', ''))
        else:
            return {}

        alt_count = len(alt_companies)
        primary_count = len(primary_companies)

        # Normalised name overlap
        def _norm(n):
            import re as _re
            n = n.lower()
            n = _re.sub(r'[^\w\s]', ' ', n)
            n = _re.sub(r'\s+(ag|se|plc|inc|corp|ltd|nv|sa|ab|asa|oyj|co|llc)$', '', n)
            return _re.sub(r'\s+', ' ', n).strip()

        primary_names = {_norm(c['name']) for c in primary_companies}
        alt_names = {_norm(c['name']) for c in alt_companies}
        overlap = len(primary_names & alt_names)
        overlap_pct = round(overlap / max(len(primary_names), 1) * 100, 1)

        # Count deviation % — primary signal (name formatting varies too much)
        count_dev = abs(alt_count - primary_count) / max(primary_count, 1) * 100

        if count_dev <= 5:
            status = 'ok'       # counts match within 5% — high confidence
        elif count_dev <= 15:
            status = 'warn'     # noticeable gap — worth investigating
        else:
            status = 'fail'     # >15% off — likely stale or wrong source

        return {
            'status': status,
            'primary_count': primary_count,
            'alt_count': alt_count,
            'overlap_pct': overlap_pct,
            'validated_at': validated_at,
        }

    except Exception as e:
        return {
            'status': 'error',
            'error': str(e),
            'validated_at': validated_at,
        }
