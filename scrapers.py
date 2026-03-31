"""
scrapers.py — Index composition scrapers for each data source.

Supported sources:
  - tradingview   : Playwright-based JS-rendered pages
  - wikipedia     : requests + BeautifulSoup table parsing
  - marketscreener: requests + BeautifulSoup with pagination
  - euronext      : requests + BeautifulSoup
  - manual        : CSV upload, no scraper
"""

import requests
from bs4 import BeautifulSoup
import time
import re
from typing import List

# ---------------------------------------------------------------------------
# Index Configuration
# ---------------------------------------------------------------------------

INDICES_CONFIG = {
    'omcx_25': {
        'name': 'OMXC25',
        'source': 'tradingview',
        'url': 'https://www.tradingview.com/symbols/NASDAQ-OMXC25/components/',
    },
    'omxh_25': {
        'name': 'OMXH25',
        'source': 'tradingview',
        'url': 'https://www.tradingview.com/symbols/OMXHEX-OMXH25/components/',
    },
    'obx': {
        'name': 'OBX',
        'source': 'tradingview',
        'url': 'https://www.tradingview.com/symbols/INDEX-OBX/components/',
    },
    'ta_35': {
        'name': 'TA-35',
        'source': 'tradingview',
        'url': 'https://www.tradingview.com/symbols/TASE-TA35/components/',
    },
    'smi': {
        'name': 'SMI',
        'source': 'tradingview',
        'url': 'https://www.tradingview.com/symbols/SIX-SMI/components/',
    },
    'aex': {
        'name': 'AEX',
        'source': 'tradingview',
        'url': 'https://www.tradingview.com/symbols/EURONEXT-AEX/components/',
    },
    'bel_20': {
        'name': 'BEL 20',
        'source': 'tradingview',
        'url': 'https://www.tradingview.com/symbols/EURONEXT-BEL20/components/',
    },
    'omx_stockholm_30': {
        'name': 'OMX Stockholm 30',
        'source': 'tradingview',
        'url': 'https://www.tradingview.com/symbols/NASDAQ-OMXS30/components/',
    },
    'ibex_35': {
        'name': 'IBEX 35',
        'source': 'tradingview',
        'url': 'https://www.tradingview.com/symbols/BME-IBC/components/',
    },
    'nifty_50': {
        'name': 'NIFTY 50',
        'source': 'tradingview',
        'url': 'https://www.tradingview.com/symbols/NSE-NIFTY/components/',
    },
    'nikkei_225': {
        'name': 'Nikkei 225',
        'source': 'tradingview',
        'url': 'https://www.tradingview.com/symbols/TVC-NI225/components/',
    },
    'euro_stoxx_50': {
        'name': 'Euro STOXX 50',
        'source': 'tradingview',
        'url': 'https://www.tradingview.com/symbols/TVC-SX5E/components/',
    },
    'dji': {
        'name': 'Dow Jones Industrial Average',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average',
        'table_index': 1,
    },
    'stoxx_eu_600': {
        'name': 'STOXX Europe 600',
        'source': 'tradingview',
        'url': 'https://www.tradingview.com/symbols/TVC-SX5P/components/',
    },
    'stoxx_global_1800': {
        'name': 'STOXX Global 1800',
        'source': 'ishares',
        # iShares STOXX Global 1800 UCITS ETF (ISWD) holdings CSV
        'url': 'https://www.ishares.com/uk/individual/en/products/251892/ishares-stoxx-global-1800-ucits-etf/1478372549651.ajax?fileType=csv&fileName=ISWD_holdings&dataType=fund',
    },
    'msci_world': {
        'name': 'MSCI World',
        'source': 'marketscreener',
        'url': 'https://uk.marketscreener.com/quote/index/MSCI-WORLD-107361487/components/',
    },
    's_p_global_1200': {
        'name': 'S&P Global 1200',
        'source': 'ishares',
        # iShares Core S&P Global 1200 UCITS ETF (ISSP) holdings CSV
        'url': 'https://www.ishares.com/uk/individual/en/products/264119/ishares-sp-global-1200-ucits-etf/1478372549651.ajax?fileType=csv&fileName=ISSP_holdings&dataType=fund',
    },
    'cac_40': {
        'name': 'CAC 40',
        'source': 'marketscreener',
        'url': 'https://uk.marketscreener.com/quote/index/CAC-40-4941/components/',
    },
    'cac_next_20': {
        'name': 'CAC Next 20',
        'source': 'euronext',
        'url': 'https://live.euronext.com/en/product/indices/QS0010989109-XPAR/index-composition',
    },
    'nasdaq_100': {
        'name': 'Nasdaq 100',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/Nasdaq-100',
        'table_index': 4,
    },
    'dax': {
        'name': 'DAX',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/DAX',
        'table_index': 3,
    },
    'fortune_500': {
        'name': 'Fortune 500',
        'source': 'fortune',
        'url': 'https://fortune.com/ranking/fortune500/',
        'limit': 500,
    },
    'fortune_1000': {
        'name': 'Fortune 1000',
        'source': 'fortune',
        'url': 'https://fortune.com/ranking/fortune500/',
        'limit': 1000,
    },
    's_p_500': {
        'name': 'S&P 500',
        'source': 'wikipedia',
        'url': 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
        'table_index': 0,
    },
    'forbes_2000': {
        'name': 'Forbes Global 2000',
        'source': 'forbes',
        'url': 'https://www.forbes.com/lists/global2000/',
    },
    'tecdax': {
        'name': 'TecDAX',
        'source': 'tradingview',
        'url': 'https://www.tradingview.com/symbols/XETR-TDXP/components/',
    },
    'ftse_100': {
        'name': 'FTSE 100',
        'source': 'tradingview',
        'url': 'https://www.tradingview.com/symbols/FTSE-UKX/components/',
    },
    'ftse_250': {
        'name': 'FTSE 250',
        'source': 'tradingview',
        'url': 'https://www.tradingview.com/symbols/FTSE-MCX/components/',
    },
    'ftse_350': {
        'name': 'FTSE 350',
        'source': 'tradingview',
        'url': 'https://www.tradingview.com/symbols/FTSE-NMX/components/',
    },
    'mdax': {
        'name': 'MDAX',
        'source': 'tradingview',
        'url': 'https://www.tradingview.com/symbols/XETR-MDAX/components/',
    },
    'sdax': {
        'name': 'SDAX',
        'source': 'tradingview',
        'url': 'https://www.tradingview.com/symbols/XETR-SDXP/components/',
    },
    'nasdaq_composite': {
        'name': 'Nasdaq Composite',
        'source': 'tradingview',
        'url': 'https://www.tradingview.com/symbols/NASDAQ-IXIC/components/',
    },
}


# ---------------------------------------------------------------------------
# TradingView scraper (Playwright)
# ---------------------------------------------------------------------------

def scrape_tradingview(url: str) -> List[dict]:
    """
    Scrape index components from a TradingView components page.

    TradingView renders its tables with JavaScript, so we use Playwright
    to drive a headless Chromium browser. We try several CSS selector
    patterns because TradingView occasionally changes its class names.

    Returns a list of dicts: {rank, name, ticker, url}
    Raises an exception if the page loads but no data can be extracted.
    """
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

    companies = []

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

            # Try to wait for any recognisable row element
            row_selectors = [
                'tr[data-rowtype]',
                '.tv-screener-table__result-row',
                '[class*="listRow"]',
                '[class*="tableRow"]',
                '[class*="row-"]',
                'table tbody tr',
            ]
            found_selector = None
            for sel in row_selectors:
                try:
                    page.wait_for_selector(sel, timeout=10000)
                    found_selector = sel
                    break
                except PlaywrightTimeout:
                    continue

            # Give the page an extra moment for all rows to render
            page.wait_for_timeout(2000)

            # ----------------------------------------------------------------
            # Strategy 1: JSON data embedded in window.__data__ / __INITIAL_STATE__
            # ----------------------------------------------------------------
            json_data = page.evaluate("""
                () => {
                    // Try to find table rows in the DOM with data attributes
                    const rows = document.querySelectorAll('tr[data-rowtype], [class*="listRow"], [class*="tableRow"]');
                    if (rows.length === 0) return null;
                    const results = [];
                    rows.forEach((row, idx) => {
                        const cells = row.querySelectorAll('td, [class*="cell"]');
                        if (cells.length < 2) return;
                        // First cell often has ticker + name together
                        const firstCell = cells[0];
                        const ticker = firstCell.querySelector('[class*="symbol"], [class*="ticker"]')?.textContent?.trim() || '';
                        const name = firstCell.querySelector('[class*="description"], [class*="name"], [class*="title"]')?.textContent?.trim()
                                  || firstCell.textContent?.trim() || '';
                        results.push({ rank: idx + 1, ticker, name });
                    });
                    return results.length > 0 ? results : null;
                }
            """)

            if json_data:
                companies = json_data
            else:
                # ----------------------------------------------------------------
                # Strategy 2: Parse generic table rows
                # ----------------------------------------------------------------
                table_data = page.evaluate("""
                    () => {
                        const tbodies = document.querySelectorAll('table tbody');
                        for (const tbody of tbodies) {
                            const rows = tbody.querySelectorAll('tr');
                            if (rows.length < 2) continue;
                            const results = [];
                            rows.forEach((row, idx) => {
                                const cells = row.querySelectorAll('td');
                                if (cells.length < 2) return;
                                const name = cells[0].textContent?.trim() || cells[1].textContent?.trim() || '';
                                const ticker = cells[1].textContent?.trim() || '';
                                if (name) {
                                    results.push({ rank: idx + 1, ticker, name });
                                }
                            });
                            if (results.length > 0) return results;
                        }
                        return null;
                    }
                """)

                if table_data:
                    companies = table_data

            # ----------------------------------------------------------------
            # Strategy 3: Scrape visible text of any cells we can find
            # ----------------------------------------------------------------
            if not companies and found_selector:
                rows = page.query_selector_all(found_selector)
                for idx, row in enumerate(rows):
                    # Get all text nodes in the row
                    texts = row.evaluate("""
                        el => {
                            const spans = el.querySelectorAll('span, td, div');
                            return Array.from(spans).map(s => s.textContent.trim()).filter(t => t.length > 0);
                        }
                    """)
                    if texts and len(texts) >= 1:
                        companies.append({
                            'rank': idx + 1,
                            'name': texts[0],
                            'ticker': texts[1] if len(texts) > 1 else '',
                        })

        finally:
            browser.close()

    if not companies:
        raise Exception(
            f'TradingView scraper returned no data for {url}. '
            'The page may have changed its structure or blocked the request. '
            'Try again or upload data manually.'
        )

    # Normalise output: ensure all dicts have the required keys
    normalised = []
    for i, c in enumerate(companies):
        name = c.get('name', '').strip()
        ticker = c.get('ticker', '').strip()
        # Skip obviously bad rows (empty or purely numeric names)
        if not name or name.isdigit():
            continue
        normalised.append({
            'rank': c.get('rank', i + 1),
            'name': name,
            'ticker': ticker,
            'url': '',
        })

    return normalised


# ---------------------------------------------------------------------------
# Wikipedia scraper
# ---------------------------------------------------------------------------

def scrape_wikipedia(url: str, table_index: int = 0) -> List[dict]:
    """
    Scrape index components from a Wikipedia article.

    Finds the wikitable at position `table_index`. If that table doesn't
    look like a company list, tries adjacent tables until one is found.

    Returns a list of dicts: {rank, name, ticker, url}
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

    def parse_table(table) -> List[dict]:
        """Try to extract company rows from a single table."""
        rows = table.find_all('tr')
        if len(rows) < 2:
            return []

        # Find header row to determine column positions
        header_row = rows[0]
        headers_text = [
            th.get_text(strip=True).lower()
            for th in header_row.find_all(['th', 'td'])
        ]

        # Detect likely company name and ticker columns
        name_col = next(
            (i for i, h in enumerate(headers_text)
             if any(kw in h for kw in ['company', 'name', 'stock', 'security', 'constituent'])),
            0,
        )
        ticker_col = next(
            (i for i, h in enumerate(headers_text)
             if any(kw in h for kw in ['symbol', 'ticker', 'code'])),
            1,
        )

        companies = []
        for rank, row in enumerate(rows[1:], start=1):
            cells = row.find_all(['td', 'th'])
            if len(cells) <= max(name_col, ticker_col):
                continue

            name_cell = cells[name_col]
            # Prefer the link text if present (cleaner than full cell text)
            link = name_cell.find('a')
            name = (link.get_text(strip=True) if link
                    else name_cell.get_text(strip=True))

            ticker_cell = cells[ticker_col] if ticker_col < len(cells) else None
            ticker = ticker_cell.get_text(strip=True) if ticker_cell else ''

            # Skip header-like rows that leaked through
            if not name or name.lower() in ('company', 'name', 'stock', 'security'):
                continue

            # Extract Wikipedia article URL for the company if available
            wiki_url = ''
            if link and link.get('href', '').startswith('/wiki/'):
                wiki_url = 'https://en.wikipedia.org' + link['href']

            companies.append({
                'rank': rank,
                'name': name,
                'ticker': ticker,
                'url': wiki_url,
            })

        return companies

    # Try the requested table_index first, then search nearby tables
    indices_to_try = [table_index] + [
        i for i in range(len(tables)) if i != table_index
    ]

    for idx in indices_to_try:
        if idx >= len(tables):
            continue
        result = parse_table(tables[idx])
        if len(result) >= 3:  # At least 3 rows = plausible company list
            return result

    raise Exception(
        f'Could not find a valid company table on Wikipedia page: {url}. '
        f'Tried {len(tables)} tables.'
    )


# ---------------------------------------------------------------------------
# MarketScreener scraper
# ---------------------------------------------------------------------------

def scrape_marketscreener(url: str) -> List[dict]:
    """
    Scrape index components from MarketScreener.

    MarketScreener paginates its components table (~50 rows per page).
    We iterate pages until we receive an empty table.

    Returns a list of dicts: {rank, name, ticker, url}
    """
    session = requests.Session()
    session.headers.update({
        'User-Agent': (
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/120.0.0.0 Safari/537.36'
        ),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
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
                # We already have some data; stop pagination gracefully
                break
            raise Exception(f'MarketScreener request failed: {e}')

        soup = BeautifulSoup(resp.text, 'lxml')

        # MarketScreener uses a table with class "table" for the components list
        table = (
            soup.find('table', class_=re.compile(r'ComponentsTable|constituents|index-components', re.I))
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

            # Name is usually in the first cell with a link
            name_cell = cells[0]
            link = name_cell.find('a')
            name = link.get_text(strip=True) if link else name_cell.get_text(strip=True)

            if not name or name.lower() in ('name', 'company', 'stock'):
                continue

            # Ticker often in second cell
            ticker = cells[1].get_text(strip=True) if len(cells) > 1 else ''

            company_url = ''
            if link and link.get('href', ''):
                href = link['href']
                if href.startswith('http'):
                    company_url = href
                elif href.startswith('/'):
                    company_url = 'https://uk.marketscreener.com' + href

            page_companies.append({
                'rank': rank,
                'name': name,
                'ticker': ticker,
                'url': company_url,
            })
            rank += 1

        if not page_companies:
            break

        companies.extend(page_companies)
        page += 1
        time.sleep(1)  # Be polite between pagination requests

        # Safety limit to avoid infinite loops
        if page > 50:
            break

    if not companies:
        raise Exception(
            f'MarketScreener scraper returned no data for {url}. '
            'The page structure may have changed or access was blocked.'
        )

    return companies


# ---------------------------------------------------------------------------
# Euronext scraper
# ---------------------------------------------------------------------------

def scrape_euronext(url: str) -> List[dict]:
    """
    Scrape index composition from a Euronext live index-composition page.

    Euronext renders its composition table server-side, so plain requests
    should work. We look for the main data table.

    Returns a list of dicts: {rank, name, ticker, url}
    """
    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/120.0.0.0 Safari/537.36'
        ),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.9',
    }

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise Exception(f'Euronext request failed: {e}')

    soup = BeautifulSoup(resp.text, 'lxml')

    # Try known table patterns on live.euronext.com
    table = (
        soup.find('table', id=re.compile(r'index.composition|constituents', re.I))
        or soup.find('table', class_=re.compile(r'composition|constituent', re.I))
        or soup.find('table')
    )

    if not table:
        raise Exception(f'No table found on Euronext page: {url}')

    companies = []
    rows = table.find_all('tr')

    for rank, row in enumerate(rows, start=1):
        cells = row.find_all('td')
        if len(cells) < 2:
            continue

        # Determine column layout from content
        name = ''
        ticker = ''

        # Euronext typically has: Name | ISIN | Currency | ...
        # Sometimes: Rank | Name | Ticker | ...
        first_text = cells[0].get_text(strip=True)
        if first_text.isdigit():
            # First column is a rank number
            name = cells[1].get_text(strip=True) if len(cells) > 1 else ''
            ticker = cells[2].get_text(strip=True) if len(cells) > 2 else ''
            rank = int(first_text)
        else:
            name = first_text
            ticker = cells[1].get_text(strip=True) if len(cells) > 1 else ''

        if not name or name.lower() in ('name', 'company', 'instrument'):
            continue

        # Extract link if present
        link = cells[0].find('a') or (cells[1].find('a') if len(cells) > 1 else None)
        company_url = ''
        if link and link.get('href', ''):
            href = link['href']
            if href.startswith('http'):
                company_url = href
            elif href.startswith('/'):
                company_url = 'https://live.euronext.com' + href

        companies.append({
            'rank': rank,
            'name': name,
            'ticker': ticker,
            'url': company_url,
        })

    if not companies:
        raise Exception(
            f'Euronext scraper returned no data for {url}. '
            'The page structure may have changed or the table was empty.'
        )

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

    Returns a list of dicts: {rank, name, ticker, url}
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
        # The data header row contains 'Name' and 'Ticker' or 'Asset Class'
        if re.search(r'\bName\b', line) and re.search(r'\bTicker\b|\bAsset Class\b|\bISIN\b', line):
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
    # Asset classes to skip (cash, futures, derivatives etc.)
    skip_asset_classes = {'cash', 'money market', 'future', 'forward', 'option', 'swap', 'other'}

    for row in reader:
        name = (row.get('Name') or row.get('Issuer') or '').strip()
        ticker = (row.get('Ticker') or '').strip()
        asset_class = (row.get('Asset Class') or '').strip().lower()

        if not name or name in ('-', '', 'N/A'):
            continue
        if asset_class in skip_asset_classes:
            continue
        # Skip rows that are clearly totals/headers
        if name.lower().startswith('total') or name.lower() == 'name':
            continue

        companies.append({
            'rank': rank,
            'name': name,
            'ticker': ticker,
            'url': '',
        })
        rank += 1

    if not companies:
        raise Exception(
            f'iShares scraper found no equity holdings in the CSV from {url}. '
            'Check the URL is correct and still active.'
        )

    return companies


# ---------------------------------------------------------------------------
# Fortune 500 / 1000 scraper
# ---------------------------------------------------------------------------

def scrape_fortune(url: str, limit: int = 500) -> List[dict]:
    """
    Scrape the Fortune 500/1000 ranking from fortune.com.

    First tries Fortune's internal franchise API (fast, no browser).
    Falls back to Playwright if the API is unavailable.

    Returns a list of dicts: {rank, name, ticker, url}
    """
    # Try the internal franchise API first
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
                rank_val = item.get('rank') or item.get('fields', {}).get('rank') or len(companies) + 1
                company_url = item.get('url') or item.get('fields', {}).get('url') or ''
                if title:
                    companies.append({
                        'rank': int(rank_val) if str(rank_val).isdigit() else len(companies) + 1,
                        'name': title,
                        'ticker': '',
                        'url': company_url,
                    })
            if companies:
                return companies
    except Exception:
        pass  # Fall through to Playwright

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

            # Try to find ranking table rows
            try:
                page.wait_for_selector(
                    '[class*="rank"], [class*="company"], table tr, [data-testid*="row"]',
                    timeout=15000,
                )
            except PlaywrightTimeout:
                pass

            page.wait_for_timeout(3000)

            # Extract company names from the page
            rows = page.evaluate(f"""
                () => {{
                    const results = [];
                    // Try table rows
                    const tableRows = document.querySelectorAll('table tr, [class*="listItem"], [class*="rankRow"], [class*="company-row"]');
                    tableRows.forEach((row, idx) => {{
                        if (results.length >= {limit}) return;
                        const nameEl = row.querySelector('[class*="company"], [class*="name"], td:nth-child(2), td:first-child a');
                        const rankEl = row.querySelector('[class*="rank"], td:first-child');
                        if (nameEl) {{
                            const name = nameEl.textContent.trim();
                            const rank = rankEl ? rankEl.textContent.trim() : String(idx + 1);
                            const link = nameEl.closest('a') || nameEl.querySelector('a');
                            if (name && name.length > 1 && !/^\\d+$/.test(name)) {{
                                results.push({{ rank: rank, name: name, url: link ? link.href : '' }});
                            }}
                        }}
                    }});
                    return results;
                }}
            """)

            for i, r in enumerate(rows[:limit]):
                rank_str = str(r.get('rank', i + 1))
                rank_val = int(rank_str) if rank_str.isdigit() else i + 1
                companies.append({
                    'rank': rank_val,
                    'name': r.get('name', '').strip(),
                    'ticker': '',
                    'url': r.get('url', ''),
                })

        finally:
            browser.close()

    if not companies:
        raise Exception(
            f'Fortune scraper returned no data from {url}. '
            'The page structure may have changed.'
        )

    return companies[:limit]


# ---------------------------------------------------------------------------
# Forbes Global 2000 scraper
# ---------------------------------------------------------------------------

def scrape_forbes(url: str) -> List[dict]:
    """
    Scrape the Forbes Global 2000 ranking from forbes.com.

    First tries Forbes' internal forbesapi (fast, no browser).
    Falls back to Playwright if the API is unavailable.

    Returns a list of dicts: {rank, name, ticker, url}
    """
    # Try the internal Forbes API first
    current_year = 2025  # Update annually or derive dynamically
    api_url = (
        f'https://www.forbes.com/forbesapi/org/global2000/{current_year}'
        '/position/true.json?fields=rank,organizationName,publicStatus,uri&limit=2000'
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
            companies = []
            for item in items:
                name = (item.get('organizationName') or item.get('name') or '').strip()
                rank_val = item.get('rank') or len(companies) + 1
                uri = item.get('uri') or ''
                company_url = f'https://www.forbes.com{uri}' if uri.startswith('/') else uri
                if name:
                    companies.append({
                        'rank': int(rank_val) if str(rank_val).isdigit() else len(companies) + 1,
                        'name': name,
                        'ticker': '',
                        'url': company_url,
                    })
            if companies:
                return companies
    except Exception:
        pass  # Fall through to Playwright

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
                page.wait_for_selector('table tr, [class*="listItem"], [class*="row"]', timeout=15000)
            except PlaywrightTimeout:
                pass

            page.wait_for_timeout(3000)

            rows = page.evaluate("""
                () => {
                    const results = [];
                    const rows = document.querySelectorAll('table tr, [class*="listItem"], [class*="rankRow"]');
                    rows.forEach((row, idx) => {
                        const nameEl = row.querySelector('[class*="name"], [class*="org"], td:nth-child(2), a');
                        if (nameEl) {
                            const name = nameEl.textContent.trim();
                            const link = nameEl.closest('a') || nameEl.querySelector('a');
                            if (name && name.length > 1 && !/^\\d+$/.test(name)) {
                                results.push({ rank: idx + 1, name: name, url: link ? link.href : '' });
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
                })

        finally:
            browser.close()

    if not companies:
        raise Exception(
            f'Forbes scraper returned no data from {url}. '
            'The page structure may have changed.'
        )

    return companies


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def scrape_index(index_id: str, config: dict) -> List[dict]:
    """
    Route to the correct scraper based on config['source'].

    Returns a list of company dicts: {rank, name, ticker, url}
    Raises Exception on failure (callers should catch and report to UI).
    """
    source = config.get('source')

    if source == 'manual' or config.get('manual'):
        raise Exception(
            f'Index "{config.get("name", index_id)}" requires manual CSV upload. '
            'Use the Upload CSV button to populate this index.'
        )

    if source == 'tradingview':
        return scrape_tradingview(config['url'])

    if source == 'wikipedia':
        return scrape_wikipedia(config['url'], config.get('table_index', 0))

    if source == 'marketscreener':
        return scrape_marketscreener(config['url'])

    if source == 'euronext':
        return scrape_euronext(config['url'])

    if source == 'ishares':
        return scrape_ishares(config['url'])

    if source == 'fortune':
        return scrape_fortune(config['url'], config.get('limit', 500))

    if source == 'forbes':
        return scrape_forbes(config['url'])

    raise Exception(f'Unknown source "{source}" for index "{index_id}".')
