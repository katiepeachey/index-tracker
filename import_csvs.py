"""
import_csvs.py — Import index constituent data from exported CSV files.

Saves parsed data to data/snapshots/ as reference baselines for QA comparison.
Does NOT overwrite live data in data/ — use refresh.py for live data.

Supported formats
-----------------
stoxx     STOXX index export (e.g. STOXX Global 1800)
          Columns: Name, Symbol (TICKER-REGION), % Index Weight, ...
          Header rows at top; footer rows at bottom — auto-detected.
          Symbol format: CTAS-US → ticker=CTAS, country=United States

bloomberg Bloomberg terminal export
          Columns: Ticker, Name, Price, Net Chg, % Chg, Volume
          Ticker format: "9984 JT Equity" → ticker=9984, country=Japan

generic   Any CSV with a "Name" column (and optional Ticker, Country columns)
          Use for ad-hoc imports when format doesn't match stoxx/bloomberg.

Usage
-----
  # Auto-detect format from column headers:
  python import_csvs.py path/to/file.csv --index stoxx_global_1800

  # Explicitly specify format:
  python import_csvs.py path/to/file.csv --index s_p_global_1200 --format bloomberg

  # Multiple files at once:
  python import_csvs.py stoxx.csv --index stoxx_global_1800 \\
                        spg.csv   --index s_p_global_1200 --format bloomberg

  # List available index IDs:
  python import_csvs.py --list
"""

import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))

SNAPSHOTS_DIR = os.path.join(os.path.dirname(__file__), 'data', 'snapshots')
os.makedirs(SNAPSHOTS_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# Country mappings
# ---------------------------------------------------------------------------

STOXX_REGION_TO_COUNTRY = {
    'AT': 'Austria',       'AU': 'Australia',     'BE': 'Belgium',
    'BR': 'Brazil',        'CA': 'Canada',        'CH': 'Switzerland',
    'CN': 'China',         'CZ': 'Czech Republic','DE': 'Germany',
    'DK': 'Denmark',       'ES': 'Spain',         'FI': 'Finland',
    'FR': 'France',        'GB': 'United Kingdom','GR': 'Greece',
    'HK': 'Hong Kong',     'HU': 'Hungary',       'IE': 'Ireland',
    'IL': 'Israel',        'IN': 'India',         'IT': 'Italy',
    'JP': 'Japan',         'KR': 'South Korea',   'LU': 'Luxembourg',
    'MX': 'Mexico',        'NL': 'Netherlands',   'NO': 'Norway',
    'NZ': 'New Zealand',   'PL': 'Poland',        'PT': 'Portugal',
    'SE': 'Sweden',        'SG': 'Singapore',     'TW': 'Taiwan',
    'US': 'United States', 'ZA': 'South Africa',
}

BLOOMBERG_EXCHANGE_TO_COUNTRY = {
    'UN': 'United States',   # NYSE
    'UW': 'United States',   # Nasdaq Global Select
    'UQ': 'United States',   # Nasdaq Global Market
    'UF': 'United States',   # NYSE American
    'US': 'United States',   # Composite
    'UA': 'United States',   # NYSE ARCA
    'JT': 'Japan',           # Tokyo Stock Exchange
    'LN': 'United Kingdom',  # London Stock Exchange
    'CT': 'Canada',          # Toronto Stock Exchange
    'AT': 'Australia',       # Australian Securities Exchange
    'FP': 'France',          # Euronext Paris
    'GY': 'Germany',         # Xetra / Deutsche Börse
    'SE': 'Switzerland',     # SIX Swiss Exchange
    'SS': 'Sweden',          # Nasdaq Stockholm
    'KP': 'South Korea',     # Korea Exchange
    'SM': 'Spain',           # Bolsa de Madrid
    'IM': 'Italy',           # Borsa Italiana
    'NA': 'Netherlands',     # Euronext Amsterdam
    'BB': 'Belgium',         # Euronext Brussels
    'DC': 'Denmark',         # Nasdaq Copenhagen
    'HB': 'Finland',         # Nasdaq Helsinki
    'NO': 'Norway',          # Oslo Stock Exchange
    'PL': 'Portugal',        # Euronext Lisbon
    'SJ': 'South Africa',    # Johannesburg Stock Exchange
    'HK': 'Hong Kong',       # Hong Kong Exchange
    'SI': 'Singapore',       # Singapore Exchange
    'SP': 'Singapore',       # Singapore Exchange (alt)
    'SQ': 'Singapore',       # Singapore Exchange (alt)
    'TW': 'Taiwan',          # Taiwan Stock Exchange
    'TT': 'Taiwan',          # Taipei Exchange (OTC)
    'BS': 'Brazil',          # B3
    'BZ': 'Brazil',          # B3 (alt)
    'CC': 'China',           # Shanghai / Shenzhen
    'CH': 'China',           # Shanghai (alt)
    'MF': 'Mexico',          # Mexican Stock Exchange
    'CX': 'Canada',          # Canadian Securities Exchange
    'CF': 'Canada',          # Toronto Venture Exchange
    'VX': 'Switzerland',     # SIX Swiss (alt)
    'AV': 'Austria',         # Vienna Stock Exchange
    'SW': 'Switzerland',     # SIX (alt)
    'PW': 'Poland',          # Warsaw Stock Exchange
    'GA': 'Greece',          # Athens Stock Exchange
}


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def _detect_format(csv_path: str) -> str:
    """
    Inspect the first few rows of a CSV to detect its format.
    Returns 'stoxx', 'bloomberg', or 'generic'.
    """
    with open(csv_path, encoding='utf-8-sig') as f:
        head = [f.readline() for _ in range(10)]
    joined = ' '.join(head).lower()

    if 'symbol' in joined and re.search(r'[A-Z]{2,6}-[A-Z]{2}\b', ' '.join(head)):
        return 'stoxx'
    if 'ticker' in joined and 'equity' in joined:
        return 'bloomberg'
    if 'ticker' in joined or 'name' in joined:
        return 'generic'
    return 'generic'


def parse_stoxx(csv_path: str) -> list:
    """
    Parse a STOXX index CSV export.
    Symbol format: TICKER-REGION (e.g. CTAS-US, BNR-DE, ABB-CH).
    Auto-detects and skips header metadata rows and footer rows.
    """
    with open(csv_path, encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        rows = list(reader)

    # Find the data header row (first row with 'Name' in column 0 and 'Symbol' in col 1)
    header_idx = next(
        (i for i, row in enumerate(rows)
         if row and row[0].strip() == 'Name' and len(row) > 1 and row[1].strip() == 'Symbol'),
        None,
    )
    if header_idx is None:
        raise ValueError(
            'Could not find header row (expected "Name" in col A, "Symbol" in col B). '
            'Is this a STOXX export?'
        )

    # Row immediately after header is the index-level summary (e.g. "STOXX Global 1800, SX001899, 100.00,...")
    # Skip it by starting at header_idx + 2
    companies = []
    rank = 0
    for row in rows[header_idx + 2:]:
        if not row or not row[0].strip():
            continue
        name = row[0].strip().strip('"')
        symbol = row[1].strip() if len(row) > 1 else ''

        # Stop at footer rows
        if any(name.startswith(prefix) for prefix in
               ('Data as of', 'Constituents', 'Price', 'Not Grouped')):
            continue
        # Skip any leftover index-level summary rows (identified by having no dash in symbol)
        if symbol and '-' not in symbol and len(symbol) <= 10:
            continue

        rank += 1
        ticker, country = '', ''
        if symbol and symbol != '-':
            parts = symbol.rsplit('-', 1)
            if len(parts) == 2:
                ticker = parts[0].strip()
                country = STOXX_REGION_TO_COUNTRY.get(parts[1].strip().upper(), parts[1].strip())

        companies.append({'rank': rank, 'name': name, 'ticker': ticker, 'url': '', 'country': country})

    return companies


def parse_bloomberg(csv_path: str) -> list:
    """
    Parse a Bloomberg terminal export.
    Ticker format: "TICKER EXCHANGE Equity" (e.g. "9984 JT Equity").
    Company names are often truncated to 30 characters.
    """
    with open(csv_path, encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        companies = []
        for rank, row in enumerate(reader, start=1):
            raw_ticker = (row.get('Ticker') or '').strip()
            name = (row.get('Name') or '').strip()
            if not name or not raw_ticker:
                continue
            ticker, country = '', ''
            parts = raw_ticker.split()
            if len(parts) >= 2:
                ticker = parts[0]
                country = BLOOMBERG_EXCHANGE_TO_COUNTRY.get(parts[1].upper(), '')
            companies.append({'rank': rank, 'name': name, 'ticker': ticker, 'url': '', 'country': country})
    return companies


def parse_generic(csv_path: str) -> list:
    """
    Parse any CSV with at minimum a "Name" column.
    Optionally reads: Ticker/Symbol, Country, Rank/Position.
    Column names are matched case-insensitively.
    """
    with open(csv_path, encoding='utf-8-sig') as f:
        content = f.read()

    reader = csv.DictReader(__import__('io').StringIO(content))
    if not reader.fieldnames:
        raise ValueError('CSV file appears to be empty or has no headers.')

    fl = {f: f.lower().strip() for f in reader.fieldnames}

    name_key = next((k for k, v in fl.items() if v in ('name', 'company', 'company name', 'issuer')), None)
    if not name_key:
        raise ValueError(f'No "Name" column found. Columns: {list(reader.fieldnames)}')

    ticker_key  = next((k for k, v in fl.items() if v in ('ticker', 'symbol', 'code')), None)
    country_key = next((k for k, v in fl.items() if v in ('country', 'country of incorporation', 'location')), None)
    rank_key    = next((k for k, v in fl.items() if v in ('rank', 'position', '#', 'no', 'no.')), None)

    companies = []
    for row_num, row in enumerate(reader, start=1):
        name = (row.get(name_key) or '').strip()
        if not name:
            continue
        rank_val = row_num
        if rank_key:
            try:
                rank_val = int(row[rank_key])
            except (ValueError, TypeError):
                pass
        companies.append({
            'rank': rank_val,
            'name': name,
            'ticker': (row.get(ticker_key) or '').strip() if ticker_key else '',
            'url': '',
            'country': (row.get(country_key) or '').strip() if country_key else '',
        })
    return companies


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------

def save_snapshot(index_id: str, index_name: str, companies: list, snapshot_date: str):
    """
    Save parsed companies to data/snapshots/<index_id>_<YYYYMMDD>.json.
    Snapshots are reference baselines — they do NOT affect live data.
    """
    date_tag = snapshot_date[:10].replace('-', '')
    filename = f'{index_id}_{date_tag}.json'
    path = os.path.join(SNAPSHOTS_DIR, filename)
    data = {
        'id': index_id,
        'name': index_name,
        'companies': companies,
        'snapshot_date': snapshot_date,
        'count': len(companies),
        'source': 'csv_import',
    }
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)
    print(f'  Saved {len(companies):,} companies → data/snapshots/{filename}')


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Import index CSVs as QA reference snapshots.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument('csv', nargs='?', help='Path to the CSV file to import.')
    parser.add_argument('--index', required=False, help='Index ID to associate with this file (e.g. stoxx_global_1800).')
    parser.add_argument('--format', choices=['stoxx', 'bloomberg', 'generic', 'auto'],
                        default='auto', help='CSV format (default: auto-detect).')
    parser.add_argument('--date', default=None,
                        help='Snapshot date in YYYY-MM-DD format (default: today).')
    parser.add_argument('--list', action='store_true', help='List available index IDs and exit.')

    args = parser.parse_args()

    if args.list:
        from scrapers import INDICES_CONFIG
        print(f'{"ID":<25} Name')
        print('-' * 60)
        for iid, cfg in INDICES_CONFIG.items():
            print(f'{iid:<25} {cfg["name"]}')
        return 0

    if not args.csv:
        parser.print_help()
        return 1

    if not args.index:
        print('Error: --index is required. Run --list to see available index IDs.')
        return 1

    csv_path = os.path.expanduser(args.csv)
    if not os.path.exists(csv_path):
        print(f'Error: file not found: {csv_path}')
        return 1

    # Resolve index name from config
    try:
        from scrapers import INDICES_CONFIG
        index_name = INDICES_CONFIG[args.index]['name']
    except KeyError:
        print(f'Error: unknown index ID "{args.index}". Run --list to see available IDs.')
        return 1

    # Auto-detect or use specified format
    fmt = args.format
    if fmt == 'auto':
        fmt = _detect_format(csv_path)
        print(f'  Detected format: {fmt}')

    snapshot_date = args.date or datetime.utcnow().strftime('%Y-%m-%d')

    print(f'Importing {index_name} from {os.path.basename(csv_path)} ({fmt} format)...')

    try:
        if fmt == 'stoxx':
            companies = parse_stoxx(csv_path)
        elif fmt == 'bloomberg':
            companies = parse_bloomberg(csv_path)
        else:
            companies = parse_generic(csv_path)
    except Exception as e:
        print(f'Error parsing CSV: {e}')
        return 1

    print(f'  Parsed {len(companies):,} companies')

    # Show country breakdown
    from collections import Counter
    top = Counter(c['country'] for c in companies).most_common(5)
    print(f'  Top countries: {", ".join(f"{c} ({n})" for c, n in top)}')

    save_snapshot(args.index, index_name, companies, snapshot_date)
    return 0


if __name__ == '__main__':
    sys.exit(main())
