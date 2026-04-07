"""
app.py — Flask backend for the Index Tracker internal tool.

Routes:
  GET  /                     Serve the main UI
  GET  /api/indices           List all indices with status metadata
  GET  /api/indices/<id>      Return stored companies for one index
  POST /api/refresh/<id>      Scrape + store one index (auto-scraped only)
  POST /api/upload/<id>       CSV upload for manual indices
  GET  /api/export/csv        Download full multi-index CSV export
  GET  /api/export/csv/<id>   Download single-index CSV export
"""

from flask import (
    Flask,
    render_template,
    jsonify,
    request,
    make_response,
    send_from_directory,
)
import json
import os
import csv
import io
import re
from datetime import datetime
from typing import Optional, Dict

from scrapers import scrape_index, INDICES_CONFIG

app = Flask(__name__)
app.wsgi_app = __import__('whitenoise').WhiteNoise(app.wsgi_app, root='static/', prefix='static')

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
ENRICHMENT_DIR = os.path.join(DATA_DIR, 'enrichment')
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(ENRICHMENT_DIR, exist_ok=True)

# Human-readable labels for each scraper source type.
# Keep this in sync with scrapers.py when adding new source types.
SOURCE_LABELS = {
    'wikipedia':         'Wikipedia',
    'wikipedia_navbox':  'Wikipedia (navbox)',
    'ishares':           'iShares ETF CSV',
    'nasdaq':            'Nasdaq API',
    'fortune':           'us500.com / fortune.com',
    'forbes':            'Forbes API',
    'investing':         'investing.com',
    'derived':           'Derived (combined)',
    'marketscreener':    'MarketScreener',
}


# ---------------------------------------------------------------------------
# JSON storage helpers
# ---------------------------------------------------------------------------

def load_index_data(index_id: str) -> Optional[dict]:
    """Load stored index data from disk. Returns None if not yet scraped."""
    path = os.path.join(DATA_DIR, f'{index_id}.json')
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return None


def load_index_meta(index_id: str) -> Optional[dict]:
    """
    Load only top-level metadata (everything except 'companies') for sidebar/summary use.
    Avoids parsing thousands of company records on every page load.
    """
    path = os.path.join(DATA_DIR, f'{index_id}.json')
    if not os.path.exists(path):
        return None
    # Stream-parse just enough of the file to extract scalar fields,
    # stopping before the (potentially huge) 'companies' array.
    # Fallback: full json.load if fast path fails.
    try:
        with open(path) as f:
            # Read up to 2 KB — enough to capture id/name/count/last_updated
            # when those fields are stored before 'companies' (current format).
            head = f.read(2048)
        cut = head.find('"companies"')
        if cut == -1:
            with open(path) as f:
                return json.load(f)
        snippet = head[:cut].rstrip().rstrip(',') + '}'
        meta = json.loads(snippet)
        if 'last_updated' not in meta:
            # Old file format: last_updated stored after companies — full load needed.
            with open(path) as f:
                full = json.load(f)
            return {k: v for k, v in full.items() if k != 'companies'}
        return meta
    except Exception:
        # Fallback: full load
        with open(path) as f:
            return json.load(f)


def save_index_data(index_id: str, data: dict) -> None:
    """Persist index data to a JSON file."""
    path = os.path.join(DATA_DIR, f'{index_id}.json')
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# Enrichment helpers
# ---------------------------------------------------------------------------

def _norm(s: str) -> str:
    """Normalise a company name for fuzzy matching."""
    return re.sub(r'[^a-z0-9]', '', s.lower())


def load_enrichment(index_id: str) -> dict:
    """
    Load the enrichment store for an index.
    Returns a dict keyed by normalised company name:
      { norm_name: {kernel_id, legal_name, account_url} }
    """
    path = os.path.join(ENRICHMENT_DIR, f'{index_id}.json')
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)


def save_enrichment(index_id: str, store: dict) -> None:
    path = os.path.join(ENRICHMENT_DIR, f'{index_id}.json')
    with open(path, 'w') as f:
        json.dump(store, f, indent=2)


def apply_enrichment(index_id: str, companies: list) -> list:
    """
    Merge enrichment data into a company list in-place.
    Only fills fields that are currently empty — never overwrites existing values.
    Returns the same list (mutated).
    """
    store = load_enrichment(index_id)
    if not store:
        return companies
    for c in companies:
        key = _norm(c.get('name', ''))
        match = store.get(key)
        if not match:
            continue
        if match.get('kernel_id') and not c.get('kernel_id'):
            c['kernel_id'] = match['kernel_id']
        if match.get('legal_name') and not c.get('legal_name'):
            c['legal_name'] = match['legal_name']
        if match.get('account_url') and not c.get('url'):
            c['url'] = match['account_url']
    return companies


def get_index_summary(index_id: str) -> dict:
    """
    Return a summary dict for the sidebar, combining config and stored data.
    """
    config = INDICES_CONFIG[index_id]
    stored = load_index_meta(index_id)

    # count/last_updated come from pre-computed top-level fields —
    # no need to load the full companies array for sidebar metadata
    count = stored.get('count', 0) if stored else 0
    last_updated = stored.get('last_updated') if stored else None

    return {
        'id': index_id,
        'name': config['name'],
        'source': config.get('source', 'unknown'),
        'manual': bool(config.get('manual')),
        'notes': config.get('notes', ''),
        'count': count,
        'last_updated': last_updated,
        'has_data': count > 0,
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route('/')
def index():
    """Render the main SPA, injecting sidebar metadata into the template."""
    indices = [get_index_summary(iid) for iid in INDICES_CONFIG]
    indices_meta = {
        iid: {
            'expected_count': cfg.get('expected_count'),
            'data_notes': cfg.get('data_notes', ''),
            'source': cfg.get('source', ''),
            'rebalance_schedule': cfg.get('rebalance_schedule', ''),
            'stale_days': cfg.get('stale_days'),
            'alt_source': cfg.get('alt_source', ''),
        }
        for iid, cfg in INDICES_CONFIG.items()
    }
    return render_template('index.html', indices=indices, indices_config=INDICES_CONFIG, indices_meta=indices_meta, source_labels=SOURCE_LABELS)


@app.route('/api/indices')
def api_indices():
    """Return JSON list of all indices with their status."""
    indices = [get_index_summary(iid) for iid in INDICES_CONFIG]
    return jsonify(indices)


@app.route('/api/indices/<index_id>')
def api_index_detail(index_id: str):
    """Return the stored companies for one index."""
    if index_id not in INDICES_CONFIG:
        return jsonify({'error': f'Unknown index: {index_id}'}), 404

    stored = load_index_data(index_id)
    if not stored:
        return jsonify({
            'id': index_id,
            'name': INDICES_CONFIG[index_id]['name'],
            'companies': [],
            'last_updated': None,
            'count': 0,
        })

    return jsonify({
        'id': index_id,
        'name': INDICES_CONFIG[index_id]['name'],
        'companies': stored.get('companies', []),
        'last_updated': stored.get('last_updated'),
        'count': len(stored.get('companies', [])),
        'error': stored.get('error'),
    })


@app.route('/api/refresh/<index_id>', methods=['POST'])
def api_refresh(index_id: str):
    """
    Trigger a fresh scrape for one auto-scraped index.
    Manual indices should use /api/upload/<id> instead.
    """
    if index_id not in INDICES_CONFIG:
        return jsonify({'error': f'Unknown index: {index_id}'}), 404

    config = INDICES_CONFIG[index_id]

    if config.get('manual'):
        return jsonify({
            'error': (
                f'{config["name"]} requires manual CSV upload. '
                'Use the Upload CSV button.'
            )
        }), 400

    try:
        companies = scrape_index(index_id, config)
        apply_enrichment(index_id, companies)
        data = {
            'id': index_id,
            'name': config['name'],
            'last_updated': datetime.utcnow().isoformat() + 'Z',
            'count': len(companies),
            'companies': companies,
        }
        save_index_data(index_id, data)

        # Cross-validate against alt source if configured
        from scrapers import validate_against_alt
        validation = validate_against_alt(index_id, config, companies)
        if validation:
            data['validation'] = validation
            save_index_data(index_id, data)

        return jsonify({
            'success': True,
            'count': len(companies),
            'last_updated': data['last_updated'],
            'validation': data.get('validation', {}),
        })
    except Exception as exc:
        # Store the error so the UI can show it
        error_data = {
            'id': index_id,
            'name': config['name'],
            'last_updated': datetime.utcnow().isoformat() + 'Z',
            'count': 0,
            'error': str(exc),
            'companies': [],
        }
        save_index_data(index_id, error_data)
        return jsonify({'error': str(exc)}), 500


@app.route('/api/refresh-all', methods=['POST'])
def api_refresh_all():
    """
    Refresh all auto-scraped (non-manual) indices.
    Returns a JSON summary of successes and failures.
    """
    results = []
    for index_id, config in INDICES_CONFIG.items():
        if config.get('manual'):
            continue
        try:
            companies = scrape_index(index_id, config)
            apply_enrichment(index_id, companies)
            data = {
                'id': index_id,
                'name': config['name'],
                'last_updated': datetime.utcnow().isoformat() + 'Z',
                'count': len(companies),
                'companies': companies,
            }
            save_index_data(index_id, data)
            results.append({'id': index_id, 'name': config['name'], 'success': True, 'count': len(companies)})
        except Exception as exc:
            results.append({'id': index_id, 'name': config['name'], 'success': False, 'error': str(exc)})
    return jsonify({'results': results})


@app.route('/api/upload/<index_id>', methods=['POST'])
def api_upload(index_id: str):
    """
    Accept a CSV file upload for a manual (or any) index.

    Expected CSV columns (case-insensitive, flexible order):
      name      — company name (required)
      ticker    — ticker symbol (optional)
      url       — website URL (optional)
      rank      — position in index (optional, defaults to row order)
    """
    if index_id not in INDICES_CONFIG:
        return jsonify({'error': f'Unknown index: {index_id}'}), 404

    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded. Send as multipart/form-data with key "file".'}), 400

    file = request.files['file']
    if not file.filename:
        return jsonify({'error': 'Empty filename.'}), 400

    # Decode the uploaded bytes as UTF-8 (with BOM support)
    try:
        content = file.read().decode('utf-8-sig')
    except UnicodeDecodeError:
        try:
            file.seek(0)
            content = file.read().decode('latin-1')
        except Exception as e:
            return jsonify({'error': f'Could not decode file: {e}'}), 400

    reader = csv.DictReader(io.StringIO(content))

    # Normalise column names to lowercase
    if reader.fieldnames is None:
        return jsonify({'error': 'CSV file appears to be empty.'}), 400

    fieldnames_lower = {f: f.lower().strip() for f in reader.fieldnames}

    # Find the "name" column (required)
    name_key = next(
        (k for k, v in fieldnames_lower.items()
         if v in ('name', 'company', 'company name', 'issuer')),
        None,
    )
    if not name_key:
        return jsonify({
            'error': (
                'CSV must have a "name" column. '
                f'Found columns: {list(reader.fieldnames)}'
            )
        }), 400

    ticker_key = next(
        (k for k, v in fieldnames_lower.items()
         if v in ('ticker', 'symbol', 'code')),
        None,
    )
    url_key = next(
        (k for k, v in fieldnames_lower.items()
         if v in ('url', 'website', 'homepage', 'domain', 'account_url')),
        None,
    )
    rank_key = next(
        (k for k, v in fieldnames_lower.items()
         if v in ('rank', 'position', '#', 'no', 'no.')),
        None,
    )
    country_key = next(
        (k for k, v in fieldnames_lower.items()
         if v in ('country', 'country of incorporation', 'incorporation', 'location')),
        None,
    )

    companies = []
    for row_num, row in enumerate(reader, start=1):
        name = row.get(name_key, '').strip()
        if not name:
            continue

        rank_val = row_num
        if rank_key:
            try:
                rank_val = int(row[rank_key])
            except (ValueError, TypeError):
                rank_val = row_num

        companies.append({
            'rank': rank_val,
            'name': name,
            'ticker': row.get(ticker_key, '').strip() if ticker_key else '',
            'url': row.get(url_key, '').strip() if url_key else '',
            'country': row.get(country_key, '').strip() if country_key else '',
        })

    if not companies:
        return jsonify({'error': 'No valid rows found in CSV.'}), 400

    apply_enrichment(index_id, companies)
    config = INDICES_CONFIG[index_id]
    data = {
        'id': index_id,
        'name': config['name'],
        'last_updated': datetime.utcnow().isoformat() + 'Z',
        'count': len(companies),
        'source': 'upload',
        'companies': companies,
    }
    save_index_data(index_id, data)

    return jsonify({
        'success': True,
        'count': len(companies),
        'last_updated': data['last_updated'],
    })


@app.route('/api/enrich/<index_id>', methods=['POST'])
def api_enrich(index_id: str):
    """
    Upload a CSV of enrichment data (kernel_id, legal_name, account_url) for any index.
    Matches companies by name. Accepted name column headers (case-insensitive):
      name, original_name, crm original name, crm company name, company, issuer
    Never overwrites existing non-empty enrichment values.
    Re-applies the updated enrichment store to the live data immediately.
    """
    if index_id not in INDICES_CONFIG:
        return jsonify({'error': f'Unknown index: {index_id}'}), 404

    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded.'}), 400

    file = request.files['file']
    try:
        content = file.read().decode('utf-8-sig')
    except UnicodeDecodeError:
        file.seek(0)
        content = file.read().decode('latin-1')

    reader = csv.DictReader(io.StringIO(content))
    if not reader.fieldnames:
        return jsonify({'error': 'CSV appears empty.'}), 400

    fl = {f: f.lower().strip() for f in reader.fieldnames}

    name_key = next(
        (k for k, v in fl.items() if v in (
            'name', 'original_name', 'crm original name',
            'crm company name', 'company', 'issuer',
        )), None
    )
    if not name_key:
        return jsonify({'error': f'No name column found. Columns: {list(reader.fieldnames)}'}), 400

    kernel_key  = next((k for k, v in fl.items() if v in ('kernel_id', 'kernel id')), None)
    legal_key   = next((k for k, v in fl.items() if v in ('legal_name', 'legal name')), None)
    url_key     = next((k for k, v in fl.items() if v in (
        'account_url', 'account url', 'url', 'website', 'homepage', 'domain',
    )), None)

    if not any([kernel_key, legal_key, url_key]):
        return jsonify({'error': 'CSV must have at least one of: kernel_id, legal_name, account_url'}), 400

    # Load existing enrichment store and merge (never overwrite populated values)
    store = load_enrichment(index_id)
    added = updated = 0

    for row in reader:
        raw_name = (row.get(name_key) or '').strip()
        if not raw_name:
            continue
        key = _norm(raw_name)
        entry = store.get(key, {})
        is_new = key not in store

        if kernel_key:
            v = (row.get(kernel_key) or '').strip()
            if v and not entry.get('kernel_id'):
                entry['kernel_id'] = v
        if legal_key:
            v = (row.get(legal_key) or '').strip()
            if v and not entry.get('legal_name'):
                entry['legal_name'] = v
        if url_key:
            v = (row.get(url_key) or '').strip()
            if v and not entry.get('account_url'):
                entry['account_url'] = v

        if entry:
            if is_new:
                added += 1
            else:
                updated += 1
            store[key] = entry

    save_enrichment(index_id, store)

    # Re-apply to live data immediately
    stored = load_index_data(index_id)
    matched = 0
    if stored and stored.get('companies'):
        apply_enrichment(index_id, stored['companies'])
        matched = sum(1 for c in stored['companies'] if c.get('kernel_id') or c.get('legal_name'))
        save_index_data(index_id, stored)

    return jsonify({
        'success': True,
        'store_entries': len(store),
        'new': added,
        'updated': updated,
        'matched_in_index': matched,
    })


@app.route('/api/export/csv')
def api_export_all():
    """
    Export all indices as a single CSV.

    Columns: original_name, account_url, <field1>, <field2>, ...

    Each unique company (keyed by URL if available, else name) appears once.
    Companies present in multiple indices get TRUE in each relevant column.
    Only companies with at least one TRUE are included.
    """
    # Collect all field keys in config order
    all_fields = list(INDICES_CONFIG.keys())

    # Build a master company map: key → {name, url, country, field_values, ranks}
    # Key is normalised URL (if present) or lowercased name
    company_map: Dict[str, dict] = {}

    for field_id in all_fields:
        stored = load_index_data(field_id)
        if not stored:
            continue
        for company in stored.get('companies', []):
            url = (company.get('url') or '').strip()
            name = (company.get('name') or '').strip()
            country = (company.get('country') or '').strip()
            if not name:
                continue
            key = url.lower() if url else name.lower()

            if key not in company_map:
                company_map[key] = {
                    'original_name': name,
                    'account_url': url,
                    'country': country,
                    'kernel_id': company.get('kernel_id', ''),
                    'legal_name': company.get('legal_name', ''),
                    'fields': {f: False for f in all_fields},
                    'ranks': {f: '' for f in all_fields},
                }

            company_map[key]['fields'][field_id] = True
            company_map[key]['ranks'][field_id] = company.get('rank', '')
            # Prefer entries that have more data
            if url and not company_map[key]['account_url']:
                company_map[key]['account_url'] = url
            if country and not company_map[key]['country']:
                company_map[key]['country'] = country
            if company.get('kernel_id') and not company_map[key]['kernel_id']:
                company_map[key]['kernel_id'] = company['kernel_id']
            if company.get('legal_name') and not company_map[key]['legal_name']:
                company_map[key]['legal_name'] = company['legal_name']

    # Determine if any company has kernel enrichment data
    has_kernel = any(e.get('kernel_id') for e in company_map.values())

    # Build CSV — for each index: a TRUE/FALSE column then a _rank column
    output = io.StringIO()
    writer = csv.writer(output)

    header = ['original_name', 'account_url', 'crm_country']
    if has_kernel:
        header += ['kernel_id', 'legal_name']
    for f in all_fields:
        header.append(f)
        header.append(f'{f}_rank')
    writer.writerow(header)

    for entry in sorted(company_map.values(), key=lambda x: x['original_name'].lower()):
        row = [entry['original_name'], entry['account_url'], entry['country']]
        if has_kernel:
            row += [entry.get('kernel_id', ''), entry.get('legal_name', '')]
        for f in all_fields:
            row.append(str(entry['fields'][f]).upper())
            row.append(entry['ranks'][f])
        writer.writerow(row)

    response = make_response(output.getvalue())
    response.headers['Content-Type'] = 'text/csv; charset=utf-8'
    response.headers['Content-Disposition'] = (
        f'attachment; filename="index_tracker_export_{_today()}.csv"'
    )
    return response


@app.route('/api/export/csv/<index_id>')
def api_export_single(index_id: str):
    """
    Export one index as a CSV.

    Columns: original_name, account_url, rank, <field_name>
    """
    if index_id not in INDICES_CONFIG:
        return jsonify({'error': f'Unknown index: {index_id}'}), 404

    config = INDICES_CONFIG[index_id]
    stored = load_index_data(index_id)

    if not stored or not stored.get('companies'):
        return jsonify({'error': 'No data available for this index.'}), 404

    companies = stored['companies']
    has_kernel = any(c.get('kernel_id') for c in companies)

    output = io.StringIO()
    writer = csv.writer(output)

    header = ['original_name', 'account_url', 'crm_country']
    if has_kernel:
        header += ['kernel_id', 'legal_name']
    header += [index_id, f'{index_id}_rank']
    writer.writerow(header)

    for company in companies:
        row = [
            company.get('name', ''),
            company.get('url', ''),
            company.get('country', ''),
        ]
        if has_kernel:
            row += [company.get('kernel_id', ''), company.get('legal_name', '')]
        row += ['TRUE', company.get('rank', '')]
        writer.writerow(row)

    safe_name = config['name'].replace(' ', '_').replace('/', '-')
    response = make_response(output.getvalue())
    response.headers['Content-Type'] = 'text/csv; charset=utf-8'
    response.headers['Content-Disposition'] = (
        f'attachment; filename="{safe_name}_{_today()}.csv"'
    )
    return response


def _today() -> str:
    return datetime.utcnow().strftime('%Y-%m-%d')


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
