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
from datetime import datetime
from typing import Optional, Dict

from scrapers import scrape_index, INDICES_CONFIG

app = Flask(__name__)

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
os.makedirs(DATA_DIR, exist_ok=True)


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


def save_index_data(index_id: str, data: dict) -> None:
    """Persist index data to a JSON file."""
    path = os.path.join(DATA_DIR, f'{index_id}.json')
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)


def get_index_summary(index_id: str) -> dict:
    """
    Return a summary dict for the sidebar, combining config and stored data.
    """
    config = INDICES_CONFIG[index_id]
    stored = load_index_data(index_id)

    companies = stored.get('companies', []) if stored else []
    last_updated = stored.get('last_updated') if stored else None

    return {
        'id': index_id,
        'name': config['name'],
        'source': config.get('source', 'unknown'),
        'manual': bool(config.get('manual')),
        'notes': config.get('notes', ''),
        'count': len(companies),
        'last_updated': last_updated,
        'has_data': bool(companies),
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route('/')
def index():
    """Render the main SPA, injecting sidebar metadata into the template."""
    indices = [get_index_summary(iid) for iid in INDICES_CONFIG]
    return render_template('index.html', indices=indices, indices_config=INDICES_CONFIG)


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
        data = {
            'id': index_id,
            'name': config['name'],
            'companies': companies,
            'last_updated': datetime.utcnow().isoformat() + 'Z',
            'count': len(companies),
        }
        save_index_data(index_id, data)
        return jsonify({
            'success': True,
            'count': len(companies),
            'last_updated': data['last_updated'],
        })
    except Exception as exc:
        # Store the error so the UI can show it
        error_data = {
            'id': index_id,
            'name': config['name'],
            'companies': [],
            'last_updated': datetime.utcnow().isoformat() + 'Z',
            'count': 0,
            'error': str(exc),
        }
        save_index_data(index_id, error_data)
        return jsonify({'error': str(exc)}), 500


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
         if v in ('url', 'website', 'homepage', 'domain')),
        None,
    )
    rank_key = next(
        (k for k, v in fieldnames_lower.items()
         if v in ('rank', 'position', '#', 'no', 'no.')),
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
        })

    if not companies:
        return jsonify({'error': 'No valid rows found in CSV.'}), 400

    config = INDICES_CONFIG[index_id]
    data = {
        'id': index_id,
        'name': config['name'],
        'companies': companies,
        'last_updated': datetime.utcnow().isoformat() + 'Z',
        'count': len(companies),
        'source': 'upload',
    }
    save_index_data(index_id, data)

    return jsonify({
        'success': True,
        'count': len(companies),
        'last_updated': data['last_updated'],
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

    # Build a master company map: key → {name, url, field_values}
    # Key is normalised URL (if present) or lowercased name
    company_map: Dict[str, dict] = {}

    for field_id in all_fields:
        stored = load_index_data(field_id)
        if not stored:
            continue
        for company in stored.get('companies', []):
            url = (company.get('url') or '').strip()
            name = (company.get('name') or '').strip()
            if not name:
                continue
            key = url.lower() if url else name.lower()

            if key not in company_map:
                company_map[key] = {
                    'original_name': name,
                    'account_url': url,
                    'fields': {f: False for f in all_fields},
                }

            company_map[key]['fields'][field_id] = True
            # Prefer the entry with a URL
            if url and not company_map[key]['account_url']:
                company_map[key]['account_url'] = url

    # Build CSV
    output = io.StringIO()
    writer = csv.writer(output)

    header = ['original_name', 'account_url'] + all_fields
    writer.writerow(header)

    for entry in sorted(company_map.values(), key=lambda x: x['original_name'].lower()):
        row = [entry['original_name'], entry['account_url']]
        row += [str(entry['fields'][f]).upper() for f in all_fields]
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

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['original_name', 'account_url', 'rank', index_id])

    for company in stored['companies']:
        writer.writerow([
            company.get('name', ''),
            company.get('url', ''),
            company.get('rank', ''),
            'TRUE',
        ])

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
