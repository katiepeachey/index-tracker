"""
refresh.py — Standalone CLI to scrape and refresh index data.

Writes directly to data/*.json — does not require the Flask server to be running.
Cross-validates against the alt source (investing.com) for indices that have one.

Usage:
  python refresh.py                      # refresh all auto-scraped indices
  python refresh.py cac_40 dax ftse_100  # refresh specific indices by ID
  python refresh.py --list               # print all index IDs and exit
  python refresh.py --dry-run            # show what would be refreshed, don't scrape

Exit codes:
  0 — all requested indices succeeded
  1 — one or more indices failed
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

# Allow running from any working directory
sys.path.insert(0, os.path.dirname(__file__))

from scrapers import scrape_index, validate_against_alt, INDICES_CONFIG
from app import apply_enrichment

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
os.makedirs(DATA_DIR, exist_ok=True)

# ANSI colours (disabled on Windows or non-TTY)
_USE_COLOUR = sys.stdout.isatty() and os.name != 'nt'
GREEN  = '\033[32m' if _USE_COLOUR else ''
YELLOW = '\033[33m' if _USE_COLOUR else ''
RED    = '\033[31m' if _USE_COLOUR else ''
CYAN   = '\033[36m' if _USE_COLOUR else ''
BOLD   = '\033[1m'  if _USE_COLOUR else ''
RESET  = '\033[0m'  if _USE_COLOUR else ''


def _validation_badge(v: dict) -> str:
    if not v:
        return ''
    status = v.get('status', '')
    alt = v.get('alt_count', '?')
    pct = v.get('overlap_pct', '?')
    if status == 'ok':
        return f'  {GREEN}✓ validated (alt={alt}, overlap={pct}%){RESET}'
    elif status == 'warn':
        return f'  {YELLOW}⚠ validation warn (alt={alt}, overlap={pct}%){RESET}'
    elif status == 'fail':
        return f'  {RED}✗ validation fail (alt={alt}, overlap={pct}%){RESET}'
    elif status == 'error':
        return f'  {YELLOW}⚠ alt source error: {v.get("error", "")[:60]}{RESET}'
    return ''


def refresh_one(index_id: str, config: dict, dry_run: bool = False) -> dict:
    """
    Scrape one index, run cross-validation if configured, and save to disk.

    Returns a result dict:
      {id, name, success, count, validation, error, duration_s}
    """
    name = config['name']

    if config.get('manual'):
        return {
            'id': index_id, 'name': name, 'success': False,
            'error': 'Manual upload only — use the web UI Upload CSV button',
            'skipped': True,
        }

    if dry_run:
        alt = f" + {config['alt_source']} validation" if config.get('alt_source') else ''
        print(f'  {CYAN}[dry-run]{RESET} {name} ({config.get("source", "?")}){alt}')
        return {'id': index_id, 'name': name, 'success': True, 'skipped': True}

    t0 = datetime.now(timezone.utc)
    try:
        companies = scrape_index(index_id, config)
        apply_enrichment(index_id, companies)
        duration = round((datetime.now(timezone.utc) - t0).total_seconds(), 1)

        data = {
            'id': index_id,
            'name': name,
            'last_updated': datetime.utcnow().isoformat() + 'Z',
            'count': len(companies),
            'companies': companies,
        }

        # Cross-validate against alt source if configured
        validation = validate_against_alt(index_id, config, companies)
        if validation:
            data['validation'] = validation

        path = os.path.join(DATA_DIR, f'{index_id}.json')
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)

        expected = config.get('expected_count')
        count_note = ''
        if expected:
            ratio = len(companies) / expected
            if ratio < 0.85 or ratio > 1.15:
                count_note = f' {YELLOW}(expected {expected}){RESET}'

        badge = _validation_badge(validation)
        print(
            f'  {GREEN}✓{RESET} {BOLD}{name}{RESET}  '
            f'{len(companies):,} companies{count_note}  '
            f'{CYAN}{duration}s{RESET}'
            f'{badge}'
        )
        return {
            'id': index_id, 'name': name, 'success': True,
            'count': len(companies), 'validation': validation, 'duration_s': duration,
        }

    except Exception as exc:
        duration = round((datetime.now(timezone.utc) - t0).total_seconds(), 1)
        print(f'  {RED}✗ {name}: {exc}{RESET}')
        return {
            'id': index_id, 'name': name, 'success': False,
            'error': str(exc), 'duration_s': duration,
        }


def main():
    parser = argparse.ArgumentParser(
        description='Refresh index constituent data from configured sources.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        'indices', nargs='*',
        help='Index IDs to refresh (default: all auto-scraped indices). '
             'Run --list to see all available IDs.',
    )
    parser.add_argument(
        '--list', action='store_true',
        help='Print all index IDs and exit.',
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Show what would be refreshed without actually scraping.',
    )
    parser.add_argument(
        '--include-manual', action='store_true',
        help='Include manual-upload indices in the output (they are always skipped).',
    )
    args = parser.parse_args()

    if args.list:
        print(f'{"ID":<22} {"Name":<35} {"Source":<20} {"Alt":<12} Rebalances')
        print('-' * 110)
        for iid, cfg in INDICES_CONFIG.items():
            alt = cfg.get('alt_source', '')
            manual = ' [manual]' if cfg.get('manual') else ''
            print(
                f'{iid:<22} {cfg["name"]:<35} {cfg.get("source",""):<20} '
                f'{alt:<12} {cfg.get("rebalance_schedule","")}{manual}'
            )
        return 0

    # Determine which indices to refresh
    if args.indices:
        unknown = [i for i in args.indices if i not in INDICES_CONFIG]
        if unknown:
            print(f'{RED}Unknown index IDs: {", ".join(unknown)}{RESET}')
            print(f'Run  python refresh.py --list  to see all valid IDs.')
            return 1
        targets = [(iid, INDICES_CONFIG[iid]) for iid in args.indices]
    else:
        targets = [
            (iid, cfg) for iid, cfg in INDICES_CONFIG.items()
            if not cfg.get('manual')
        ]

    label = 'dry run' if args.dry_run else 'refresh'
    print(f'\n{BOLD}Index Tracker — {label}{RESET}')
    print(f'Targets: {len(targets)} indices\n')

    results = []
    t_start = datetime.now(timezone.utc)

    for iid, cfg in targets:
        result = refresh_one(iid, cfg, dry_run=args.dry_run)
        results.append(result)

    total_s = round((datetime.now(timezone.utc) - t_start).total_seconds(), 1)

    if args.dry_run:
        return 0

    # Summary
    successes = [r for r in results if r.get('success') and not r.get('skipped')]
    failures  = [r for r in results if not r.get('success') and not r.get('skipped')]
    skipped   = [r for r in results if r.get('skipped')]

    val_ok   = sum(1 for r in successes if r.get('validation', {}).get('status') == 'ok')
    val_warn = sum(1 for r in successes if r.get('validation', {}).get('status') in ('warn', 'fail'))

    print(f'\n{"─"*60}')
    print(f'{BOLD}Summary{RESET}  ({total_s}s total)')
    print(f'  {GREEN}✓ {len(successes)} succeeded{RESET}', end='')
    if val_ok:   print(f'  ({val_ok} validated ✓)', end='')
    if val_warn: print(f'  ({val_warn} validation warnings)', end='')
    print()
    if failures:
        print(f'  {RED}✗ {len(failures)} failed:{RESET}')
        for r in failures:
            print(f'    • {r["name"]}: {r.get("error", "unknown error")[:80]}')
    if skipped:
        print(f'  {YELLOW}— {len(skipped)} skipped (manual){RESET}')
    print()

    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())
