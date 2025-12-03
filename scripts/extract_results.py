#!/usr/bin/env python3
"""
Extract benchmark results from Criterion output.

Usage: python3 scripts/extract_results.py [--csv] [--filter <pattern>]
"""

import json
import glob
import argparse
import csv
import sys
from pathlib import Path

def extract_benchmark_data(criterion_dir="target/criterion"):
    results = []

    for path in glob.glob(f'{criterion_dir}/**/estimates.json', recursive=True):
        path = Path(path)
        parts = path.parts

        try:
            criterion_idx = parts.index('criterion')
            group = parts[criterion_idx + 1]
            benchmark = parts[criterion_idx + 2]
        except (ValueError, IndexError):
            group = "unknown"
            benchmark = path.parent.parent.name

        with open(path) as f:
            data = json.load(f)

        mean = data['mean']['point_estimate']
        mean_lo = data['mean']['confidence_interval']['lower_bound']
        mean_hi = data['mean']['confidence_interval']['upper_bound']
        std_dev = data['std_dev']['point_estimate']
        median = data['median']['point_estimate']

        results.append({
            'group': group,
            'benchmark': benchmark,
            'mean_ns': mean,
            'mean_ms': mean / 1e6,
            'ci_lower_ms': mean_lo / 1e6,
            'ci_upper_ms': mean_hi / 1e6,
            'error_ms': (mean_hi - mean_lo) / 2 / 1e6,
            'std_dev_ms': std_dev / 1e6,
            'median_ms': median / 1e6,
        })

    return sorted(results, key=lambda x: (x['group'], x['benchmark']))

def print_table(results):
    print(f"{'Group':<40} {'Benchmark':<30} {'Mean (ms)':>12} {'Error':>12} {'Std Dev':>12}")
    print("-" * 110)
    for r in results:
        print(f"{r['group']:<40} {r['benchmark']:<30} {r['mean_ms']:>12.3f} {r['error_ms']:>12.3f} {r['std_dev_ms']:>12.3f}")

def print_csv(results):
    writer = csv.DictWriter(sys.stdout, fieldnames=[
        'group', 'benchmark', 'mean_ms', 'ci_lower_ms', 'ci_upper_ms', 'error_ms', 'std_dev_ms', 'median_ms'
    ])
    writer.writeheader()
    for r in results:
        writer.writerow({k: v for k, v in r.items() if k != 'mean_ns'})

def main():
    parser = argparse.ArgumentParser(description='Extract Criterion benchmark results')
    parser.add_argument('--csv', action='store_true', help='Output as CSV')
    parser.add_argument('--filter', type=str, help='Filter by group name (substring match)')
    parser.add_argument('--dir', type=str, default='target/criterion', help='Criterion output directory')
    args = parser.parse_args()

    results = extract_benchmark_data(args.dir)

    if args.filter:
        results = [r for r in results if args.filter.lower() in r['group'].lower()]

    if not results:
        print("No benchmark results found.", file=sys.stderr)
        sys.exit(1)

    if args.csv:
        print_csv(results)
    else:
        print_table(results)
        print(f"\nTotal: {len(results)} benchmarks")

if __name__ == "__main__":
    main()
