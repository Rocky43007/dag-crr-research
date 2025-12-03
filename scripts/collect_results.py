#!/usr/bin/env python3
"""Collect benchmark results from Criterion into results/"""

import json
import glob
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
CRITERION_DIR = REPO_ROOT / "target" / "criterion"
RESULTS_DIR = REPO_ROOT / "results"

def extract_criterion_results():
    results = {}
    for path in glob.glob(str(CRITERION_DIR / "**/new/estimates.json"), recursive=True):
        parts = Path(path).parts
        criterion_idx = parts.index("criterion")
        bench_name = "/".join(parts[criterion_idx + 1:-2])

        with open(path) as f:
            data = json.load(f)

        mean_ns = data["mean"]["point_estimate"]
        ci_lower = data["mean"]["confidence_interval"]["lower_bound"]
        ci_upper = data["mean"]["confidence_interval"]["upper_bound"]

        results[bench_name] = {
            "mean_ns": mean_ns,
            "mean_ms": mean_ns / 1e6,
            "ci_lower_ms": ci_lower / 1e6,
            "ci_upper_ms": ci_upper / 1e6,
            "error_ms": (ci_upper - ci_lower) / 2 / 1e6,
        }
    return results

def main():
    RESULTS_DIR.mkdir(exist_ok=True)

    criterion = extract_criterion_results()
    if criterion:
        with open(RESULTS_DIR / "criterion.json", "w") as f:
            json.dump(criterion, f, indent=2)
        print(f"Extracted {len(criterion)} benchmarks -> results/criterion.json")
    else:
        print("No Criterion results found. Run: cargo bench")

    network_file = RESULTS_DIR / "network.json"
    if network_file.exists():
        with open(network_file) as f:
            network = json.load(f)
        print(f"Found network results: {len(network)} peers")
        for r in network:
            print(f"  {r['peer']}: RTT {r['rtt_mean_us']}us, speedup {r['speedup']:.0f}x")

    if criterion:
        print("\n=== Results ===\n")
        groups = {}
        for name, data in sorted(criterion.items()):
            group = name.split("/")[0]
            groups.setdefault(group, []).append((name, data))

        for group, items in sorted(groups.items()):
            print(f"## {group}")
            for name, data in items:
                short_name = "/".join(name.split("/")[1:])
                print(f"  {short_name}: {data['mean_ms']:.3f} +/- {data['error_ms']:.3f} ms")
            print()

if __name__ == "__main__":
    main()
