#!/usr/bin/env python3
"""Detect overlapping entities across key categories within a combined report JSON."""

import argparse
import json
from collections import defaultdict


CategorySpec = tuple[str, str, str]


DEFAULT_SPECS: list[CategorySpec] = [
    # (report_path, clinical_category, human label)
    ("sample", "unresolved", "sample.unresolved"),
    ("sample", "test_repeated", "sample.test_repeated"),
    ("sample", "error_ignored", "sample.error_ignored"),
    ("control", "unresolved", "control.unresolved"),
    ("control", "test_repeated", "control.test_repeated"),
    ("control", "error_ignored", "control.error_ignored"),
    ("discrepancy", "acted_upon", "discrepancy.acted_upon"),
    ("discrepancy", "samples_repeated", "discrepancy.samples_repeated"),
    ("discrepancy", "ignored", "discrepancy.ignored"),
]


def extract_category_sets(data: dict, specs: list[CategorySpec]):
    """Return {label: set(well_ids)} for the requested categories."""

    sets: dict[str, set[str]] = {}
    reports = data.get("reports") or {}

    for report_key, clinical_category, label in specs:
        report_data = reports.get(report_key, {})
        errors = report_data.get("errors", [])
        matching = {
            str(entry.get("well_id"))
            for entry in errors
            if entry.get("clinical_category") == clinical_category
        }
        sets[label] = matching

    return sets


def find_overlaps(sets: dict[str, set[str]]):
    """Return list of overlap details between every pair of categories."""

    labels = list(sets.keys())
    overlaps = []

    for i, label_a in enumerate(labels):
        wells_a = sets[label_a]
        for label_b in labels[i + 1 :]:
            wells_b = sets[label_b]
            shared = wells_a & wells_b
            if shared:
                overlaps.append((label_a, label_b, shared))

    return overlaps


def main():
    parser = argparse.ArgumentParser(description="Detect overlap between report categories")
    parser.add_argument("json", help="Path to combined JSON report")
    args = parser.parse_args()

    with open(args.json, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    category_sets = extract_category_sets(data, DEFAULT_SPECS)
    overlaps = find_overlaps(category_sets)

    if not overlaps:
        print("✅ No overlaps detected between tracked categories.")
        return

    print("❌ Overlaps detected:")
    for label_a, label_b, shared in overlaps:
        print(f"  - {label_a} ↔ {label_b}: {len(shared)} wells")

    # Optionally: list up to a few sample IDs per overlap for debugging
    sample_dump = defaultdict(list)
    for label_a, label_b, shared in overlaps:
        sample_dump[(label_a, label_b)] = list(shared)[:5]

    for (label_a, label_b), sample_ids in sample_dump.items():
        print(f"    example wells ({label_a} ↔ {label_b}): {', '.join(sample_ids)}")


if __name__ == "__main__":
    main()
