#!/usr/bin/env python3
"""Legacy entry point for discrepancy report extraction."""

from __future__ import annotations

import sys

from extract_report_with_curves import main as unified_main


def main() -> None:
    unified_main(['discrepancy', *sys.argv[1:]])


if __name__ == '__main__':
    main()
