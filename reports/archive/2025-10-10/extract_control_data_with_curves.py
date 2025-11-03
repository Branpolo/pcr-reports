#!/usr/bin/env python3
"""Legacy entry point for control report extraction."""

from __future__ import annotations

import sys

from extract_report_with_curves import main as unified_main


def main() -> None:
    unified_main(['control', *sys.argv[1:]])


if __name__ == '__main__':
    main()
