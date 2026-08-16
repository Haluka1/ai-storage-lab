from __future__ import annotations

import argparse


def main() -> int:
    parser = argparse.ArgumentParser(description="IO path benchmark helpers")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("help-config", help="print a short config note")
    args = parser.parse_args()
    if args.command == "help-config":
        print("Use run_matrix.py with JSON-compatible YAML config files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
