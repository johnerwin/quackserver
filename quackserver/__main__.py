"""
Entry point for `python -m quackserver <command>`.

Commands:
  rebuild   Rebuild the DuckDB projection from the append log.
"""

import sys


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print("Usage: python -m quackserver <command> [options]")
        print()
        print("Commands:")
        print("  rebuild   Rebuild the DuckDB projection from the append log")
        print()
        print("Run `python -m quackserver <command> --help` for command options.")
        sys.exit(0)

    cmd, rest = sys.argv[1], sys.argv[2:]

    if cmd == "rebuild":
        from quackserver.cli.rebuild import main as _rebuild
        sys.argv = ["quackserver rebuild"] + rest
        _rebuild()
    else:
        print(f"Unknown command: {cmd!r}", file=sys.stderr)
        print("Run `python -m quackserver --help` for available commands.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
