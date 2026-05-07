"""
Entry point for `python -m quackserver <command>`.

Commands:
  serve     Start the HTTP server.
  rebuild   Rebuild the DuckDB projection from the append log.
  inspect   Inspect any DuckDB file (also available as `duckdb-inspect`).
  compact   Compact any DuckDB file (also available as `duckdb-compact`).
"""

import sys


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print("Usage: python -m quackserver <command> [options]")
        print()
        print("Commands:")
        print("  serve     Start the HTTP server")
        print("  rebuild   Rebuild the DuckDB projection from the append log")
        print("  inspect   Inspect any DuckDB file (table inventory, views, sizes)")
        print("  compact   Compact any DuckDB file (ATTACH streaming copy + validation)")
        print()
        print("Run `python -m quackserver <command> --help` for command options.")
        sys.exit(0)

    cmd, rest = sys.argv[1], sys.argv[2:]

    if cmd == "serve":
        from quackserver.cli.serve import main as _serve
        sys.argv = ["quackserver serve"] + rest
        _serve()
    elif cmd == "rebuild":
        from quackserver.cli.rebuild import main as _rebuild
        sys.argv = ["quackserver rebuild"] + rest
        _rebuild()
    elif cmd == "inspect":
        from quackserver.cli.inspect import main as _inspect
        sys.argv = ["duckdb-inspect"] + rest
        _inspect()
    elif cmd == "compact":
        from quackserver.cli.compact import main as _compact
        sys.argv = ["duckdb-compact"] + rest
        _compact()
    else:
        print(f"Unknown command: {cmd!r}", file=sys.stderr)
        print("Run `python -m quackserver --help` for available commands.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
