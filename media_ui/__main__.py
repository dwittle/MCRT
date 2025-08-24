import argparse
from .app import main as app_main

def main():
    p = argparse.ArgumentParser(prog="media_ui", description="Media UI server")
    p.add_argument("--db", dest="db_path", help="Path to media_index.db")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=5000)
    p.add_argument("--debug", action="store_true")
    p.add_argument("--no-smoke-test", action="store_true",
                   help="Skip CLI --help smoketest on startup")
    args = p.parse_args()

    app_main(
        db_path=args.db_path,
        host=args.host,
        port=args.port,
        debug=args.debug,
        smoke_test=not args.no_smoke_test,
    )

if __name__ == "__main__":
    main()