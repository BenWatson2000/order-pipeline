"""order-pipeline run | order-pipeline query (--file F | --sql S)"""

import argparse
import json
import logging
import sys
from pathlib import Path

import duckdb

from .pipeline import run


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="order-pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("run", help="run the ETL")
    p.add_argument("--input", default="data/raw/order_data")
    p.add_argument("--output", default="output")

    q = sub.add_parser("query", help="run SQL against the output db")
    src = q.add_mutually_exclusive_group(required=True)
    src.add_argument("--file")
    src.add_argument("--sql")
    q.add_argument("--db", default="output/orders.duckdb")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.command == "run":
        print(json.dumps(run(Path(args.input), Path(args.output)), indent=2))
        return 0

    if not Path(args.db).exists():
        print(f"{args.db} not found - run `order-pipeline run` first", file=sys.stderr)
        return 1
    sql = Path(args.file).read_text() if args.file else args.sql
    with duckdb.connect(args.db, read_only=True) as con:
        print(con.sql(sql).df().to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
