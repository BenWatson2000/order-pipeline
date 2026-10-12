"""Extract -> clean -> model -> load."""

import json
import logging
import os
from pathlib import Path

import duckdb
import pandas as pd

from .transform import build_order_items, build_orders, clean_events

log = logging.getLogger(__name__)


def read_events(input_dir: Path) -> pd.DataFrame:
    """Read every *.json file as JSON Lines, keeping file + line for lineage."""
    files = sorted(Path(input_dir).glob("*.json"))
    if not files:
        raise FileNotFoundError(f"No *.json files in {input_dir}")
    rows = []
    for path in files:
        with path.open(encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                if line.strip():
                    rows.append({**json.loads(line), "source_file": path.name, "source_line": line_no})
    log.info("Read %d events from %d files", len(rows), len(files))
    return pd.DataFrame(rows)


def write_tables(tables: dict[str, pd.DataFrame], output_dir: Path) -> Path:
    """Write each table to Parquet, then build a DuckDB file from the Parquet.
    The db is written to a temp file and swapped in so a failed run can't
    leave a half-built database behind."""
    (output_dir / "parquet").mkdir(parents=True, exist_ok=True)
    db_path, tmp_path = output_dir / "orders.duckdb", output_dir / "orders.duckdb.tmp"
    tmp_path.unlink(missing_ok=True)

    with duckdb.connect(str(tmp_path)) as con:
        for name, df in tables.items():
            df = df.copy()
            # store timestamps as plain UTC so results don't depend on session timezone
            for col in df.select_dtypes("datetimetz"):
                df[col] = df[col].dt.tz_localize(None).dt.as_unit("us")
            for col in df.select_dtypes("object"):
                df[col] = df[col].map(lambda v: None if v is None or v is pd.NA or v != v else str(v))
            path = output_dir / "parquet" / f"{name}.parquet"
            df.to_parquet(path, index=False)
            con.execute(f"CREATE TABLE {name} AS SELECT * FROM read_parquet(?)", [str(path)])
            log.info("Loaded %s (%d rows)", name, len(df))

    os.replace(tmp_path, db_path)
    return db_path


def counts(s: pd.Series) -> dict:
    return {k: int(v) for k, v in s.value_counts().items()}


def quality_report(raw, rejected, events, items, orders) -> dict:
    return {
        "raw_events": len(raw),
        "clean_events": len(events),
        "rejected": counts(rejected["reason"]),
        "timestamp_issues": counts(events["timestamp_issue"]),
        "order_total_null": int(events["order_total"].isna().sum()),
        "order_total_sign_flipped": int(events["order_total_sign_flipped"].sum()),
        "customer_id_backfilled": int(events["customer_id_backfilled"].sum()),
        "line_issues": counts(items["line_issue"]),
        "orders": len(orders),
        "orders_missing_created_event": int(orders["missing_created_event"].sum()),
        "orders_by_status": counts(orders["current_status"]),
    }


def run(input_dir: Path, output_dir: Path) -> dict:
    output_dir = Path(output_dir)
    raw = read_events(input_dir)
    events, rejected = clean_events(raw)
    items = build_order_items(events)
    orders = build_orders(events, items)

    write_tables({
        "fct_order_events": events.drop(columns="items"),
        "fct_order_items": items,
        "dim_orders": orders,
        "rejected_events": rejected,
    }, output_dir)

    report = quality_report(raw, rejected, events, items, orders)
    (output_dir / "data_quality_report.json").write_text(json.dumps(report, indent=2))
    return report
