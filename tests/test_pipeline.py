"""End-to-end run over the real sample data, checked through the SQL files."""

from pathlib import Path

import duckdb
import pytest

from order_pipeline import pipeline

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def db(tmp_path_factory):
    out = tmp_path_factory.mktemp("output")
    report = pipeline.run(ROOT / "data" / "raw" / "order_data", out)
    with duckdb.connect(str(out / "orders.duckdb"), read_only=True) as con:
        yield con, report


def run_sql(con, name):
    return con.sql((ROOT / "sql" / name).read_text()).fetchall()


def test_every_raw_event_is_accounted_for(db):
    con, report = db
    rejected = sum(report["rejected"].values())
    assert report["raw_events"] == report["clean_events"] + rejected


def test_one_row_per_order_and_no_duplicate_events(db):
    con, _ = db
    assert con.sql("SELECT COUNT(*) = COUNT(DISTINCT order_id) FROM dim_orders").fetchone()[0]
    assert con.sql("SELECT COUNT(*) = COUNT(DISTINCT event_id) FROM fct_order_events").fetchone()[0]
    assert con.sql("SELECT COUNT(*) FROM fct_order_events WHERE customer_id IS NULL").fetchone()[0] == 0


def test_active_customers_query(db):
    con, _ = db
    customers, orders = run_sql(con, "01_customers_with_active_order.sql")[0]
    assert 0 < customers <= orders
    expected = con.sql(
        "SELECT COUNT(*) FROM dim_orders WHERE current_status IN ('paid', 'shipped')"
    ).fetchone()[0]
    assert orders == expected


def test_shipped_2024_by_category_query(db):
    con, _ = db
    rows = run_sql(con, "02_orders_shipped_2024_by_category.sql")
    categories = {category for category, _ in rows}
    assert categories == {"apparel", "beauty", "electronics", "grocery", "home", "sports", "toys"}
    total_2024 = run_sql(con, "03_orders_shipped_2024_total.sql")[0][0]
    assert all(0 < n <= total_2024 for _, n in rows)


# Expected answers on the sample data (also quoted in the README). Any change
# to the cleaning or modelling rules that moves these should be deliberate.
def test_headline_numbers_on_sample_data(db):
    con, report = db
    assert run_sql(con, "01_customers_with_active_order.sql") == [(109, 128)]
    assert dict(run_sql(con, "02_orders_shipped_2024_by_category.sql")) == {
        "grocery": 153, "home": 149, "electronics": 136, "toys": 126,
        "sports": 114, "beauty": 113, "apparel": 110,
    }
    assert run_sql(con, "03_orders_shipped_2024_total.sql") == [(594, 14, 0)]
    assert report["raw_events"] == 3024
    assert report["rejected"] == {"duplicate_event_id": 73, "missing_order_id": 33}
    assert report["orders"] == 800
