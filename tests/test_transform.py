import pandas as pd

from order_pipeline.transform import build_order_items, build_orders, clean_events, parse_nested, parse_timestamps


def event(**overrides):
    e = {
        "event_id": "e-1", "event_type": "order_created", "event_timestamp": "2024-03-01T10:00:00Z",
        "order_id": "ORD-1", "customer_id": "CUST-1", "order_total": 30.0, "currency": "GBP",
        "channel": "web", "payment_method": "card",
        "items": "[{'sku': 'A', 'qty': 2, 'unit_price': 10.0, 'category': 'toys'}, "
                 "{'sku': 'B', 'qty': 1, 'unit_price': 10.0, 'category': 'home'}]",
        "shipping": "{'region': 'Leeds', 'country': 'GB', 'method': None}",
        "source_file": "test.json", "source_line": 1,
    }
    return {**e, **overrides}


def run(*events):
    clean, rejected = clean_events(pd.DataFrame(events))
    items = build_order_items(clean)
    return clean, rejected, items, build_orders(clean, items).set_index("order_id")


def test_parse_nested():
    assert parse_nested("[{'a': 1, 'b': None}]") == [{"a": 1, "b": None}]
    assert parse_nested('{"a": 1}') == {"a": 1}
    assert parse_nested("__import__('os')") is None  # never executes code
    assert parse_nested(None) is None


def test_parse_timestamps():
    ts, issue = parse_timestamps(pd.Series(["2024-03-01T10:00:00Z", "1970-01-01T00:00:00Z", "2024-13-40T99:99:99"]))
    assert ts[0] == pd.Timestamp("2024-03-01T10:00:00Z")
    assert ts[1:].isna().all()
    assert issue[1:].tolist() == ["sentinel_timestamp", "unparseable_timestamp"]


def test_rejections():
    clean, rejected, _, _ = run(
        event(event_id="e-1"),
        event(event_id="e-1"),                 # duplicate
        event(event_id="e-2", order_id=""),    # empty string, not null
        event(event_id="e-3", event_type="order_teleported"),
    )
    assert clean["event_id"].tolist() == ["e-1"]
    assert sorted(rejected["reason"]) == ["duplicate_event_id", "missing_order_id", "unknown_event_type"]


def test_repairs():
    clean, _, _, _ = run(event(), event(event_id="e-2", event_type="order_paid", customer_id=None, order_total=-30.0))
    assert clean["customer_id"].tolist() == ["CUST-1", "CUST-1"]
    assert clean["order_total"].tolist() == [30.0, 30.0]
    assert clean["order_total_sign_flipped"].tolist() == [False, True]


def test_items_taken_once_and_bad_lines_flagged():
    basket = "[{'sku': 'A', 'qty': 2, 'unit_price': 10.0, 'category': 'Toys'}, {'sku': 'B', 'qty': -1, 'unit_price': 5.0, 'category': 'home'}]"
    _, _, items, _ = run(event(items=basket), event(event_id="e-2", event_type="order_paid", items=basket))
    assert len(items) == 2
    assert items["is_valid_line"].tolist() == [True, False]
    assert items["line_amount"].iloc[0] == 20.0
    assert items["category"].iloc[0] == "toys"


def test_status_and_active():
    def status(*types):
        o = run(*[event(event_id=f"e-{i}", event_type=t) for i, t in enumerate(types)])[3].loc["ORD-1"]
        return o["current_status"], bool(o["is_active"])

    assert status("order_created") == ("created", False)
    assert status("order_created", "order_paid") == ("paid", True)
    assert status("order_created", "order_paid", "order_shipped") == ("shipped", True)
    assert status("order_paid", "order_shipped", "order_delivered") == ("delivered", False)
    assert status("order_created", "order_paid", "order_cancelled", "refund_issued") == ("refunded", False)


def test_junk_timestamp_still_counts_for_status():
    orders = run(event(event_type="order_paid"),
                 event(event_id="e-2", event_type="order_delivered", event_timestamp="1970-01-01T00:00:00Z"))[3]
    assert orders.loc["ORD-1", "has_delivered"] and pd.isna(orders.loc["ORD-1", "delivered_at"])
    assert not orders.loc["ORD-1", "is_active"]


def test_total_falls_back_to_items():
    order = run(event(order_total=None))[3].loc["ORD-1"]
    assert order["order_total"] == 30.0 and order["order_total_from_items"]
