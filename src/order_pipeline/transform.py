"""Cleaning and modelling of order events."""

import ast
import json
import logging

import pandas as pd

log = logging.getLogger(__name__)

# event_type -> stage name used for the has_<stage> / <stage>_at columns
STAGES = {
    "order_created": "created",
    "order_paid": "paid",
    "order_shipped": "shipped",
    "order_delivered": "delivered",
    "order_cancelled": "cancelled",
    "order_returned": "returned",
    "refund_issued": "refunded",
}

# Most terminal first. Status is picked by which events exist rather than by
# timestamp, because some timestamps in the feed are junk.
STATUS_PRECEDENCE = ["refunded", "returned", "cancelled", "delivered", "shipped", "paid", "created"]

# Placeholder dates the upstream system uses when it has no real value
SENTINEL_DATES = pd.to_datetime(["1970-01-01T00:00:00Z", "2099-12-31T00:00:00Z"])


def parse_nested(value):
    """items/shipping arrive as Python-style strings ("[{'sku': ...}]"), not JSON.
    literal_eval only evaluates literals so it's safe on untrusted input."""
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return None
    for parse in (json.loads, ast.literal_eval):
        try:
            return parse(value)
        except (ValueError, SyntaxError):
            pass
    return None


def clean_str(s: pd.Series) -> pd.Series:
    """Strip whitespace and treat empty strings as null."""
    s = s.astype("string").str.strip()
    return s.mask(s == "")


def parse_timestamps(values: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Return (UTC timestamps, issue). Bad values become NaT with a reason."""
    ts = pd.to_datetime(values, utc=True, errors="coerce", format="ISO8601")
    issue = pd.Series(pd.NA, index=values.index, dtype="string")
    issue[values.notna() & ts.isna()] = "unparseable_timestamp"
    issue[ts.isin(SENTINEL_DATES)] = "sentinel_timestamp"
    return ts.mask(ts.isin(SENTINEL_DATES)), issue


def clean_events(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (events, rejected).

    Rows are only rejected if they're unusable (duplicate, no order id, unknown
    type). Anything fixable is fixed and flagged instead.
    """
    df = raw.copy()
    for col in ["event_id", "event_type", "order_id", "customer_id", "channel", "payment_method", "currency"]:
        df[col] = clean_str(df[col])
    df["event_type"] = df["event_type"].str.lower()
    df["currency"] = df["currency"].str.upper()

    rejected = []
    for reason, bad in [
        ("missing_event_id", lambda d: d["event_id"].isna()),
        ("duplicate_event_id", lambda d: d.duplicated("event_id")),
        ("missing_order_id", lambda d: d["order_id"].isna()),
        ("unknown_event_type", lambda d: ~d["event_type"].isin(STAGES.keys())),
    ]:
        mask = bad(df)
        if mask.any():
            log.info("Rejecting %d events: %s", mask.sum(), reason)
            rejected.append(df[mask].assign(reason=reason))
        df = df[~mask]

    df["event_stage"] = df["event_type"].map(STAGES)
    df["event_ts"], df["timestamp_issue"] = parse_timestamps(df["event_timestamp"])

    # customer_id never changes within an order, so fill gaps from other events
    df["customer_id_backfilled"] = df["customer_id"].isna()
    df["customer_id"] = df.groupby("order_id")["customer_id"].transform("first")

    # some totals have the sign flipped
    total = pd.to_numeric(df["order_total"], errors="coerce")
    df["order_total_sign_flipped"] = total < 0
    df["order_total"] = total.abs()

    df["items"] = df["items"].map(parse_nested)
    shipping = df["shipping"].map(parse_nested).map(lambda v: v if isinstance(v, dict) else {})
    for field in ["region", "country", "method"]:
        df[f"shipping_{field}"] = clean_str(shipping.map(lambda s: s.get(field)))

    events = df[[
        "event_id", "order_id", "customer_id", "event_type", "event_stage", "event_ts",
        "timestamp_issue", "order_total", "order_total_sign_flipped", "currency", "channel",
        "payment_method", "shipping_region", "shipping_country", "shipping_method",
        "customer_id_backfilled", "items", "source_file", "source_line",
    ]].reset_index(drop=True)

    rejected = pd.concat(rejected, ignore_index=True) if rejected else pd.DataFrame(columns=[*raw.columns, "reason"])
    return events, rejected


def build_order_items(events: pd.DataFrame) -> pd.DataFrame:
    """One row per line item. The basket is repeated (identically) on every event
    of an order, so take it once per order to avoid double counting."""
    baskets = events[events["items"].map(lambda v: isinstance(v, list))].drop_duplicates("order_id")
    rows = [
        {"order_id": order_id, "line_number": n, **item}
        for order_id, items in zip(baskets["order_id"], baskets["items"])
        for n, item in enumerate(items, start=1)
    ]
    items = pd.DataFrame(rows).rename(columns={"qty": "quantity"})
    items["category"] = clean_str(items["category"]).str.lower()
    items["quantity"] = pd.to_numeric(items["quantity"], errors="coerce")
    items["unit_price"] = pd.to_numeric(items["unit_price"], errors="coerce")

    # qty 0 / -1 lines aren't real items - the reported order totals leave them out
    items["is_valid_line"] = items["quantity"] > 0
    items["line_issue"] = pd.Series(pd.NA, index=items.index, dtype="string")
    items.loc[items["unit_price"].isna(), "line_issue"] = "missing_unit_price"
    items.loc[~items["is_valid_line"], "line_issue"] = "non_positive_quantity"
    items["line_amount"] = (items["quantity"] * items["unit_price"]).where(items["is_valid_line"]).round(2)
    return items[["order_id", "line_number", "sku", "category", "quantity", "unit_price",
                  "line_amount", "is_valid_line", "line_issue"]]


def build_orders(events: pd.DataFrame, items: pd.DataFrame) -> pd.DataFrame:
    """One row per order: attributes, lifecycle timestamps, current status."""
    by_order = events.sort_values("event_ts").groupby("order_id")
    orders = by_order[["customer_id", "channel", "currency", "payment_method",
                       "shipping_region", "shipping_country", "shipping_method"]].first()

    # has_<stage>: the event happened (even if its timestamp was junk)
    # <stage>_at: earliest valid timestamp for that stage
    first_ts = events.pivot_table(index="order_id", columns="event_stage", values="event_ts", aggfunc="min")
    seen = pd.crosstab(events["order_id"], events["event_stage"]) > 0
    for stage in STAGES.values():
        orders[f"has_{stage}"] = seen[stage] if stage in seen else False
        orders[f"{stage}_at"] = first_ts[stage] if stage in first_ts else pd.Series(pd.NaT, index=orders.index, dtype="datetime64[ns, UTC]")

    orders["current_status"] = orders.apply(
        lambda o: next(s for s in STATUS_PRECEDENCE if o[f"has_{s}"]), axis=1)
    orders["is_active"] = orders["has_paid"] & ~(orders["has_delivered"] | orders["has_cancelled"] | orders["has_refunded"])

    # Reported total is consistent across an order's events once the sign is fixed.
    # Fall back to summing the valid lines when every event's total is null.
    valid = items[items["is_valid_line"]]
    from_items = valid.groupby("order_id")["line_amount"].sum(min_count=1)
    from_items[valid.groupby("order_id")["unit_price"].apply(lambda p: p.isna().any())] = None
    orders["order_total"] = by_order["order_total"].max().fillna(from_items.round(2))
    orders["order_total_from_items"] = by_order["order_total"].max().isna() & orders["order_total"].notna()
    orders["item_count"] = valid.groupby("order_id")["quantity"].sum().reindex(orders.index, fill_value=0)

    orders["missing_created_event"] = ~orders["has_created"]
    orders["event_count"] = by_order.size()
    return orders.reset_index()
