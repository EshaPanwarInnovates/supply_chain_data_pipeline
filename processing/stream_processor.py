
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

from kafka import KafkaConsumer
import psycopg2


# --------------------------------------------------
# Configuration
# --------------------------------------------------
BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "supply-chain-events"
)

PG = os.getenv(
    "POSTGRES_DSN",
    "dbname=supply_chain user=sc_user password=sc_password "
    "host=localhost port=5432"
)

WINDOW = 10
ALLOWED_LATENESS = 5

VALID_EVENT_TYPES = {
    "ORDER_PLACED",
    "SHIPMENT_CREATED",
    "INVENTORY_RECEIPT"
}


# --------------------------------------------------
# Database
# --------------------------------------------------
def db():
    return psycopg2.connect(PG)


conn = db()
conn.autocommit = True
cur = conn.cursor()


# --------------------------------------------------
# Kafka consumer
# --------------------------------------------------
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id="supply-chain-processor",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda v: json.loads(v.decode()),
    consumer_timeout_ms=1000
)


# --------------------------------------------------
# Runtime state
# --------------------------------------------------
seen = set()
windows = defaultdict(list)
max_event_ts = None


print(
    "Streaming processor started.",
    flush=True
)


# --------------------------------------------------
# Timestamp parser
# --------------------------------------------------
def parse_ts(value):
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )


# --------------------------------------------------
# Record rejected event
# --------------------------------------------------
def reject(event, reason):

    event_time = None

    if event.get("event_time"):
        try:
            event_time = parse_ts(
                event["event_time"]
            )
        except Exception:
            event_time = None

    cur.execute(
        """
        INSERT INTO rejected_events(
            event_id,
            event_type,
            event_time,
            reason
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
        """,
        (
            event.get("event_id") or str(
                "INVALID-" + str(time.time_ns())
            ),
            event.get("event_type"),
            event_time,
            reason
        )
    )


# --------------------------------------------------
# Event validation
# --------------------------------------------------
def valid(event):

    # Required fields
    if not event.get("event_id"):
        return False, "missing event_id"

    if not event.get("event_type"):
        return False, "missing event_type"

    if not event.get("event_time"):
        return False, "missing event_time"

    # Event timestamp
    try:
        ts = parse_ts(
            event["event_time"]
        )
    except Exception:
        return False, "invalid event_time"

    # Event type
    if event["event_type"] not in VALID_EVENT_TYPES:
        return False, "invalid event_type"

    # Order validation
    if event["event_type"] == "ORDER_PLACED":

        try:
            quantity = int(
                event.get("quantity", 0)
            )

            unit_price = float(
                event.get("unit_price", 0)
            )
        except (TypeError, ValueError):
            return False, "invalid order values"

        if not event.get("product_id"):
            return False, "missing product_id"

        if not event.get("warehouse_id"):
            return False, "missing warehouse_id"

        if quantity <= 0:
            return False, "invalid order quantity"

        if unit_price <= 0:
            return False, "invalid unit_price"

    # Inventory validation
    if event["event_type"] == "INVENTORY_RECEIPT":

        try:
            quantity_delta = int(
                event.get("quantity_delta", 0)
            )
        except (TypeError, ValueError):
            return False, "invalid inventory quantity"

        if not event.get("product_id"):
            return False, "missing product_id"

        if not event.get("warehouse_id"):
            return False, "missing warehouse_id"

        if quantity_delta <= 0:
            return False, "invalid inventory quantity"

    # Shipment validation
    if event["event_type"] == "SHIPMENT_CREATED":

        if not event.get("shipment_id"):
            return False, "missing shipment_id"

        if not event.get("order_id"):
            return False, "missing order_id"

    return True, ts


# --------------------------------------------------
# Increment metric
# --------------------------------------------------
def increment_metric(metric):

    cur.execute(
        """
        INSERT INTO pipeline_metrics(
            metric,
            metric_value
        )
        VALUES (%s, 1)
        ON CONFLICT (metric)
        DO UPDATE SET
            metric_value =
            pipeline_metrics.metric_value + 1
        """,
        (metric,)
    )


# --------------------------------------------------
# Flush completed event-time windows
# --------------------------------------------------
def flush_ready(watermark):

    ready = [
        key
        for key in windows
        if key + WINDOW + ALLOWED_LATENESS
        <= watermark
    ]

    for key in sorted(ready):

        rows = windows.pop(key)

        by_type = defaultdict(int)
        order_value = 0.0

        for event in rows:

            event_type = event["event_type"]

            by_type[event_type] += 1

            if event_type == "ORDER_PLACED":

                order_value += (
                    float(event["quantity"])
                    * float(event["unit_price"])
                )

        cur.execute(
            """
            INSERT INTO stream_metrics(
                window_start,
                window_end,
                orders,
                shipments,
                inventory_receipts,
                order_value,
                processed_at
            )
            VALUES (
                to_timestamp(%s),
                to_timestamp(%s),
                %s,
                %s,
                %s,
                %s,
                now()
            )
            ON CONFLICT (window_start)
            DO UPDATE SET
                orders = EXCLUDED.orders,
                shipments = EXCLUDED.shipments,
                inventory_receipts =
                    EXCLUDED.inventory_receipts,
                order_value = EXCLUDED.order_value,
                processed_at = now()
            """,
            (
                key,
                key + WINDOW,
                by_type["ORDER_PLACED"],
                by_type["SHIPMENT_CREATED"],
                by_type["INVENTORY_RECEIPT"],
                order_value
            )
        )


# --------------------------------------------------
# Main streaming loop
# --------------------------------------------------
while True:

    polled = False

    for message in consumer:

        polled = True

        event = message.value

        # ------------------------------------------
        # Validate
        # ------------------------------------------
        ok, info = valid(event)

        if not ok:

            reject(
                event,
                info
            )

            increment_metric(
                "invalid_events"
            )

            print(
                f"REJECTED | "
                f"{event.get('event_type')} | "
                f"{info}",
                flush=True
            )

            consumer.commit()

            continue

        # ------------------------------------------
        # Event timestamp
        # ------------------------------------------
        timestamp = info
        epoch = timestamp.timestamp()

        # ------------------------------------------
        # Update event-time watermark
        # ------------------------------------------
        if max_event_ts is None:
            max_event_ts = epoch
        else:
            max_event_ts = max(
                max_event_ts,
                epoch
            )

        # ------------------------------------------
        # Deduplication
        # ------------------------------------------
        event_id = event["event_id"]

        if event_id in seen:

            increment_metric(
                "duplicate_events"
            )

            print(
                f"DUPLICATE | {event_id}",
                flush=True
            )

            consumer.commit()

            continue

        seen.add(event_id)

        # ------------------------------------------
        # Late-event detection
        # ------------------------------------------
        if (
            max_event_ts - epoch
            > ALLOWED_LATENESS
        ):

            increment_metric(
                "late_events"
            )

        # ------------------------------------------
        # Event-time window
        # ------------------------------------------
        bucket = (
            int(epoch // WINDOW)
            * WINDOW
        )

        windows[bucket].append(
            event
        )

        # ------------------------------------------
        # Processing metric
        # ------------------------------------------
        increment_metric(
            "processed_events"
        )

        # ------------------------------------------
        # Flush completed windows
        # ------------------------------------------
        flush_ready(
            max_event_ts
        )

        # ------------------------------------------
        # Commit Kafka offset
        # ------------------------------------------
        consumer.commit()

    if not polled:
        time.sleep(0.2)

