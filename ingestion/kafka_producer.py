

import json
import os
import random
import time
import uuid

from datetime import datetime, timezone, timedelta

from kafka import KafkaProducer


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

INTERVAL = float(
    os.getenv(
        "EVENT_INTERVAL_SECONDS",
        "1.5"
    )
)

# Percentage of events deliberately made invalid
INVALID_EVENT_RATE = 0.05


# --------------------------------------------------
# Synthetic reference data
# --------------------------------------------------
products = [
    {
        "product_id": f"P{i:03d}",
        "unit_price": round(
            random.uniform(10, 250),
            2
        ),
        "reorder_level": random.randint(
            30,
            100
        )
    }
    for i in range(1, 21)
]

warehouses = [
    f"WH{i:02d}"
    for i in range(1, 5)
]

stock = {
    (p["product_id"], w): random.randint(80, 300)
    for p in products
    for w in warehouses
}

open_orders = []


# --------------------------------------------------
# Kafka producer
# --------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode()
)


# --------------------------------------------------
# Event creation
# --------------------------------------------------
def make_event(
    event_type,
    event_time=None,
    **payload
):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": (
            event_time
            or datetime.now(timezone.utc)
        ).isoformat(),
        **payload
    }


# --------------------------------------------------
# Invalid-event injection
# --------------------------------------------------
def make_invalid_event():
    """
    Create one deliberately invalid event.

    The stream processor should reject this event
    and write it to rejected_events.
    """

    invalid_type = random.choice(
        [
            "NEGATIVE_QUANTITY",
            "INVALID_EVENT_TYPE",
            "MISSING_EVENT_ID",
            "INVALID_PRICE",
            "INVALID_TIMESTAMP"
        ]
    )

    # Start with a structurally normal order
    p = random.choice(products)
    w = random.choice(warehouses)

    event = make_event(
        "ORDER_PLACED",
        order_id=f"INVALID-{uuid.uuid4().hex[:8].upper()}",
        product_id=p["product_id"],
        warehouse_id=w,
        quantity=random.randint(1, 10),
        unit_price=p["unit_price"]
    )

    # --------------------------------------------------
    # Inject one specific problem
    # --------------------------------------------------
    if invalid_type == "NEGATIVE_QUANTITY":
        event["quantity"] = -10

    elif invalid_type == "INVALID_EVENT_TYPE":
        event["event_type"] = "UNKNOWN_EVENT_TYPE"

    elif invalid_type == "MISSING_EVENT_ID":
        event["event_id"] = ""

    elif invalid_type == "INVALID_PRICE":
        event["unit_price"] = -50

    elif invalid_type == "INVALID_TIMESTAMP":
        event["event_time"] = "not-a-valid-timestamp"

    print(
        f"INVALID EVENT GENERATED | {invalid_type}",
        flush=True
    )

    return event


# --------------------------------------------------
# Send event
# --------------------------------------------------
def emit(event):
    producer.send(
        TOPIC,
        event
    ).get(timeout=10)

    identifier = event.get(
        "order_id",
        event.get(
            "shipment_id",
            event.get(
                "product_id",
                "unknown"
            )
        )
    )

    print(
        f'{event.get("event_time")} | '
        f'{event.get("event_type")} | '
        f'{identifier}',
        flush=True
    )


# --------------------------------------------------
# Start simulator
# --------------------------------------------------
print(
    "Live supply-chain simulator started. "
    "Ctrl+C to stop.",
    flush=True
)

print(
    f"Invalid event rate: "
    f"{INVALID_EVENT_RATE * 100:.0f}%",
    flush=True
)


# --------------------------------------------------
# Continuous event generation
# --------------------------------------------------
while True:

    # ----------------------------------------------
    # Occasionally generate an invalid event
    # ----------------------------------------------
    if random.random() < INVALID_EVENT_RATE:

        invalid_event = make_invalid_event()
        emit(invalid_event)

    else:

        # ------------------------------------------
        # Normal event generation
        # ------------------------------------------
        action = random.choices(
            [
                "order",
                "shipment",
                "inventory"
            ],
            [
                0.60,
                0.20,
                0.20
            ]
        )[0]

        # ------------------------------------------
        # Order
        # ------------------------------------------
        if action == "order":

            p = random.choice(products)
            w = random.choice(warehouses)

            available = stock[
                (p["product_id"], w)
            ]

            qty = random.randint(
                1,
                max(
                    1,
                    min(
                        20,
                        available
                    )
                )
            )

            oid = (
                f"ORD-"
                f"{uuid.uuid4().hex[:8].upper()}"
            )

            stock[
                (p["product_id"], w)
            ] -= qty

            open_orders.append(
                (
                    oid,
                    p["product_id"],
                    w
                )
            )

            emit(
                make_event(
                    "ORDER_PLACED",
                    order_id=oid,
                    product_id=p["product_id"],
                    warehouse_id=w,
                    quantity=qty,
                    unit_price=p["unit_price"]
                )
            )

        # ------------------------------------------
        # Shipment
        # ------------------------------------------
        elif action == "shipment" and open_orders:

            oid, pid, w = open_orders.pop(0)

            sid = (
                f"SHIP-"
                f"{uuid.uuid4().hex[:8].upper()}"
            )

            emit(
                make_event(
                    "SHIPMENT_CREATED",
                    shipment_id=sid,
                    order_id=oid,
                    product_id=pid,
                    warehouse_id=w,
                    carrier=random.choice(
                        [
                            "DHL",
                            "FedEx",
                            "UPS",
                            "BlueDart"
                        ]
                    )
                )
            )

        # ------------------------------------------
        # Inventory receipt
        # ------------------------------------------
        else:

            p = random.choice(products)
            w = random.choice(warehouses)

            delta = random.randint(
                5,
                30
            )

            stock[
                (p["product_id"], w)
            ] += delta

            event_time = datetime.now(
                timezone.utc
            )

            # Occasionally create late events
            if random.random() < 0.08:

                event_time -= timedelta(
                    seconds=random.randint(
                        6,
                        12
                    )
                )

            emit(
                make_event(
                    "INVENTORY_RECEIPT",
                    event_time=event_time,
                    product_id=p["product_id"],
                    warehouse_id=w,
                    quantity_delta=delta,
                    current_stock=stock[
                        (p["product_id"], w)
                    ]
                )
            )

    time.sleep(INTERVAL)

