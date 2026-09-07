
from pathlib import Path
import pandas as pd
import psycopg2

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "data" / "sample"

DSN = (
    "dbname=supply_chain "
    "user=sc_user "
    "password=sc_password "
    "host=localhost "
    "port=5432"
)


def main():
    # -----------------------------
    # Load source data
    # -----------------------------
    products = pd.read_csv(SRC / "products.csv")
    orders = pd.read_csv(SRC / "orders.csv")
    shipments = pd.read_csv(SRC / "shipments.csv")
    inventory = pd.read_csv(SRC / "inventory.csv")

    # -----------------------------
    # Orders transformation
    # -----------------------------
    orders = orders.drop_duplicates("order_id")

    orders["quantity"] = pd.to_numeric(
        orders["quantity"],
        errors="coerce"
    )

    # Join product price onto orders
    orders = orders.merge(
        products[["product_id", "unit_price"]],
        on="product_id",
        how="left"
    )

    orders = orders[orders["quantity"] > 0]

    orders["order_value"] = (
        orders["quantity"] * orders["unit_price"]
    )

    # -----------------------------
    # Shipment transformation
    # -----------------------------
    shipments = shipments.drop_duplicates("shipment_id")

    shipments["ship_timestamp"] = pd.to_datetime(
        shipments["ship_timestamp"]
    )

    shipments["promised_delivery"] = pd.to_datetime(
        shipments["promised_delivery"]
    )

    shipments["actual_delivery"] = pd.to_datetime(
        shipments["actual_delivery"]
    )

    shipments["delivery_days"] = (
        shipments["actual_delivery"]
        - shipments["ship_timestamp"]
    ).dt.days

    shipments["delay_days"] = (
        shipments["actual_delivery"]
        - shipments["promised_delivery"]
    ).dt.days.clip(lower=0)

    # -----------------------------
    # Inventory transformation
    # -----------------------------
    inventory = inventory.drop_duplicates(
        ["product_id", "warehouse_id"]
    )

    inventory["inventory_status"] = inventory.apply(
        lambda r: (
            "CRITICAL"
            if r.current_stock <= r.reorder_level * 0.5
            else (
                "REORDER"
                if r.current_stock <= r.reorder_level
                else "NORMAL"
            )
        ),
        axis=1
    )

    inventory = inventory.merge(
        products[["product_id", "product_name"]],
        on="product_id",
        how="left"
    )

    # -----------------------------
    # Delivery performance
    # -----------------------------
    delivery = (
        shipments
        .groupby("carrier", as_index=False)
        .agg(
            shipments=("shipment_id", "count"),
            avg_delivery_days=("delivery_days", "mean"),
            avg_delay_days=("delay_days", "mean"),
            delayed_shipments=(
                "status",
                lambda s: (s == "DELAYED").sum()
            )
        )
    )

    # -----------------------------
    # PostgreSQL connection
    # -----------------------------
    con = psycopg2.connect(DSN)
    con.autocommit = True
    cur = con.cursor()

    # -----------------------------
    # Write DataFrames to PostgreSQL
    # -----------------------------
    def write(df, table):
        cur.execute(f'DROP TABLE IF EXISTS "{table}"')

        cols = []

        for c in df.columns:
            if pd.api.types.is_float_dtype(df[c]):
                dtype = "DOUBLE PRECISION"
            elif pd.api.types.is_integer_dtype(df[c]):
                dtype = "BIGINT"
            else:
                dtype = "TEXT"

            cols.append(f'"{c}" {dtype}')

        cur.execute(
            f'CREATE TABLE "{table}" ({", ".join(cols)})'
        )

        placeholders = ",".join(
            ["%s"] * len(df.columns)
        )

        insert_sql = (
            f'INSERT INTO "{table}" '
            f'VALUES ({placeholders})'
        )

        for row in (
            df.astype(object)
            .where(pd.notna(df), None)
            .itertuples(index=False, name=None)
        ):
            cur.execute(insert_sql, row)

    # -----------------------------
    # Load warehouse tables
    # -----------------------------
    write(orders, "fact_orders")
    write(shipments, "fact_shipments")
    write(inventory, "inventory_health")
    write(delivery, "delivery_performance")

    # -----------------------------
    # Record pipeline execution
    # -----------------------------
    cur.execute(
        """
        INSERT INTO pipeline_runs(pipeline_name, status)
        VALUES ('batch_pipeline', 'SUCCESS')
        """
    )

    cur.close()
    con.close()

    print("Batch pipeline completed.")


if __name__ == "__main__":
    main()

