# Dashboard

`app.py` is a Streamlit dashboard reading directly from the PostgreSQL serving layer. Four pages:

1. **Executive Overview** — total orders, order value, shipment count, delayed shipments.
2. **Inventory Health** — stock by warehouse, critical/reorder SKUs, inventory status.
3. **Supplier & Product Performance** — top products by revenue. (Supplier-level KPIs aren't available yet — see note below.)
4. **Delivery Performance** — carrier comparison, average delivery days, delay rate.

## Run

Starts automatically with `docker compose up -d` on **http://localhost:8501**, once the batch pipeline has been run at least once (`make batch && make db`, or the manual steps in the main README).

To run it standalone, outside Docker:
```bash
pip install -r ../requirements.txt
POSTGRES_HOST=localhost streamlit run app.py
```

Refresh: the dashboard queries Postgres live on every page load, so it reflects whatever the batch pipeline last wrote — just reload the page after a new run.

## Note on supplier KPIs

`products.csv` doesn't carry a `supplier_id`, so `fact_orders` has nothing to join suppliers against. `run_batch.py` computes a `supplier_perf` DataFrame but never writes it to Gold. Once a real product↔supplier relationship and a persisted `supplier_performance` table exist, the Supplier & Product Performance page will pick it up automatically — no dashboard changes needed.
