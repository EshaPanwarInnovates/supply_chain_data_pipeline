# import os,pandas as pd,streamlit as st,plotly.express as px
# from sqlalchemy import create_engine,text
# st.set_page_config(page_title="Supply Chain Lightweight",layout="wide")
# engine=create_engine(os.getenv("POSTGRES_URL","postgresql+psycopg2://sc_user:sc_password@localhost:5432/supply_chain"))
# def q(sql):
#  try:
#   with engine.connect() as c:return pd.read_sql(text(sql),c)
#  except Exception:return pd.DataFrame()
# st.title("Supply Chain Streaming & Analytics")
# orders=q("select * from fact_orders"); ship=q("select * from fact_shipments"); inv=q("select * from inventory_health"); metrics=q("select * from pipeline_metrics")
# a,b,c,d=st.columns(4); a.metric("Orders",len(orders)); b.metric("Order value",f"${orders.order_value.sum():,.0f}" if not orders.empty else "—"); c.metric("Shipments",len(ship)); d.metric("Critical stock",int((inv.inventory_status=='CRITICAL').sum()) if not inv.empty else 0)
# st.subheader("Batch analytics")
# if not orders.empty:
#  orders['date']=pd.to_datetime(orders.order_timestamp).dt.date; daily=orders.groupby('date',as_index=False).order_value.sum(); st.plotly_chart(px.line(daily,x='date',y='order_value',title='Daily order value'),use_container_width=True)
# if not inv.empty: st.dataframe(inv[inv.inventory_status!='NORMAL'].sort_values('current_stock').head(20),use_container_width=True,hide_index=True)
# st.subheader("Streaming pipeline health")
# sm=q("select * from stream_metrics order by window_start desc limit 30"); rej=q("select count(*) n from rejected_events");
# if not sm.empty: st.plotly_chart(px.line(sm.sort_values('window_start'),x='window_start',y='orders',title='Orders per 10-second event-time window'),use_container_width=True)
# if not metrics.empty:
#  st.dataframe(metrics,use_container_width=True,hide_index=True)
# st.metric("Rejected events",int(rej.iloc[0,0]) if not rej.empty else 0)

import os
import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine, text

# --------------------------------------------------
# Page configuration
# --------------------------------------------------
st.set_page_config(
    page_title="Supply Chain Live Dashboard",
    page_icon="📦",
    layout="wide"
)

# --------------------------------------------------
# PostgreSQL connection
# --------------------------------------------------
POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql+psycopg2://sc_user:sc_password@localhost:5432/supply_chain"
)

engine = create_engine(POSTGRES_URL)


# --------------------------------------------------
# Database query helper
# --------------------------------------------------
def query(sql):
    try:
        with engine.connect() as connection:
            return pd.read_sql(text(sql), connection)
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


# --------------------------------------------------
# Auto refresh
# --------------------------------------------------
try:
    from streamlit_autorefresh import st_autorefresh

    st_autorefresh(
        interval=10_000,
        key="supply_chain_refresh"
    )
except ImportError:
    st.warning(
        "Auto-refresh is not installed. "
        "Run: pip install streamlit-autorefresh"
    )


# --------------------------------------------------
# Title
# --------------------------------------------------
st.title("📦 Supply Chain Live Operations Dashboard")

st.caption(
    "Near-real-time streaming analytics powered by "
    "Redpanda → Python Stream Processor → PostgreSQL"
)

# --------------------------------------------------
# LIVE STREAMING DATA
# --------------------------------------------------
metrics = query("""
    SELECT metric, metric_value
    FROM pipeline_metrics
""")

stream = query("""
    SELECT
        window_start,
        window_end,
        orders,
        shipments,
        inventory_receipts,
        order_value,
        processed_at
    FROM stream_metrics
    ORDER BY window_start DESC
    LIMIT 30
""")

rejected = query("""
    SELECT COUNT(*) AS rejected_events
    FROM rejected_events
""")

# --------------------------------------------------
# Extract live metrics
# --------------------------------------------------
processed_events = 0
late_events = 0

if not metrics.empty:
    metric_dict = dict(
        zip(metrics["metric"], metrics["metric_value"])
    )

    processed_events = int(
        metric_dict.get("processed_events", 0)
    )

    late_events = int(
        metric_dict.get("late_events", 0)
    )

rejected_events = (
    int(rejected.iloc[0]["rejected_events"])
    if not rejected.empty
    else 0
)

# Calculate live streaming totals from stream_metrics
live_orders = (
    int(stream["orders"].sum())
    if not stream.empty
    else 0
)

live_shipments = (
    int(stream["shipments"].sum())
    if not stream.empty
    else 0
)

live_order_value = (
    float(stream["order_value"].sum())
    if not stream.empty
    else 0
)

live_inventory_receipts = (
    int(stream["inventory_receipts"].sum())
    if not stream.empty
    else 0
)

# --------------------------------------------------
# LIVE KPI SECTION
# --------------------------------------------------
st.subheader("🔴 Live Streaming KPIs")

k1, k2, k3, k4, k5 = st.columns(5)

k1.metric(
    "Processed Events",
    f"{processed_events:,}"
)

k2.metric(
    "Live Orders",
    f"{live_orders:,}"
)

k3.metric(
    "Live Shipments",
    f"{live_shipments:,}"
)

k4.metric(
    "Late Events",
    f"{late_events:,}"
)

k5.metric(
    "Rejected Events",
    f"{rejected_events:,}"
)

# --------------------------------------------------
# Second KPI row
# --------------------------------------------------
k6, k7, k8 = st.columns(3)

k6.metric(
    "Live Order Value",
    f"${live_order_value:,.2f}"
)

k7.metric(
    "Inventory Receipts",
    f"{live_inventory_receipts:,}"
)

if not stream.empty:
    latest_processed = pd.to_datetime(
        stream["processed_at"]
    ).max()

    k8.metric(
        "Last Processed",
        latest_processed.strftime("%H:%M:%S")
    )
else:
    k8.metric(
        "Last Processed",
        "No data"
    )

# --------------------------------------------------
# LIVE STREAM CHARTS
# --------------------------------------------------
st.subheader("📈 Live Event-Time Windows")

if not stream.empty:

    chart_data = stream.copy()

    chart_data["window_start"] = pd.to_datetime(
        chart_data["window_start"]
    )

    chart_data = chart_data.sort_values(
        "window_start"
    )

    # Orders per window
    fig_orders = px.line(
        chart_data,
        x="window_start",
        y="orders",
        markers=True,
        title="Orders per 10-Second Event-Time Window"
    )

    fig_orders.update_layout(
        xaxis_title="Event Time",
        yaxis_title="Orders",
        hovermode="x unified"
    )

    st.plotly_chart(
        fig_orders,
        use_container_width=True
    )

    # Order value per window
    fig_value = px.line(
        chart_data,
        x="window_start",
        y="order_value",
        markers=True,
        title="Order Value per 10-Second Window"
    )

    fig_value.update_layout(
        xaxis_title="Event Time",
        yaxis_title="Order Value",
        hovermode="x unified"
    )

    st.plotly_chart(
        fig_value,
        use_container_width=True
    )

    # Event types
    event_chart = chart_data[
        [
            "window_start",
            "orders",
            "shipments",
            "inventory_receipts"
        ]
    ].melt(
        id_vars="window_start",
        var_name="event_type",
        value_name="count"
    )

    fig_events = px.line(
        event_chart,
        x="window_start",
        y="count",
        color="event_type",
        markers=True,
        title="Live Event Volume by Type"
    )

    fig_events.update_layout(
        xaxis_title="Event Time",
        yaxis_title="Events",
        hovermode="x unified"
    )

    st.plotly_chart(
        fig_events,
        use_container_width=True
    )

else:
    st.info("Waiting for streaming data...")


# --------------------------------------------------
# LATEST STREAMING WINDOWS
# --------------------------------------------------
st.subheader("⚡ Latest Streaming Windows")

if not stream.empty:

    display_stream = stream.copy()

    display_stream["window_start"] = pd.to_datetime(
        display_stream["window_start"]
    ).dt.strftime("%Y-%m-%d %H:%M:%S")

    display_stream["window_end"] = pd.to_datetime(
        display_stream["window_end"]
    ).dt.strftime("%Y-%m-%d %H:%M:%S")

    display_stream["processed_at"] = pd.to_datetime(
        display_stream["processed_at"]
    ).dt.strftime("%Y-%m-%d %H:%M:%S")

    st.dataframe(
        display_stream,
        use_container_width=True,
        hide_index=True
    )

# --------------------------------------------------
# BATCH / HISTORICAL ANALYTICS
# --------------------------------------------------
st.divider()

st.subheader("📊 Historical Batch Analytics")

orders = query("""
    SELECT *
    FROM fact_orders
""")

shipments = query("""
    SELECT *
    FROM fact_shipments
""")

inventory = query("""
    SELECT *
    FROM inventory_health
""")

delivery = query("""
    SELECT *
    FROM delivery_performance
""")

# --------------------------------------------------
# Batch KPIs
# --------------------------------------------------
b1, b2, b3, b4 = st.columns(4)

b1.metric(
    "Historical Orders",
    f"{len(orders):,}"
)

if not orders.empty:
    historical_value = orders["order_value"].sum()
else:
    historical_value = 0

b2.metric(
    "Historical Order Value",
    f"${historical_value:,.0f}"
)

b3.metric(
    "Historical Shipments",
    f"{len(shipments):,}"
)

critical_stock = (
    int((inventory["inventory_status"] == "CRITICAL").sum())
    if not inventory.empty
    else 0
)

b4.metric(
    "Critical Inventory",
    f"{critical_stock:,}"
)

# --------------------------------------------------
# Daily historical order value
# --------------------------------------------------
if not orders.empty:

    orders["date"] = pd.to_datetime(
        orders["order_timestamp"]
    ).dt.date

    daily = (
        orders
        .groupby("date", as_index=False)["order_value"]
        .sum()
    )

    fig_daily = px.line(
        daily,
        x="date",
        y="order_value",
        markers=True,
        title="Historical Daily Order Value"
    )

    st.plotly_chart(
        fig_daily,
        use_container_width=True
    )


# --------------------------------------------------
# Inventory alerts
# --------------------------------------------------
st.subheader("🚨 Inventory Alerts")

if not inventory.empty:

    alerts = inventory[
        inventory["inventory_status"] != "NORMAL"
    ].sort_values(
        "current_stock"
    ).head(20)

    st.dataframe(
        alerts,
        use_container_width=True,
        hide_index=True
    )

else:
    st.info("No inventory data available.")


# --------------------------------------------------
# Delivery performance
# --------------------------------------------------
st.subheader("🚚 Delivery Performance")

if not delivery.empty:

    fig_delivery = px.bar(
        delivery,
        x="carrier",
        y="avg_delay_days",
        title="Average Delivery Delay by Carrier"
    )

    fig_delivery.update_layout(
        xaxis_title="Carrier",
        yaxis_title="Average Delay (Days)"
    )

    st.plotly_chart(
        fig_delivery,
        use_container_width=True
    )

    st.dataframe(
        delivery,
        use_container_width=True,
        hide_index=True
    )


# --------------------------------------------------
# Pipeline observability
# --------------------------------------------------
st.subheader("🔍 Pipeline Observability")

if not metrics.empty:

    st.dataframe(
        metrics,
        use_container_width=True,
        hide_index=True
    )

st.caption(
    "Dashboard refreshes every 10 seconds. "
    "Live KPIs are calculated from PostgreSQL streaming results."
)


