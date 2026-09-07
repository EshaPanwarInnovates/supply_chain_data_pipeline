CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    run_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rejected_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT,
    event_time TIMESTAMPTZ,
    reason TEXT,
    payload JSONB
);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    metric TEXT PRIMARY KEY,
    metric_value BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS stream_metrics (
    window_start TIMESTAMPTZ PRIMARY KEY,
    window_end TIMESTAMPTZ,
    orders BIGINT,
    shipments BIGINT,
    inventory_receipts BIGINT,
    order_value DOUBLE PRECISION,
    processed_at TIMESTAMPTZ
);