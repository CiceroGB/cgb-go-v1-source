CREATE UNLOGGED TABLE payments (
    correlation_id UUID PRIMARY KEY,
    amount DECIMAL NOT NULL,
    processed_by TEXT NOT NULL,
    requested_at_utc TIMESTAMPTZ NOT NULL
);

CREATE INDEX CONCURRENTLY idx_payments_requested_at_utc_processed_by 
ON payments(requested_at_utc, processed_by);