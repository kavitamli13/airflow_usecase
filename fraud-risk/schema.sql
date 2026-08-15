-- ==========================================================================
-- PostgreSQL: operational store (latest scored transactions)
-- ==========================================================================
CREATE TABLE IF NOT EXISTS transactions_scored (
    transaction_id      TEXT PRIMARY KEY,
    account_id          TEXT NOT NULL,
    amount              NUMERIC(12, 2) NOT NULL,
    merchant_category   TEXT NOT NULL,
    location            TEXT,
    event_time          TIMESTAMPTZ NOT NULL,
    fraud_probability   DOUBLE PRECISION NOT NULL,
    is_flagged          SMALLINT NOT NULL,
    scored_at           TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_txn_scored_account ON transactions_scored (account_id);
CREATE INDEX IF NOT EXISTS idx_txn_scored_flagged ON transactions_scored (is_flagged, scored_at);


-- ==========================================================================
-- ClickHouse: analytics store (feeds Superset)
-- ==========================================================================
CREATE TABLE IF NOT EXISTS analytics.transactions_scored_analytics
(
    transaction_id      String,
    account_id          String,
    amount              Decimal(12, 2),
    merchant_category   LowCardinality(String),
    location            LowCardinality(String),
    fraud_probability   Float64,
    is_flagged          UInt8,
    scored_at           DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(scored_at)
ORDER BY (scored_at, account_id);

-- Pre-aggregated daily rollup — this is what the Superset dashboard queries
-- directly, so it stays fast regardless of raw event volume.
CREATE TABLE IF NOT EXISTS analytics.fraud_metrics_daily
(
    metric_date            Date,
    total_transactions     UInt64,
    flagged_transactions   UInt64,
    flagged_amount         Decimal(14, 2),
    avg_fraud_probability  Float64
)
ENGINE = MergeTree()
ORDER BY metric_date;


-- ==========================================================================
-- Iceberg (via Nessie catalog): full history for retraining + audit
-- Run via Spark SQL with the Nessie catalog configured (see spark_streaming_scorer.py header)
-- ==========================================================================
CREATE TABLE IF NOT EXISTS nessie.fraud.transactions_scored_history (
    transaction_id      STRING,
    account_id           STRING,
    amount               DOUBLE,
    merchant_category    STRING,
    location             STRING,
    event_time           TIMESTAMP,
    fraud_probability    DOUBLE,
    is_flagged           INT,
    scored_at            TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(event_time));
