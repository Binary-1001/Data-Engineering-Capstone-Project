-- ================================================================
-- Analytics Schema & Prepared Tables
-- File: pgsql_persistence/analytics_schema.sql
-- ================================================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- ================================================================
-- Table 1: analytics_cdr_summary
-- Pre-aggregated call & data metrics per SIM card
-- Source: cdr_raw.cdr_events
-- ================================================================
CREATE TABLE IF NOT EXISTS analytics.analytics_cdr_summary (
    id                   SERIAL PRIMARY KEY,
    msisdn               VARCHAR(20),
    event_date           DATE,                  -- ← new: the date being summarised
    total_calls          INTEGER      DEFAULT 0,
    total_duration_sec   BIGINT       DEFAULT 0,
    total_data_mb        NUMERIC(12,2) DEFAULT 0,
    last_cell_tower      VARCHAR(100),
    first_seen           TIMESTAMP,
    last_seen            TIMESTAMP,
    updated_at           TIMESTAMP DEFAULT NOW(),
    UNIQUE (msisdn, event_date)                 -- ← prevents duplicate summaries
);

-- ================================================================
-- Table 2: analytics_crm_flat
-- One row per CRM record — Debezium 'after' payload unpacked
-- Source: crm_cdc.crm_events (payload->after JSONB)
-- entity_type maps to topic: crm.accounts / crm.addresses / crm.devices
-- ================================================================
CREATE TABLE IF NOT EXISTS analytics.analytics_crm_flat (
    id               SERIAL PRIMARY KEY,
    record_id        INTEGER,       -- account_id or device_id from after{}
    entity_type      VARCHAR(50),   -- which topic it came from
    operation        VARCHAR(10),   -- op field: 'c', 'u', 'd'

    -- From crm.accounts
    account_name     VARCHAR(255),
    account_status   VARCHAR(50),
    msisdn           VARCHAR(20),   -- lets you JOIN to cdr_summary

    -- From crm.addresses
    city             VARCHAR(100),
    country          VARCHAR(100),

    -- From crm.devices
    device_type      VARCHAR(100),
    device_model     VARCHAR(100),

    source_timestamp TIMESTAMP,     -- ts_ms from Debezium, parsed to datetime
    updated_at       TIMESTAMP DEFAULT NOW()
);

-- ================================================================
-- Table 3: analytics_forex_rates
-- Clean typed rows — raw_payload stripped
-- Source: forex_stream.forex_events
-- ================================================================
CREATE TABLE IF NOT EXISTS analytics.analytics_forex_rates (
    id            SERIAL PRIMARY KEY,
    currency_pair VARCHAR(10),
    rate          NUMERIC(18,6),
    captured_at   TIMESTAMP,     -- the original event timestamp
    created_at    TIMESTAMP DEFAULT NOW()
);

-- ================================================================
-- Indexes
-- ================================================================
CREATE INDEX IF NOT EXISTS idx_ana_cdr_msisdn
    ON analytics.analytics_cdr_summary (msisdn);
CREATE INDEX IF NOT EXISTS idx_ana_cdr_last_seen
    ON analytics.analytics_cdr_summary (last_seen);

CREATE INDEX IF NOT EXISTS idx_ana_crm_record_id
    ON analytics.analytics_crm_flat (record_id);
CREATE INDEX IF NOT EXISTS idx_ana_crm_entity_type
    ON analytics.analytics_crm_flat (entity_type);
CREATE INDEX IF NOT EXISTS idx_ana_crm_msisdn
    ON analytics.analytics_crm_flat (msisdn);

CREATE INDEX IF NOT EXISTS idx_ana_forex_pair
    ON analytics.analytics_forex_rates (currency_pair);
CREATE INDEX IF NOT EXISTS idx_ana_forex_captured_at
    ON analytics.analytics_forex_rates (captured_at);