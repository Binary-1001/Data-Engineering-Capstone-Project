-- ================================================================
-- Indexing & Query Optimisation
-- File: pgsql_persistance/indexes.sql
-- All indexes consolidated in one place for visibility
-- ================================================================

-- ================================================================
-- RAW LAYER INDEXES
-- Already created in migrations.sql — listed here for reference
-- ================================================================

-- CDR raw
CREATE INDEX IF NOT EXISTS idx_cdr_msisdn
    ON cdr_raw.cdr_events (msisdn);

CREATE INDEX IF NOT EXISTS idx_cdr_timestamp
    ON cdr_raw.cdr_events (timestamp);

-- CRM raw
CREATE INDEX IF NOT EXISTS idx_crm_record_id
    ON crm_cdc.crm_events (record_id);

CREATE INDEX IF NOT EXISTS idx_crm_timestamp
    ON crm_cdc.crm_events (timestamp);

-- Forex raw
CREATE INDEX IF NOT EXISTS idx_forex_timestamp
    ON forex_stream.forex_events (timestamp);

CREATE INDEX IF NOT EXISTS idx_forex_currency_pair
    ON forex_stream.forex_events (currency_pair);

-- ================================================================
-- ANALYTICS LAYER INDEXES
-- Already created in analytics_schema.sql — listed here for reference
-- ================================================================

-- CDR summary
CREATE INDEX IF NOT EXISTS idx_ana_cdr_msisdn
    ON analytics.analytics_cdr_summary (msisdn);

CREATE INDEX IF NOT EXISTS idx_ana_cdr_last_seen
    ON analytics.analytics_cdr_summary (last_seen);

CREATE INDEX IF NOT EXISTS idx_ana_cdr_event_date
    ON analytics.analytics_cdr_summary (event_date);

-- CRM flat
CREATE INDEX IF NOT EXISTS idx_ana_crm_record_id
    ON analytics.analytics_crm_flat (record_id);

CREATE INDEX IF NOT EXISTS idx_ana_crm_entity_type
    ON analytics.analytics_crm_flat (entity_type);

CREATE INDEX IF NOT EXISTS idx_ana_crm_msisdn
    ON analytics.analytics_crm_flat (msisdn);

-- Forex rates
CREATE INDEX IF NOT EXISTS idx_ana_forex_pair
    ON analytics.analytics_forex_rates (currency_pair);

CREATE INDEX IF NOT EXISTS idx_ana_forex_captured_at
    ON analytics.analytics_forex_rates (captured_at);