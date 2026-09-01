-- CDR Events Table
CREATE TABLE IF NOT EXISTS cdr_raw.cdr_events (
    id  SERIAL PRIMARY KEY,
    msisdn VARCHAR(20),
    timestamp TIMESTAMP,
    call_duration INTEGER,
    data_usage_mb  NUMERIC(10 , 2),
    cell_tower   VARCHAR(100),
    raw_payload  JSONB,
    created_at   TIMESTAMP DEFAULT NOW()
);

-- CRM Events Table
CREATE TABLE IF NOT EXISTS crm_cdc.crm_events(
    id  SERIAL PRIMARY KEY,
    event_type VARCHAR(50),
    table_name VARCHAR(100),
    record_id  INTEGER,
    payload    JSONB,
    timestamp  TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Forex Events Table 
CREATE TABLE IF NOT EXISTS forex_stream.forex_events(
    id   SERIAL PRIMARY KEY,
    currency_pair VARCHAR(10),
    rate   NUMERIC(18,6),
    timestamp  TIMESTAMP,
    raw_payload JSONB,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_cdr_msisdn ON cdr_raw.cdr_events (msisdn);
CREATE INDEX IF NOT EXISTS idx_cdr_timestamp ON cdr_raw.cdr_events (timestamp);
CREATE INDEX IF NOT EXISTS idx_crm_record_id ON crm_cdc.crm_events (record_id);
CREATE INDEX IF NOT EXISTS idx_crm_timestamp ON crm_cdc.crm_events (timestamp);
CREATE INDEX IF NOT EXISTS idx_forex_timestamp ON forex_stream.forex_events (timestamp);
