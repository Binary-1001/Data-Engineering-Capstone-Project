#!/bin/bash
psql -U postgres -d wtc_analytics -c "CREATE SCHEMA IF NOT EXISTS hvs;"
psql -U postgres -d wtc_analytics -c "CREATE TABLE IF NOT EXISTS hvs.daily_usage_summary (
    id BIGSERIAL PRIMARY KEY,
    msisdn VARCHAR(20) NOT NULL,
    category VARCHAR(20) NOT NULL,
    usage_type VARCHAR(50) NOT NULL,
    total_usage NUMERIC NOT NULL,
    measure VARCHAR(20) NOT NULL,
    total_cost NUMERIC(12,4) NOT NULL,
    summary_timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);"
psql -U postgres -d wtc_analytics -c "CREATE INDEX idx_usage_msisdn ON hvs.daily_usage_summary(msisdn);"
psql -U postgres -d wtc_analytics -c "CREATE INDEX idx_usage_timestamp ON hvs.daily_usage_summary(summary_timestamp);"
