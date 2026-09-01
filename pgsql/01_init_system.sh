#!/bin/bash
psql -U postgres -d postgres -c "CREATE DATABASE wtc_prod;";
psql -U postgres -d wtc_prod -c "CREATE SCHEMA crm_system;";
psql -U postgres -d wtc_prod -c "CREATE TABLE IF NOT EXISTS crm_system.accounts (account_id INTEGER PRIMARY KEY, owner_name VARCHAR(100), email VARCHAR(100), phone_number VARCHAR(100), modified_ts TIMESTAMP);";
psql -U postgres -d wtc_prod -c "CREATE TABLE IF NOT EXISTS crm_system.addresses (account_id INTEGER PRIMARY KEY, street_address VARCHAR(100), city VARCHAR(100), state VARCHAR(100), postal_code VARCHAR(100), country VARCHAR(100), modified_ts TIMESTAMP );";
psql -U postgres -d wtc_prod -c "CREATE TABLE IF NOT EXISTS crm_system.devices ( device_id INTEGER PRIMARY KEY, account_id INTEGER, device_name VARCHAR(100), device_type VARCHAR(100), device_os VARCHAR(100), modified_ts TIMESTAMP );";
psql -U postgres -d postgres -c "CREATE DATABASE wtc_analytics;";
psql -U postgres -d postgres -c "CREATE DATABASE airflow;";
psql -U postgres -d wtc_analytics -c "CREATE SCHEMA cdr_data;";
psql -U postgres -d wtc_analytics -c "CREATE SCHEMA crm_data;";
psql -U postgres -d wtc_analytics -c "CREATE SCHEMA forex_data;";
psql -U postgres -d wtc_analytics -c "CREATE SCHEMA prepared_layers;";
psql -U postgres -d postgres -c "CREATE SCHEMA airflow;";
psql -U postgres -d wtc_analytics -c "CREATE TABLE IF NOT EXISTS prepared_layers.cdr_daily_data_summary (msisdn VARCHAR(20), date DATE, usage_type VARCHAR(20), up_bytes BIGINT DEFAULT 0, down_bytes BIGINT DEFAULT 0, cost_wak DOUBLE PRECISION DEFAULT 0, updated_at TIMESTAMP, PRIMARY KEY (msisdn, date, usage_type));"
psql -U postgres -d wtc_analytics -c "CREATE TABLE IF NOT EXISTS prepared_layers.cdr_daily_voice_summary (msisdn VARCHAR(20), date DATE, usage_type VARCHAR(20), call_duration_sec BIGINT DEFAULT 0, cost_wak DOUBLE PRECISION DEFAULT 0, updated_at TIMESTAMP, PRIMARY KEY (msisdn, date, usage_type));"
psql -U postgres -d wtc_analytics -c "CREATE SCHEMA IF NOT EXISTS hvs;"
psql -U postgres -d wtc_analytics -c "CREATE TABLE IF NOT EXISTS hvs.daily_usage_summary (id BIGSERIAL PRIMARY KEY, msisdn VARCHAR(20) NOT NULL, category VARCHAR(20) NOT NULL, usage_type VARCHAR(50) NOT NULL, total_usage NUMERIC NOT NULL, measure VARCHAR(20) NOT NULL, total_cost NUMERIC(12,4) NOT NULL, summary_timestamp TIMESTAMP NOT NULL, created_at TIMESTAMP DEFAULT NOW());"
psql -U postgres -d wtc_analytics -c "CREATE INDEX idx_usage_msisdn ON hvs.daily_usage_summary(msisdn);"
psql -U postgres -d wtc_analytics -c "CREATE INDEX idx_usage_timestamp ON hvs.daily_usage_summary(summary_timestamp);"
