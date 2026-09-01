#!/bin/bash
psql -U postgres -d postgres -c "CREATE DATABASE telecom_platform;"
psql -U postgres -d telecom_platform -c "CREATE SCHEMA cdr_raw;"
psql -U postgres -d telecom_platform -c "CREATE SCHEMA crm_cdc;"
psql -U postgres -d telecom_platform -c "CREATE SCHEMA forex_stream;"
psql -U postgres -d telecom_platform -f /docker-entrypoint-initdb.d/migrations.sql
psql -U postgres -d telecom_platform -f /docker-entrypoint-initdb.d/analytics_schema.sql
psql -U postgres -d telecom_platform -f /docker-entrypoint-initdb.d/indexes.sql