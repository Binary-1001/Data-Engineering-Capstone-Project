# Data Engineering Project 2024

## 📁 Project Structure
```
PROJECT_HOME/
├── cdr/                    # Generates CDR data and uploads to SFTP
├── cdr_consumer/           # Consumes CDR events from Redpanda and validates them
├── crm/                    # Generates CRM data and registers Debezium CDC connector
├── forex/                  # Generates forex tick data and produces to Redpanda
├── persistence_layer/      # Persists Redpanda topic data to PostgreSQL
├── pgsql/                  # Main PostgreSQL instance (wtc_prod, wtc_analytics)
├── pgsql_persistance/      # Secondary PostgreSQL instance (telecom_platform)
├── sftp_consumer/          # Downloads CDR files from SFTP and produces to Redpanda
├── stream_processor/       # Aggregates CDR data and writes daily summaries to PostgreSQL
├── usage_api/              # REST API for querying usage summaries
├── test/                   # Project-level unit tests
├── docker-compose.yml
└── reset-env.sh
```

---

## 🔄 Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DATA GENERATORS                                    │
│                                                                             │
│   ┌─────────┐        ┌─────────┐        ┌─────────┐                        │
│   │   cdr   │        │   crm   │        │  forex  │                        │
│   └────┬────┘        └────┬────┘        └────┬────┘                        │
└────────┼─────────────────┼─────────────────┼───────────────────────────────┘
         │ CSV files        │ SQL inserts      │ tick events
         ▼                  ▼                  ▼
    ┌─────────┐      ┌────────────┐     ┌─────────────────────┐
    │  sftp   │      │  postgres  │     │                     │
    └────┬────┘      │ (wtc_prod) │     │                     │
         │           └─────┬──────┘     │                     │
         │ download        │ CDC        │                     │
         ▼                 ▼            │                     │
  ┌──────────────┐  ┌──────────────┐   │  Redpanda Cluster   │
  │sftp_consumer │  │   debezium   │   │  (redpanda-0/1/2)   │
  └──────┬───────┘  └──────┬───────┘   │                     │
         │ cdr-data         │ crm.*     │                     │
         │ cdr-voice        └──────────►│                     │
         └─────────────────────────────►│                     │
                                        └──────────┬──────────┘
                                                   │
                          ┌────────────────────────┼────────────────────────┐
                          │                        │                        │
                          ▼                        ▼                        ▼
                  ┌──────────────┐      ┌──────────────────┐    ┌──────────────────┐
                  │cdr_consumer  │      │ stream_processor │    │persistence_layer │
                  │ (validates)  │      │  (aggregates)    │    │  (raw storage)   │
                  └──────────────┘      └────────┬─────────┘    └────────┬─────────┘
                                                 │                       │
                                                 ▼                       ▼
                                        ┌─────────────────┐   ┌──────────────────────┐
                                        │    postgres      │   │  postgres_persistence│
                                        │ (wtc_analytics)  │   │  (telecom_platform)  │
                                        │ prepared_layers  │   │  analytics schema    │
                                        └────────┬─────────┘   └──────────────────────┘
                                                 │
                                                 ▼
                                        ┌─────────────────┐
                                        │   usage_api     │
                                        │ :18089/data_usage│
                                        └─────────────────┘
```

---

## ⚙️ Requirements
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) with the following resource settings:
  - CPU: >= 8
  - Memory: >= 8GB
  - Swap: >= 1GB
  - Virtual Disk: >= 64GB

---

## 🚀 Start the Environment

**🪟 Windows:**
```bash
docker compose up -d
```

**🐧 Linux / 🍎 macOS:**
```bash
./reset-env.sh
```

---

## ✅ Verify Containers
```bash
docker compose ps
```

All containers should show a status of `running` or `exited` (generators exit with code 0 once done).

---

## 🔗 Register the Debezium CDC Connector
Debezium must be running before registering the connector.

**Check Debezium is running:**
```bash
docker compose ps | grep debezium
```

**Register the connector:**
```bash
cd crm
python3 connector.py
```

This connects Debezium to Redpanda and starts streaming CRM change events from PostgreSQL.

---

## 🌐 Usage API
Once the environment is running, the API is available at:
```
http://localhost:18089/data_usage?msisdn=<msisdn>&start_time=<YYYYMMDDHHmmss>&end_time=<YYYYMMDDHHmmss>
```

**Example request:**
```
http://localhost:18089/data_usage?msisdn=2712345678&start_time=20240101000000&end_time=20240101235959
```

**Authentication:** HTTP Basic Auth
- Username: `admin`
- Password: `admin`

**Example response:**
```json
{
  "msisdn": "2712345678",
  "start_time": "2024-01-01 00:00:00",
  "end_time": "2024-01-01 23:59:59",
  "usage": [
    {
      "category": "data",
      "usage_type": "video",
      "total": 12312323,
      "measure": "bytes",
      "start_time": "2024-01-01 00:00:00"
    },
    {
      "category": "call",
      "usage_type": "voice",
      "total": 89,
      "measure": "seconds",
      "start_time": "2024-01-01 00:00:00"
    }
  ]
}
```

---

## 🧪 Running Tests

**CRM tests:**
```bash
cd crm
python3 -m pytest tests/ -v
```

**Project-level tests:**
```bash
python3 -m pytest test/ -v
```

---

## 🖥️ Redpanda Console
View topics and messages at:
```
http://localhost:18084
```

---

## 📦 Services Overview

| Service | Description |
|---|---|
| `postgres` | Main database — `wtc_prod` (CRM) and `wtc_analytics` (prepared layers) |
| `postgres_persistence` | Secondary database — `telecom_platform` |
| `sftp` | SFTP server where CDR files are delivered |
| `redpanda-0/1/2` | Three-node Redpanda (Kafka) cluster |
| `redpanda-console` | Redpanda UI at `http://localhost:18084` |
| `cdr` | Generates and uploads CDR data/voice CSV files to SFTP |
| `crm` | Generates CRM account data into PostgreSQL |
| `forex` | Generates forex tick data to Redpanda `tick-data` topic |
| `sftp_consumer` | Downloads CDR files from SFTP, produces rows to Redpanda |
| `cdr_consumer` | Consumes and validates CDR events from Redpanda |
| `debezium` | CDC connector — streams CRM changes from PostgreSQL to Redpanda |
| `stream_processor` | Aggregates CDR data into daily summaries in PostgreSQL |
| `persistence_layer` | Persists Redpanda topic data to PostgreSQL |
| `usage_api` | REST API for querying usage summaries at `http://localhost:18089` |

---

See https://dev.curriculum.wethinkco.de/dataengineering/de-project/project-intro/ for full project instructions.
