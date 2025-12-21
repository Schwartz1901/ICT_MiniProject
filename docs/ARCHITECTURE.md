# Medallion Data Pipeline Architecture

## Overview

This project implements a **Medallion Architecture** data pipeline for real estate data, following the Bronze-Silver-Gold pattern for data lakehouse design. The pipeline ingests data from MongoDB, streams through Kafka, and transforms it through multiple layers in Snowflake for analytics and machine learning.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│    ┌──────────────┐                                                             │
│    │   MongoDB    │  Real estate listings                                       │
│    │   (Atlas)    │  Database: Bigdata                                          │
│    │              │  Collection: details                                        │
│    └──────┬───────┘                                                             │
│           │                                                                     │
└───────────┼─────────────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         STREAMING LAYER                                         │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│    ┌──────────────┐     Batch Processing      ┌──────────────┐                  │
│    │  Ingestion   │ ────────────────────────► │    Kafka     │                  │
│    │  (Producer)  │     BATCH_SIZE=100        │   (Aiven)    │                  │
│    └──────────────┘                           │              │                  │
│                                               │  Topic:      │                  │
│                                               │  data_       │                  │
│                                               │  ingestion   │                  │
│                                               └──────┬───────┘                  │
│                                                      │                          │
└──────────────────────────────────────────────────────┼──────────────────────────┘
                                                       │
                                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         DATA WAREHOUSE (Snowflake)                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────┐     │
│  │                        BRONZE LAYER (RAW_DB)                           │     │
│  │  ┌──────────────────────────────────────────────────────────────────┐  │     │
│  │  │  LANDING.BRONZE_REAL_ESTATE                                      │  │     │
│  │  │  - id (VARCHAR)                                                  │  │     │
│  │  │  - raw_data (VARIANT) ◄── Raw JSON from Kafka                    │  │     │
│  │  │  - ingested_at (TIMESTAMP)                                       │  │     │
│  │  │  - kafka_partition, kafka_offset                                 │  │     │
│  │  └──────────────────────────────────────────────────────────────────┘  │     │
│  └────────────────────────────────────────────────────────────────────────┘     │
│                                       │                                         │
│                                       ▼ Transform & Clean                       │
│  ┌────────────────────────────────────────────────────────────────────────┐     │
│  │                       SILVER LAYER (STAGING_DB)                        │     │
│  │  ┌──────────────────────────────────────────────────────────────────┐  │     │
│  │  │  CLEANED.SILVER_REAL_ESTATE                                      │  │     │
│  │  │  - Structured columns (price, area, bedrooms, etc.)              │  │     │
│  │  │  - data_quality_score (0.0 - 1.0)                                │  │     │
│  │  │  - Cleaned & validated data                                      │  │     │
│  │  └──────────────────────────────────────────────────────────────────┘  │     │
│  └────────────────────────────────────────────────────────────────────────┘     │
│                                       │                                         │
│                                       ▼ Feature Engineering                     │
│  ┌────────────────────────────────────────────────────────────────────────┐     │
│  │                        GOLD LAYER (ANALYTICS_DB)                       │     │
│  │  ┌─────────────────────┐ ┌─────────────────────┐ ┌──────────────────┐  │     │
│  │  │  DWH.GOLD_ML_       │ │  DWH.GOLD_MARKET_   │ │  DWH.GOLD_PRICE_ │  │     │
│  │  │  FEATURES           │ │  ANALYTICS          │ │  TRENDS          │  │     │
│  │  │  - price_per_sqm    │ │  - Aggregated stats │ │  - Time series   │  │     │
│  │  │  - price_category   │ │  - By city/district │ │  - By city/month │  │     │
│  │  │  - area_category    │ │                     │ │                  │  │     │
│  │  │  - is_high_value    │ │                     │ │                  │  │     │
│  │  └─────────────────────┘ └─────────────────────┘ └──────────────────┘  │     │
│  └────────────────────────────────────────────────────────────────────────┘     │
│                                       │                                         │
│                                       ▼ ML & Analytics                          │
│  ┌────────────────────────────────────────────────────────────────────────┐     │
│  │                          ML RESULTS LAYER                              │     │
│  │  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────────────┐   │     │
│  │  │ ANOMALY_ALERTS  │ │ PRICE_          │ │ MODEL_REGISTRY          │   │     │
│  │  │ - Price outliers│ │ PREDICTIONS     │ │ FEATURE_IMPORTANCE      │   │     │
│  │  │ - Severity score│ │                 │ │                         │   │     │
│  │  └─────────────────┘ └─────────────────┘ └─────────────────────────┘   │     │
│  └────────────────────────────────────────────────────────────────────────┘     │
│                                       │                                         │
│                                       ▼                                         │
│  ┌────────────────────────────────────────────────────────────────────────┐     │
│  │                         SEMANTIC LAYER (Views)                         │     │
│  │  V_PROPERTY_LISTINGS | V_MARKET_SUMMARY | V_PRICE_TRENDS | V_ANOMALIES │     │
│  └────────────────────────────────────────────────────────────────────────┘     │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
ICT_MiniProject/
├── docs/
│   ├── ARCHITECTURE.md          # This file
│   ├── IMPLEMENTATION_GUIDE.md  # Step-by-step implementation guide
│   └── WBS - Mini Project.md    # Work breakdown structure
├── src/
│   ├── __init__.py
│   ├── logging_config.py        # Centralized logging configuration
│   ├── pipeline_orchestrator.py # Main pipeline orchestrator
│   ├── snowflake_setup.py       # Snowflake infrastructure setup
│   │
│   ├── kafka_client/            # Kafka connection module
│   │   ├── __init__.py
│   │   ├── config.py            # Producer/Consumer configuration
│   │   └── ca.pem               # SSL certificate
│   │
│   ├── mongo/                   # MongoDB connection module
│   │   ├── __init__.py
│   │   └── config.py            # MongoDB client configuration
│   │
│   ├── snowflake_client/        # Snowflake connection module
│   │   ├── __init__.py
│   │   └── config.py            # Snowflake connection helpers
│   │
│   ├── pipeline_steps/          # Pipeline step implementations
│   │   ├── __init__.py
│   │   ├── ingestion.py         # MongoDB → Kafka
│   │   ├── bronze.py            # Kafka → Snowflake Bronze
│   │   ├── silver.py            # Bronze → Silver transformation
│   │   ├── gold.py              # Silver → Gold transformation
│   │   └── ml.py                # ML & anomaly detection
│   │
│   └── sql/                     # SQL query files
│       ├── __init__.py          # SQL loader utility
│       ├── setup/               # Infrastructure DDL (13 files)
│       ├── bronze/              # Bronze layer queries (3 files)
│       ├── silver/              # Silver layer queries (5 files)
│       ├── gold/                # Gold layer queries (9 files)
│       └── ml/                  # ML layer queries (9 files)
│
├── .env                         # Environment variables (secrets)
├── .env.example                 # Environment template
├── .gitignore
└── requirements.txt             # Python dependencies
```

## Technology Stack

| Component            | Technology           | Purpose                        |
|----------------------|----------------------|--------------------------------|
| Source Database      | MongoDB Atlas        | Real estate data storage       |
| Message Broker       | Apache Kafka (Aiven) | Event streaming                |
| Data Warehouse       | Snowflake            | Medallion architecture storage |
| Programming Language | Python 3.11          | Pipeline implementation        |
| Configuration        | python-dotenv        | Environment management         |

### Python Dependencies

```
pymongo[srv]              # MongoDB driver with SRV support
kafka-python              # Apache Kafka client
snowflake-connector-python # Snowflake driver
python-dotenv             # Environment variable management
certifi                   # SSL certificate handling
```

## Data Flow

### 1. Ingestion Layer (MongoDB → Kafka)

```python
# pipeline_steps/ingestion.py
MongoDB (Bigdata.details)
    → Batch read (100 documents)
    → JSON serialization
    → Kafka Producer
    → Topic: data_ingestion
```

**Key Features:**
- Batch processing with configurable `BATCH_SIZE=100`
- ObjectId to string conversion for JSON compatibility
- Producer flush after each batch for durability

### 2. Bronze Layer (Kafka → Snowflake)

```python
# pipeline_steps/bronze.py
Kafka Consumer (data_ingestion)
    → JSON parsing
    → Raw storage in VARIANT column
    → Kafka metadata (partition, offset)
```

**Schema:**
```sql
BRONZE_REAL_ESTATE (
    id VARCHAR,
    raw_data VARIANT,        -- Raw JSON
    ingested_at TIMESTAMP,
    source VARCHAR,
    kafka_partition INT,
    kafka_offset INT
)
```

### 3. Silver Layer (Data Cleaning)

```python
# pipeline_steps/silver.py
Bronze (raw JSON)
    → Field extraction (TRY_CAST)
    → Data cleaning (TRIM)
    → Quality scoring
    → Structured storage
```

**Data Quality Score Calculation:**
```sql
(
    CASE WHEN price > 0 THEN 0.2 ELSE 0 END +
    CASE WHEN area > 0 THEN 0.2 ELSE 0 END +
    CASE WHEN location IS NOT NULL THEN 0.2 ELSE 0 END +
    CASE WHEN bedrooms IS NOT NULL THEN 0.2 ELSE 0 END +
    CASE WHEN city IS NOT NULL THEN 0.2 ELSE 0 END
) AS data_quality_score
```

### 4. Gold Layer (Feature Engineering)

```python
# pipeline_steps/gold.py
Silver (cleaned data)
    → Price percentile calculation (P25, P50, P75)
    → Feature derivation (price_per_sqm, categories)
    → Market aggregations
    → Time series trends
```

**Derived Features:**

| Feature          | Logic                                      |
|------------------|--------------------------------------------|
| `price_per_sqm`  | `price / area`                             |
| `price_category` | Budget/Standard/Premium/Luxury (quartiles) |
| `area_category`  | Small/Medium/Large/Very Large              |
| `is_high_value`  | `price > P75`                              |
| `rooms_total`    | `bedrooms + bathrooms`                     |

### 5. ML Layer (Anomaly Detection)

```python
# pipeline_steps/ml.py
Gold (ML features)
    → Statistical analysis (mean, std by city)
    → Z-score anomaly detection (threshold: 3σ)
    → Severity classification
    → Alert generation
```

**Anomaly Detection Logic:**
```sql
anomaly_score = ABS(price - city_avg) / city_std
severity = CASE
    WHEN anomaly_score > 4 THEN 'HIGH'
    WHEN anomaly_score > 3 THEN 'MEDIUM'
    ELSE 'LOW'
END
```

## Database Schema

### Snowflake Databases

| Database       | Layer     | Purpose                  |
|----------------|-----------|--------------------------|
| `RAW_DB`       | Bronze    | Raw data landing zone    |
| `STAGING_DB`   | Silver    | Cleaned/transformed data |
| `ANALYTICS_DB` | Gold + ML | Analytics-ready data     |

### Schema Organization

```
RAW_DB
└── LANDING
    └── BRONZE_REAL_ESTATE

STAGING_DB
└── CLEANED
    └── SILVER_REAL_ESTATE

ANALYTICS_DB
├── DWH
│   ├── GOLD_ML_FEATURES
│   ├── GOLD_MARKET_ANALYTICS
│   └── GOLD_PRICE_TRENDS
├── ML_RESULTS
│   ├── PRICE_PREDICTIONS
│   ├── ANOMALY_ALERTS
│   ├── MODEL_REGISTRY
│   └── FEATURE_IMPORTANCE
└── SEMANTIC
    ├── V_PROPERTY_LISTINGS
    ├── V_MARKET_SUMMARY
    ├── V_PRICE_TRENDS
    └── V_ACTIVE_ANOMALIES
```

## Pipeline Orchestration

### Command Line Interface

```bash
# Full pipeline
python pipeline_orchestrator.py --full

# Skip ingestion (when Kafka unavailable)
python pipeline_orchestrator.py --full --skip-ingestion

# Transformations only (Silver → Gold → ML)
python pipeline_orchestrator.py --transform

# Individual steps
python pipeline_orchestrator.py --step ingestion
python pipeline_orchestrator.py --step bronze
python pipeline_orchestrator.py --step silver
python pipeline_orchestrator.py --step gold
python pipeline_orchestrator.py --step ml

# Snowflake setup only
python pipeline_orchestrator.py --setup

# Custom bronze timeout
python pipeline_orchestrator.py --full --bronze-timeout 120
```

### Pipeline Execution Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    PIPELINE ORCHESTRATOR                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  [Step 0] Snowflake Setup                                   │
│     └── Create databases, schemas, tables, views            │
│                          │                                  │
│                          ▼                                  │
│  [Step 1] Ingestion (--skip-ingestion to skip)              │
│     └── MongoDB → Kafka                                     │
│                          │                                  │
│                          ▼                                  │
│  [Step 2] Bronze Loader                                     │
│     └── Kafka → Snowflake RAW_DB                            │
│                          │                                  │
│                          ▼                                  │
│  [Step 3] Silver Transformation                             │
│     └── RAW_DB → STAGING_DB (cleaning + quality scoring)    │
│                          │                                  │
│                          ▼                                  │
│  [Step 4] Gold Transformation                               │
│     └── STAGING_DB → ANALYTICS_DB (feature engineering)     │
│                          │                                  │
│                          ▼                                  │
│  [Step 5] ML Pipeline                                       │
│     └── Anomaly detection → ML_RESULTS                      │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Configuration

### Environment Variables (.env)

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=<kafka-host>:<port>
KAFKA_USERNAME=<username>
KAFKA_PASSWORD=<password>
KAFKA_TOPIC=data_ingestion

# MongoDB Configuration
MONGO_URI=mongodb+srv://<user>:<pass>@<cluster>
MONGO_DATABASE=<database>

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=<account>.<region>
SNOWFLAKE_USER=<username>
SNOWFLAKE_PASSWORD=<password>
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=RAW_DB
SNOWFLAKE_SCHEMA=LANDING
```

### SQL Query Management

All SQL queries are externalized to `.sql` files for maintainability:

```python
from sql import load_query

# Load and execute a query
query = load_query("silver", "transform_bronze_to_silver")
cursor.execute(query)

# Parameterized queries
query = load_query("gold", "merge_ml_features").format(
    min_quality_score=0.6,
    p25=1000000,
    p50=3000000,
    p75=7000000
)
```

## Security Considerations

1. **Secrets Management**: All credentials stored in `.env` file (not in code)
2. **SSL/TLS**: Kafka connections use SASL_SSL with CA certificate
3. **MongoDB**: Uses SRV connection with SSL via `certifi`
4. **Git Security**: `.env` excluded via `.gitignore`

## Error Handling

- Each pipeline step has try/except/finally blocks
- Connections are properly closed in `finally` blocks
- Detailed logging with timestamps and module names
- Graceful degradation with `--skip-ingestion` flag

## Monitoring & Observability

### Logging

```python
# Centralized logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
```

### Statistics & Metrics

Each layer provides statistics:
- **Bronze**: Total records, unique IDs, ingestion timestamps
- **Silver**: Quality score distribution, record counts
- **Gold**: Category distributions, price statistics
- **ML**: Anomaly counts by type and severity

## Future Enhancements

1. **Scheduling**: Add Apache Airflow for automated scheduling
2. **Monitoring**: Integrate with Prometheus/Grafana
3. **ML Models**: Add scikit-learn models for price prediction
4. **Data Quality**: Implement Great Expectations for validation
5. **CDC**: Add Change Data Capture for real-time updates
6. **Testing**: Add pytest unit and integration tests
