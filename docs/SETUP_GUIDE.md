# Setup and Usage Guide

## Prerequisites

- Python 3.11+
- Git
- Access to:
  - MongoDB Atlas account
  - Aiven Kafka cluster (or alternative Kafka provider)
  - Snowflake account

## Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd ICT_MiniProject
```

### 2. Create Virtual Environment

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/Mac
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

**Required packages:**

| Package                    | Version | Purpose                  |
|----------------------------|---------|--------------------------|
| pymongo[srv]               | latest  | MongoDB driver           |
| kafka-python               | latest  | Apache Kafka client      |
| snowflake-connector-python | latest  | Snowflake driver         |
| python-dotenv              | latest  | Environment management   |
| certifi                    | latest  | SSL certificate handling |

## Configuration

### 1. Create Environment File

Copy the example environment file:

```bash
cp .env.example .env
```

### 2. Configure Services

Edit `.env` with your credentials:

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=<your-kafka-host>:<port>
KAFKA_USERNAME=<username>
KAFKA_PASSWORD=<password>
KAFKA_TOPIC=data_ingestion

# MongoDB Configuration
MONGO_URI=mongodb+srv://<user>:<password>@<cluster-url>
MONGO_DATABASE=Bigdata

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=<account-id>.<region>
SNOWFLAKE_USER=<username>
SNOWFLAKE_PASSWORD=<password>
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=RAW_DB
SNOWFLAKE_SCHEMA=LANDING
```

### 3. Kafka SSL Certificate

Place your Kafka CA certificate at:

```
src/kafka_client/ca.pem
```

For Aiven Kafka:
1. Go to Aiven Console → Your Kafka Service → Overview
2. Download "CA Certificate"
3. Save as `src/kafka_client/ca.pem`

## Service Setup

### MongoDB Atlas

1. Create a MongoDB Atlas account at https://cloud.mongodb.com
2. Create a cluster (free tier available)
3. Create database `Bigdata` with collection `details`
4. Add your IP to the whitelist
5. Create a database user
6. Get the connection string (SRV format)

### Aiven Kafka

1. Create an Aiven account at https://console.aiven.io
2. Create a Kafka service (free trial available)
3. Create topic `data_ingestion`
4. Download the CA certificate
5. Note the connection details:
   - Service URI (bootstrap server)
   - Username (usually `avnadmin`)
   - Password

### Snowflake

1. Create a Snowflake account at https://signup.snowflake.com
2. Note your account identifier (e.g., `abc12345.us-east-1`)
3. Create a user with appropriate permissions
4. The pipeline will create databases/schemas automatically

## Usage

### Navigate to Source Directory

```bash
cd src
```

### Run Full Pipeline

```bash
python pipeline_orchestrator.py --full
```

This executes all steps:
1. Snowflake setup (databases, schemas, tables)
2. Ingestion (MongoDB → Kafka)
3. Bronze loading (Kafka → Snowflake)
4. Silver transformation
5. Gold transformation
6. ML pipeline

### Skip Ingestion (Kafka Unavailable)

```bash
python pipeline_orchestrator.py --full --skip-ingestion
```

### Run Transformations Only

```bash
python pipeline_orchestrator.py --transform
```

Runs only: Silver → Gold → ML

### Run Individual Steps

```bash
# Snowflake setup only
python pipeline_orchestrator.py --setup

# Individual pipeline steps
python pipeline_orchestrator.py --step ingestion
python pipeline_orchestrator.py --step bronze
python pipeline_orchestrator.py --step silver
python pipeline_orchestrator.py --step gold
python pipeline_orchestrator.py --step ml
```

### Custom Bronze Timeout

```bash
python pipeline_orchestrator.py --full --bronze-timeout 120
```

Default timeout is 60 seconds.

## CLI Reference

| Command                              | Description                          |
|--------------------------------------|--------------------------------------|
| `--full`                             | Run complete pipeline                |
| `--transform`                        | Run transformations only             |
| `--setup`                            | Run Snowflake setup only             |
| `--step <name>`                      | Run single step                      |
| `--skip-ingestion`                   | Skip MongoDB → Kafka step            |
| `--bronze-timeout <seconds>`         | Set bronze loader timeout            |

**Available steps:** `ingestion`, `bronze`, `silver`, `gold`, `ml`

## Pipeline Steps Detail

### Step 0: Snowflake Setup

Creates the Medallion architecture infrastructure:

```
Databases:
├── RAW_DB (Bronze)
├── STAGING_DB (Silver)
└── ANALYTICS_DB (Gold + ML)

Schemas:
├── RAW_DB.LANDING
├── STAGING_DB.CLEANED
├── ANALYTICS_DB.DWH
├── ANALYTICS_DB.ML_RESULTS
└── ANALYTICS_DB.SEMANTIC
```

### Step 1: Ingestion

- Reads from MongoDB `Bigdata.details` collection
- Produces to Kafka `data_ingestion` topic
- Batch size: 100 documents

### Step 2: Bronze Loading

- Consumes from Kafka topic
- Stores raw JSON in Snowflake `VARIANT` column
- Preserves Kafka metadata (partition, offset)

### Step 3: Silver Transformation

- Extracts structured fields from JSON
- Cleans data (TRIM, type casting)
- Calculates data quality score (0.0 - 1.0)

### Step 4: Gold Transformation

- Creates ML features (price_per_sqm, categories)
- Aggregates market analytics by city/district
- Generates price trends time series

### Step 5: ML Pipeline

- Detects price anomalies using z-scores
- Classifies severity (LOW, MEDIUM, HIGH)
- Stores alerts for monitoring

## Monitoring

### View Logs

The pipeline outputs detailed logs:

```
2025-12-21 20:51:53 - Orchestrator - INFO - MEDALLION DATA PIPELINE
2025-12-21 20:51:53 - Orchestrator - INFO - Started at: 2025-12-21 20:51:53
2025-12-21 20:51:56 - pipeline_steps.silver - INFO - Found 0 new records
2025-12-21 20:52:01 - pipeline_steps.gold - INFO - Updated 0 records
2025-12-21 20:52:09 - Orchestrator - INFO - TRANSFORMATIONS COMPLETED
2025-12-21 20:52:09 - Orchestrator - INFO - Duration: 0:00:15
```

### Check Snowflake Tables

After running the pipeline, query Snowflake:

```sql
-- Bronze layer records
SELECT COUNT(*) FROM RAW_DB.LANDING.BRONZE_REAL_ESTATE;

-- Silver layer with quality scores
SELECT * FROM STAGING_DB.CLEANED.SILVER_REAL_ESTATE
WHERE data_quality_score >= 0.6;

-- Gold ML features
SELECT price_category, COUNT(*)
FROM ANALYTICS_DB.DWH.GOLD_ML_FEATURES
GROUP BY price_category;

-- Anomaly alerts
SELECT * FROM ANALYTICS_DB.ML_RESULTS.ANOMALY_ALERTS
WHERE is_resolved = FALSE
ORDER BY severity DESC;
```

## Troubleshooting

### Kafka Connection Failed

**Error:** `NoBrokersAvailable`

**Causes:**
- Kafka service is down or expired
- Wrong bootstrap server URL
- Network/firewall issues

**Solutions:**
1. Verify Kafka service is running
2. Check `.env` KAFKA_BOOTSTRAP_SERVERS
3. Test DNS: `nslookup <kafka-host>`
4. Use `--skip-ingestion` to bypass

### MongoDB Connection Failed

**Error:** `ServerSelectionTimeoutError`

**Causes:**
- Wrong connection string
- IP not whitelisted
- Invalid credentials

**Solutions:**
1. Verify MONGO_URI in `.env`
2. Add your IP to MongoDB Atlas whitelist
3. Check username/password

### Snowflake Connection Failed

**Error:** `ProgrammingError`

**Causes:**
- Invalid account identifier
- Wrong credentials
- Warehouse not running

**Solutions:**
1. Verify SNOWFLAKE_ACCOUNT format: `<account>.<region>`
2. Check username/password
3. Ensure warehouse is running

### SSL Certificate Error

**Error:** `CERTIFICATE_VERIFY_FAILED`

**Solutions:**
1. Ensure `ca.pem` exists in `src/kafka_client/`
2. Download fresh certificate from Aiven
3. Check certificate validity

## Data Quality

### Quality Score Criteria

| Field    | Score  | Condition                    |
|----------|--------|------------------------------|
| price    | +0.2   | Not null and > 0             |
| area     | +0.2   | Not null and > 0             |
| location | +0.2   | Not null and not empty       |
| bedrooms | +0.2   | Not null                     |
| city     | +0.2   | Not null and not empty       |

**Minimum threshold for Gold layer:** 0.6 (3 out of 5 criteria)

### Anomaly Detection Thresholds

| Severity | Z-Score Threshold |
|----------|-------------------|
| LOW      | > 3.0             |
| MEDIUM   | > 3.0             |
| HIGH     | > 4.0             |

## Development

### Adding New SQL Queries

1. Create `.sql` file in appropriate folder:
   ```
   src/sql/<layer>/<query_name>.sql
   ```

2. Use in Python:
   ```python
   from sql import load_query

   query = load_query("layer", "query_name")
   cursor.execute(query)
   ```

### Adding New Pipeline Steps

1. Create new file in `src/pipeline_steps/`
2. Implement `run()` function
3. Add to `__init__.py`
4. Update `pipeline_orchestrator.py`

## Support

For issues or questions:
- Check the [ARCHITECTURE.md](ARCHITECTURE.md) for system design
- Review logs for detailed error messages
- Verify all service connections independently
