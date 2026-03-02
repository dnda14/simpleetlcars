# ETLCars - Vehicle Data ETL Pipeline

A robust ETL (Extract, Transform, Load) pipeline for processing vehicle/car datasets. This project handles the complete data lifecycle from raw source files to analytics-ready Parquet datasets.
<img width="1024" height="559" alt="image" src="https://github.com/user-attachments/assets/3aae2c26-eb77-4ddb-832a-642ab4d77928" />

## Features

- **Data Ingestion**: Copy raw data with SHA256 checksum verification and metadata tracking
- **Schema Validation**: Validates column presence and data types against defined schema
- **Data Quality**: Separates valid and invalid records based on business rules
- **Data Transformation**: Normalization, type conversion, deduplication, and feature engineering
- **Data Storage**: Converts to partitioned Parquet format for efficient analytics
- **Batch Processing**: Prevents re-processing with batch tracking
- **Querying**: Integrated DuckDB for quick data analysis

## Project Structure

```
ETLCars/
├── main.py              # Pipeline orchestration
├── ingest.py           # Data ingestion with checksums
├── validate.py         # Schema and data validation
├── transform.py        # Data transformation pipeline
├── store.py            # Parquet storage with partitioning
├── query.py            # DuckDB analytics queries
├── control.py          # Batch processing control
├── logger_config.py    # Logging configuration
├── data/               # Data directory
│   ├── source/         # Raw source data
│   ├── raw/            # Ingested raw data
│   ├── cleaned/       # Validated clean data
│   ├── rejected/      # Invalid records
│   ├── transformed/   # Transformed data
│   ├── analytics/     # Parquet output
│   └── metadata/      # Processing metadata
└── log/                # Pipeline logs
```

## Requirements

- Python 3.9+
- pandas
- pyarrow
- duckdb

Install dependencies:

```bash
pip install pandas pyarrow duckdb
```

## Pipeline Stages

### 1. Ingestion
- Copies source CSV to raw directory
- Generates SHA256 checksum for integrity
- Records metadata in CSV

### 2. Validation
Validates data against:
- **Schema**: 16 columns (Make, Model, Year, Engine specs, etc.)
- **Data Types**: Ensures correct numeric/string types
- **Business Rules**:
  - Year: 1950 to current year
  - MSRP: > 0
  - Highway MPG: > 0
  - City MPG: > 0

### 3. Transformation
- Normalizes text to lowercase
- Converts types to appropriate formats
- Adds derived features:
  - `Age`: Vehicle age in years
  - `consume_eff`: Highway/City MPG ratio
- Removes duplicates based on Make, Model, Year, Engine HP
- Applies business rules (max age 40 years)

### 4. Storage
- Converts to Apache Parquet format
- Partitions by ingestion date
- Validates output integrity

## Usage

### Run Full Pipeline

```bash
python main.py
```

### Run Individual Stages

```bash
# Ingestion (run once at start)
python ingest.py

# Validation
python validate.py

# Transformation
python transform.py

# Storage
python store.py

# Query analytics
python query.py
```

### Query Data

```python
import duckdb

con = duckdb.connect()
df = con.execute("""
    SELECT
        ingest_date,
        COUNT(*) AS rows,
        AVG(msrp) AS avg_price
    FROM read_parquet('data/analytics/**/*.parquet')
    GROUP BY ingest_date
    ORDER BY ingest_date DESC
""").df()
```

## Data Schema

| Column | Type | Description |
|--------|------|-------------|
| Make | string | Vehicle manufacturer |
| Model | string | Vehicle model |
| Year | integer | Manufacturing year |
| Engine Fuel Type | string | Fuel type |
| Engine HP | float | Horsepower |
| Engine Cylinders | float | Number of cylinders |
| Transmission Type | string | Transmission type |
| Driven_Wheels | string | Drive configuration |
| Number of Doors | float | Door count |
| Market Category | string | Market segment |
| Vehicle Size | string | Vehicle size |
| Vehicle Style | string | Body style |
| highway MPG | float | Highway fuel efficiency |
| city mpg | float | City fuel efficiency |
| Popularity | integer | Popularity score |
| MSRP | integer | Manufacturer's suggested retail price |

## Derived Fields

| Field | Description |
|-------|-------------|
| Age | Vehicle age (current year - Year) |
| consume_eff | Highway/City MPG ratio |
| ingest_date | Timestamp of data ingestion |

## Logging

Pipeline execution is logged to:
- `log/pipeline.log` (file)
- Console (stdout)

Log format: `%(asctime)s | %(levelname)s | %(message)s`

## Batch Processing

The pipeline tracks processed batches to prevent re-processing:
- Batch IDs stored in `data/metadata/processed_files.csv`
- Each batch includes: file_name, checksum, processed_at, status

## License

MIT License
