# Tourist Attractions ETL (PySpark + GCP)

ETL pipeline that ingests TripAdvisor attractions reviews, cleans and models data into Bronze/Silver/Gold layers, and writes results locally or to GCP (GCS) depending on environment.

## Project Goals
- Ingest raw CSV reviews for tourist attractions.
- Normalize and clean fields (dates, ratings, null handling).
- Build dimensional model:
  - `dim_attractions` (attraction attributes)
  - `fact_reviews` (user reviews)
- Apply data quality rules and separate invalid records.
- Write outputs to local filesystem or GCS buckets.

## Repository Structure
- `main.py`: ETL pipeline (Spark).
- `project_config.py`: environment detection and storage paths.
- `data/raw/`: raw CSV input.
- `data/bronze/`, `data/silver/`, `data/gold/`, `data/invalid/`: local outputs (when running locally).
- `data/processed/`: example processed CSVs (legacy/manual exports).

## Environment Detection
The pipeline auto-detects the runtime:
- Local by default.
- GCP if `APP_ENV=gcp` is set or common GCP env vars are present.

You can override explicitly:
```
set APP_ENV=gcp
```

On GCP, outputs go to the GCS buckets defined in `project_config.py`.  
Locally, outputs go to `data/bronze`, `data/silver`, `data/gold`, `data/invalid`.

## Requirements
- Python 3.9+
- Java 8+ (required for Spark)
- PySpark

If you use a virtual environment:
```
python -m venv venv
venv\Scripts\activate
pip install pyspark
```

## Running Locally
1. Place your raw file at:
   - `data/raw/Attraction_Belem.csv`
2. Run:
```
python main.py
```

If Spark complains about master/local mode, uncomment in `main.py`:
```
.master("local[*]")
```

## Running on GCP
1. Ensure buckets exist:
   - `gs://gcp-datalakehouse-raw-3`
   - `gs://gcp-datalakehouse-bronze-3`
   - `gs://gcp-datalakehouse-silver-3`
   - `gs://gcp-datalakehouse-gold-3`
   - `gs://gcp-datalakehouse-invalid-3`
2. Set:
```
set APP_ENV=gcp
```
3. Upload raw data to the raw bucket (path configured in `project_config.py`).
4. Run the job where Spark + GCP connectors are available.

## Data Model
The dimensional modeling for `fact_reviews` (user reviews) and `dim_attractions` is implemented directly in `main.py` and is a many-to-one relationship.
### Bronze
Cleaned raw data with basic validation and normalized types.

### Silver
- `dim_attractions`:
  - `Attraction_id`, `Name`, `Rating_Attraction`
- `fact_reviews`:
  - Review data joined to `dim_attractions`

### Gold
Validated records only (rules described below).

### Invalid
Records that failed quality checks.

## Data Quality Rules
### `dim_attractions`
- `Attraction_id` required and unique
- `Rating_Attraction` must be positive and <= 50
- `Name` required

### `fact_reviews`
- `Username` required
- `Rating_Review` in (0, 50]
- `Review` required
- `Date_Travel` required
- `Type_traveler` in {`couples`, `families`, `alone`, `business`, `friends`}

## Notes
- Month parsing is in Portuguese (e.g., `janeiro`, `fevereiro`, `marco`).
- Output format is Parquet.
- If you want CSV exports locally, you can add an explicit `toPandas().to_csv(...)` step.

## Troubleshooting
- If `JAVA_HOME` is missing, Spark will fail to start.
- If paths are wrong, verify `project_config.py` and the presence of `data/raw/Attraction_Belem.csv`.
- If you see encoding issues in month names, ensure the input CSV is UTF-8.
