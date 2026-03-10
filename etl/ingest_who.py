# ============================================================
# FILE: etl/ingest_who.py
# PROJECT: Global Risk Intelligence Platform
# AUTHOR: Pranoydas Eranadath
# DAY: 2-3 of Week 1
# PURPOSE: Extract WHO/OWID data, transform it, load into PostgreSQL
# ============================================================

# ============================================================
# WHAT IS AN ETL PIPELINE?
# ETL = Extract, Transform, Load
#
# EXTRACT  = Pull data from an outside source (API, CSV, website)
# TRANSFORM = Clean it, reshape it, validate it
# LOAD     = Put it into your data warehouse (PostgreSQL)
#
# Think of it like cooking:
#   Extract  = get raw vegetables from market
#   Transform = wash, cut, season
#   Load     = put in the pot to serve
#
# This is the MOST important skill for a Data Engineer.
# Every company — Amazon, Swiggy, Razorpay — runs ETL pipelines.
# ============================================================


# ── IMPORTS ──
# pandas: like Excel in Python. Works with tables of data (DataFrames)
import pandas as pd

# requests: for downloading data from the internet
import requests

# sqlalchemy: connects Python to PostgreSQL
# create_engine = creates the database connection
from sqlalchemy import create_engine, text

# For tracking when things ran and how long they took
from datetime import datetime

# logging: professional way to print messages
# Better than print() because you can control log levels and save to files
import logging

# sys: for exiting the script if something goes wrong
import sys

# os: for reading environment variables (passwords stored safely)
import os


# ── SETUP LOGGING ──
# basicConfig sets the format for all log messages
# %(asctime)s = timestamp, %(levelname)s = INFO/ERROR, %(message)s = your message
# Output will look like: 2024-01-15 06:00:01 - INFO - Starting pipeline...
logging.basicConfig(
    level=logging.INFO,  # Show INFO and above (INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Create a logger for this specific file
# Good practice: each file has its own logger
logger = logging.getLogger(__name__)


# ── DATABASE CONNECTION ──
# NEVER hardcode passwords in real code!
# Use environment variables: os.environ.get('DB_PASSWORD')
# For now we use a default for local development
from urllib.parse import quote_plus
password = quote_plus("Pranoy@1999")  # becomes Pranoy%401999
engine = create_engine(f"postgresql://postgres:{password}@localhost:5432/risk_intelligence_db")
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'risk_intelligence_db',
    'user': 'postgres',
    'password': os.environ.get('DB_PASSWORD', password)
}

# Build the connection string PostgreSQL understands
# Format: postgresql://user:password@host:port/database
DB_URL = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# create_engine creates a connection pool — not one connection
# but a pool of connections that can be reused efficiently
engine = create_engine(DB_URL)


# ============================================================
# STEP 1: EXTRACT
# Download data from Our World in Data (free, no API key needed)
# This is real COVID data for every country in the world
# ============================================================

def extract_data():
    """
    Downloads WHO/OWID health data from the internet.

    Returns:
        pd.DataFrame: raw data with columns iso_code, location, date,
                      new_cases, new_deaths, new_recovered

    Why OWID (Our World in Data)?
    - Free, no API key required
    - Updated daily
    - Data from WHO, governments worldwide
    - 200+ countries
    - Perfect for learning
    """

    logger.info("EXTRACT: Starting data download...")

    # The URL for the OWID COVID dataset
    # This is a real, live CSV file updated daily
    URL = "D:\Downloads\Pranoy_Week1_Code\week1\etl\owid-covid-data.csv"

    # We only need these columns — downloading all 67 columns would be slow
    # usecols tells pandas to only load these specific columns
    COLUMNS_NEEDED = [
        'iso_code',     # 3-letter country code (IND, USA, DEU)
        'location',     # country name ('India', 'United States')
        'date',         # date string ('2024-01-15')
        'new_cases',    # new cases reported that day
        'new_deaths',   # new deaths reported that day
    ]

    try:
        # pd.read_csv downloads and parses the CSV in one step
        # This might take 10-30 seconds — the file is ~50MB
        logger.info(f"Downloading from: {URL}")
        df = pd.read_csv(URL, usecols=COLUMNS_NEEDED)

        # Log how many rows we got
        logger.info(f"EXTRACT: Downloaded {len(df):,} rows, {len(df.columns)} columns")

        return df

    except Exception as e:
        # If download fails, log the error and stop the pipeline
        logger.error(f"EXTRACT FAILED: {e}")
        raise  # re-raise the exception to stop execution


# ============================================================
# STEP 2: TRANSFORM
# Clean and reshape the raw data so it's ready for the warehouse
#
# Raw data is always messy:
# - Missing values (NaN/NULL)
# - Wrong data types (dates as strings)
# - Aggregate rows we don't want (like 'World', 'Asia')
# - Negative numbers (reporting corrections)
# ============================================================

def transform_data(df):
    """
    Cleans and reshapes raw data for loading into the warehouse.

    Args:
        df: raw DataFrame from extract_data()

    Returns:
        pd.DataFrame: cleaned, validated data ready to load

    What we do:
    1. Remove rows without a valid country code
    2. Remove continent/world aggregate rows
    3. Fill missing numbers with 0
    4. Convert date strings to proper date type
    5. Remove rows with negative values (reporting corrections)
    6. Keep only recent data (last 90 days) for this run
    """

    logger.info(f"TRANSFORM: Starting with {len(df):,} rows")

    # ── 2a. Remove rows without iso_code ──
    # Some rows in OWID are aggregates like 'World', 'Asia', 'High income'
    # These don't have a 3-letter iso_code — they have codes like 'OWID_WRL'
    # We only want real countries

    # dropna removes rows where iso_code is missing (NaN)
    df = df.dropna(subset=['iso_code', 'location', 'date'])
    logger.info(f"After dropping NaN iso_code: {len(df):,} rows")


    # ── 2b. Remove continent/world aggregate rows ──
    # Real country iso_codes are exactly 3 letters (IND, USA, DEU)
    # OWID aggregate codes start with 'OWID_' (longer than 3)
    # str.len() gives the length of each string
    df = df[df['iso_code'].str.len() == 3]
    logger.info(f"After removing aggregates: {len(df):,} rows")


    # ── 2c. Fill missing numbers with 0 ──
    # NaN (Not a Number) means the value is missing
    # For case counts, missing = 0 cases reported that day
    # fillna(0) replaces all NaN with 0
    # astype(int) converts from float (NaN makes pandas use float) to integer
    df['new_cases']  = df['new_cases'].fillna(0).astype(int)
    df['new_deaths'] = df['new_deaths'].fillna(0).astype(int)


    # ── 2d. Convert date strings to proper dates ──
    # The CSV has dates as strings: '2024-01-15'
    # pd.to_datetime converts them to proper date objects
    # This lets us do date math: date + 30 days, extract year, etc.
    df['date'] = pd.to_datetime(df['date'])


    # ── 2e. Remove negative values ──
    # Sometimes countries correct past reports, causing negative numbers
    # e.g. "we over-reported 500 cases last week, so today = -500"
    # We remove these rows as they're data quality issues
    df = df[(df['new_cases'] >= 0) & (df['new_deaths'] >= 0)]
    logger.info(f"After removing negatives: {len(df):,} rows")


    # ── 2f. Keep only last 90 days for this run ──
    # The full dataset has data from 2020 onwards — millions of rows
    # For development, we only load recent data to keep things fast
    # In production (Month 3), we'd load everything to S3 first
   # cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=90)
    #df = df[df['date'] >= cutoff_date]
    #logger.info(f"After keeping last 90 days: {len(df):,} rows")


    # ── 2g. Rename columns to match our warehouse naming ──
    df = df.rename(columns={
        'iso_code':   'iso_code',
        'location':   'country',
        'date':       'event_date',
        'new_cases':  'cases',
        'new_deaths': 'deaths',
    })

    # Add a column for where this data came from
    df['source_name'] = 'Our World in Data'

    # Add a column for when WE processed this data
    df['loaded_at'] = datetime.now()

    logger.info(f"TRANSFORM: Complete. Final shape: {df.shape}")
    logger.info(f"Date range: {df['event_date'].min()} to {df['event_date'].max()}")
    logger.info(f"Countries: {df['iso_code'].nunique()}")

    return df


# ============================================================
# STEP 3: LOAD
# Put the clean data into PostgreSQL
#
# We use a "staging table" approach:
# 1. Load into a temporary staging table first
# 2. Then merge from staging into the real warehouse tables
#
# WHY staging?
# - If something fails midway, the real tables aren't half-updated
# - We can validate staging data before touching production tables
# - This is how real enterprise ETL works
# ============================================================

def load_to_staging(df, engine):
    """
    Loads transformed data into a staging table in PostgreSQL.

    A staging table is a temporary holding area.
    We load messy data here first, then clean it into the real tables.

    Args:
        df: cleaned DataFrame from transform_data()
        engine: SQLAlchemy database engine
    """

    logger.info("LOAD: Writing to staging table...")

    # to_sql writes a DataFrame directly to a PostgreSQL table
    # if_exists='replace' = drop the table and recreate it each time
    # This is fine for staging — we always want fresh data
    # For production tables we'd use 'append' with upsert logic
    # index=False = don't write the pandas row numbers as a column
    df.to_sql(
        name='staging_health_events',  # table name to create/replace
        con=engine,
        if_exists='replace',           # replace table on each run
        index=False,
        chunksize=1000,                # write 1000 rows at a time (memory efficient)
        method='multi'                 # faster bulk insert
    )

    logger.info(f"LOAD: Wrote {len(df):,} rows to staging_health_events")


def load_dimensions(engine):
    """
    Populates dim_location and dim_date from the staging table.
    Uses UPSERT so it's safe to run multiple times.

    This is the critical step where staging data moves into
    the real star schema warehouse tables.
    """

    logger.info("LOAD: Populating dimension tables...")

    # We use a SQL transaction — either ALL of this succeeds,
    # or NONE of it does. No partial updates.
    with engine.connect() as conn:

        # ── Load dim_location from staging ──
        # INSERT INTO ... SELECT ... pulls unique countries from staging
        # ON CONFLICT (iso_code) DO NOTHING = skip if already exists
        # This is safe to run 100 times — no duplicates created
        conn.execute(text("""
            INSERT INTO dim_location (country, iso_code)
            SELECT DISTINCT
                country,
                iso_code
            FROM staging_health_events
            WHERE iso_code IS NOT NULL
            ON CONFLICT (iso_code) DO NOTHING;
        """))
        logger.info("dim_location updated")


        # ── Load dim_date from staging ──
        # We extract all unique dates and generate the derived fields
        # DATE_PART extracts year, month, quarter from a date
        # TO_CHAR formats the date as a string for month_name
        conn.execute(text("""
            INSERT INTO dim_date (full_date, year, quarter, month, month_name, week, day_of_week)
            SELECT DISTINCT
                event_date::DATE,
                DATE_PART('year',    event_date)::INT,
                DATE_PART('quarter', event_date)::INT,
                DATE_PART('month',   event_date)::INT,
                TO_CHAR(event_date, 'Month'),
                DATE_PART('week',    event_date)::INT,
                TO_CHAR(event_date, 'Day')
            FROM staging_health_events
            WHERE event_date IS NOT NULL
            ON CONFLICT (full_date) DO NOTHING;
        """))
        logger.info("dim_date updated")

        # Commit the transaction — save all changes permanently
        conn.commit()

    logger.info("LOAD: Dimension tables complete")


def load_facts(engine):
    """
    Loads fact_health_events by joining staging with dimension tables.

    This is the final step — joining staging data with dimension IDs
    to create the fact rows in our star schema.
    """

    logger.info("LOAD: Loading fact table...")

    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO fact_health_events
                (date_id, location_id, disease_id, source_id, cases, deaths, loaded_at)
            SELECT
                dd.date_id,
                dl.location_id,
                1 AS disease_id,   -- Default to COVID-19 (disease_id=1) for now
                1 AS source_id,    -- Default to WHO (source_id=1) for now
                s.cases,
                s.deaths,
                s.loaded_at
            FROM staging_health_events s
            -- Join to get the date_id from dim_date
            JOIN dim_date     dd ON dd.full_date = s.event_date::DATE
            -- Join to get the location_id from dim_location
            JOIN dim_location dl ON dl.iso_code  = s.iso_code
            -- Only load rows where we have actual cases or deaths
            WHERE s.cases > 0 OR s.deaths > 0
            -- Skip rows where we couldn't match location or date
            -- (this handles data quality issues gracefully)
            ON CONFLICT DO NOTHING;
        """))
        conn.commit()

    # Count how many rows are now in the fact table
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM fact_health_events"))
        count = result.scalar()
        logger.info(f"LOAD: fact_health_events now has {count:,} rows")


# ============================================================
# DATA QUALITY CHECKS
# Before we declare the pipeline successful, we validate the data
# This catches problems BEFORE they reach analysts/dashboards
#
# In Month 2 we replace this with Great Expectations (professional tool)
# For now we do manual checks
# ============================================================

def run_quality_checks(df):
    """
    Runs basic data quality checks on the transformed DataFrame.
    Raises an error if critical checks fail — stopping the pipeline.

    Returns:
        bool: True if all checks pass, False otherwise
    """

    logger.info("QUALITY: Running data quality checks...")
    errors = []

    # Check 1: DataFrame should not be empty
    if len(df) == 0:
        errors.append("FAIL: DataFrame is empty — no data to load")

    # Check 2: Required columns must exist
    required_columns = ['iso_code', 'country', 'event_date', 'cases', 'deaths']
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        errors.append(f"FAIL: Missing columns: {missing_cols}")

    # Check 3: No negative cases or deaths
    if (df['cases'] < 0).any():
        neg_count = (df['cases'] < 0).sum()
        errors.append(f"FAIL: {neg_count} rows have negative cases")

    # Check 4: iso_code should be exactly 3 characters
    invalid_iso = df[df['iso_code'].str.len() != 3]
    if len(invalid_iso) > 0:
        errors.append(f"FAIL: {len(invalid_iso)} rows have invalid iso_code")

    # Check 5: Null rate in critical columns should be < 1%
    null_rate = df['iso_code'].isnull().mean()
    if null_rate > 0.01:
        errors.append(f"FAIL: iso_code null rate is {null_rate:.1%} (max 1%)")

    # Check 6: Date range should be reasonable
    if df['event_date'].max() > pd.Timestamp.now() + pd.Timedelta(days=1):
        errors.append("FAIL: Future dates found in data")

    # Report results
    if errors:
        for error in errors:
            logger.error(f"QUALITY CHECK: {error}")
        logger.error(f"QUALITY: {len(errors)} checks FAILED — pipeline stopped")
        return False
    else:
        logger.info(f"QUALITY: All checks PASSED ✓")
        logger.info(f"  Rows: {len(df):,}")
        logger.info(f"  Countries: {df['iso_code'].nunique()}")
        logger.info(f"  Date range: {df['event_date'].min().date()} to {df['event_date'].max().date()}")
        return True


# ============================================================
# MAIN PIPELINE RUNNER
# This ties everything together: Extract → Quality Check
# → Transform → Load → Report
# ============================================================

def run_pipeline():
    """
    Runs the complete ETL pipeline from start to finish.
    Logs time taken and row counts at each step.
    """

    # Record start time — we'll measure how long the pipeline takes
    pipeline_start = datetime.now()
    logger.info("=" * 60)
    logger.info("PIPELINE STARTED")
    logger.info(f"Start time: {pipeline_start.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    try:
        # ── STEP 1: EXTRACT ──
        step_start = datetime.now()
        raw_df = extract_data()
        logger.info(f"Extract took: {(datetime.now()-step_start).seconds}s")

        # ── STEP 2: TRANSFORM ──
        step_start = datetime.now()
        clean_df = transform_data(raw_df)
        logger.info(f"Transform took: {(datetime.now()-step_start).seconds}s")

        # ── STEP 3: QUALITY CHECKS ──
        step_start = datetime.now()
        checks_passed = run_quality_checks(clean_df)

        if not checks_passed:
            # Stop the pipeline if data quality fails
            # We do NOT load bad data into the warehouse
            logger.error("PIPELINE ABORTED: Data quality checks failed")
            sys.exit(1)

        # ── STEP 4: LOAD ──
        step_start = datetime.now()
        load_to_staging(clean_df, engine)
        load_dimensions(engine)
        load_facts(engine)
        logger.info(f"Load took: {(datetime.now()-step_start).seconds}s")

        # ── PIPELINE SUCCESS ──
        total_seconds = (datetime.now() - pipeline_start).seconds
        logger.info("=" * 60)
        logger.info(f"PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"Total time: {total_seconds}s")
        logger.info(f"Rows processed: {len(clean_df):,}")
        logger.info("=" * 60)

    except Exception as e:
        # Catch any unexpected error and log it properly
        logger.error(f"PIPELINE FAILED with error: {e}")
        logger.error("Check the logs above for details")
        raise  # re-raise so the error shows in Airflow if scheduled


# ============================================================
# HOW TO RUN THIS FILE
#
# 1. First install the required libraries:
#    pip install pandas sqlalchemy psycopg2-binary requests
#
# 2. Make sure PostgreSQL is running and schema.sql has been run
#
# 3. Set your password (optional, or edit DB_CONFIG above):
#    set DB_PASSWORD=yourpassword   (Windows CMD)
#
# 4. Run the pipeline:
#    python etl/ingest_who.py
#
# 5. Check pgAdmin — you should see data in:
#    - staging_health_events (raw staging)
#    - dim_location (new countries added)
#    - dim_date (new dates added)
#    - fact_health_events (new fact rows)
# ============================================================

# This pattern means: only run if THIS file is run directly
# If this file is imported by another file (like Airflow), don't auto-run
if __name__ == '__main__':
    run_pipeline()
