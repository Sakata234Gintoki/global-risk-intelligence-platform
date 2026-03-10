-- ============================================================
-- FILE: warehouse/schema.sql
-- PROJECT: Global Risk Intelligence Platform
-- AUTHOR: Pranoydas Eranadath
-- DAY: 1 of Week 1
-- PURPOSE: Create the entire data warehouse structure
-- ============================================================

-- ============================================================
-- WHAT IS A DATA WAREHOUSE?
-- Think of it like a very organised library.
-- A normal database stores current data (like a shop's inventory right now)
-- A warehouse stores historical data for ANALYSIS (like how many items sold
-- every day for the past 5 years)
-- We use a design called "Star Schema" — explained below
-- ============================================================

-- ============================================================
-- WHAT IS A STAR SCHEMA?
-- Imagine a star shape:
--   - The CENTER of the star = FACT TABLE (stores the actual numbers/events)
--   - The POINTS of the star = DIMENSION TABLES (stores the descriptions)
--
-- Example:
--   Fact: "On Jan 15 2024, in India, COVID caused 15000 cases"
--   The numbers (15000 cases) go in the FACT table
--   "Jan 15 2024" goes in dim_date
--   "India" goes in dim_location
--   "COVID" goes in dim_disease
--
-- WHY? Because instead of storing "India" 50,000 times in your fact table,
-- you store it ONCE in dim_location and just reference its ID number.
-- This saves HUGE amounts of storage and makes queries much faster.
-- ============================================================


-- STEP 1: Create the database (run this separately first)
-- CREATE DATABASE risk_intelligence_db;
-- \c risk_intelligence_db


-- ============================================================
-- DIMENSION TABLE 1: dim_date
-- Answers the question: WHEN did this event happen?
--
-- WHY store dates in a separate table?
-- Because from one date "2024-01-15" we can derive many things:
-- year=2024, quarter=1, month=1, week=3, day_of_week=Monday
-- If we stored all this in every row of the fact table, it would be
-- massive. Instead we store it once here and reference by date_id.
-- ============================================================

CREATE TABLE dim_date (
    -- SERIAL means auto-increment: 1, 2, 3, 4... automatically
    -- PRIMARY KEY means this column uniquely identifies each row
    date_id      SERIAL PRIMARY KEY,

    -- The actual date value. UNIQUE means no duplicates allowed.
    -- We don't want January 15 2024 stored twice.
    full_date    DATE NOT NULL UNIQUE,

    -- Breaking the date into parts for easy filtering in queries
    -- e.g. "give me all cases in Q1 2024" → WHERE quarter=1 AND year=2024
    year         INT,
    quarter      INT,   -- 1, 2, 3, or 4
    month        INT,   -- 1 to 12
    month_name   VARCHAR(20),  -- 'January', 'February' etc.
    week         INT,   -- week number of the year (1 to 52)
    day_of_week  VARCHAR(20)   -- 'Monday', 'Tuesday' etc.
);


-- ============================================================
-- DIMENSION TABLE 2: dim_location
-- Answers the question: WHERE did this event happen?
--
-- Every country in the world gets one row here.
-- iso_code is a standard 3-letter country code used globally:
-- India = IND, USA = USA, Germany = DEU
-- This prevents confusion between "India" vs "INDIA" vs "Bharat"
-- ============================================================

CREATE TABLE dim_location (
    location_id  SERIAL PRIMARY KEY,

    -- NOT NULL means this field is required — every location MUST have a country name
    country      VARCHAR(100) NOT NULL,

    region       VARCHAR(100),   -- e.g. 'South Asia', 'Western Europe'
    continent    VARCHAR(50),    -- e.g. 'Asia', 'Europe', 'Americas'

    -- CHAR(3) means exactly 3 characters. UNIQUE means no duplicate codes.
    -- e.g. 'IND', 'USA', 'DEU', 'ARE'
    iso_code     CHAR(3) UNIQUE
);


-- ============================================================
-- DIMENSION TABLE 3: dim_disease
-- Answers the question: WHAT type of health event is this?
--
-- Every disease or health risk gets one row here.
-- ICD code is the international medical classification code —
-- like a universal ID for every disease used by WHO globally.
-- ============================================================

CREATE TABLE dim_disease (
    disease_id   SERIAL PRIMARY KEY,
    disease_name VARCHAR(200) NOT NULL,

    -- Category groups diseases together for analysis
    -- e.g. 'Infectious', 'Chronic', 'Environmental'
    category     VARCHAR(100),

    -- ICD = International Classification of Diseases
    -- COVID-19 = U07.1, Dengue = A90, Tuberculosis = A15
    icd_code     VARCHAR(20)
);


-- ============================================================
-- DIMENSION TABLE 4: dim_source
-- Answers the question: WHERE did we get this data from?
--
-- This is important for DATA GOVERNANCE — knowing which
-- organisation reported each number. WHO data might differ
-- from World Bank data for the same country.
-- ============================================================

CREATE TABLE dim_source (
    source_id    SERIAL PRIMARY KEY,
    source_name  VARCHAR(100) NOT NULL,  -- e.g. 'WHO', 'World Bank', 'NASA'
    source_url   VARCHAR(300),           -- direct URL to the data
    update_freq  VARCHAR(50)             -- how often they update: 'Daily', 'Monthly'
);


-- ============================================================
-- FACT TABLE: fact_health_events
-- This is the CENTER of your star schema.
-- Every health event in the world becomes ONE row here.
--
-- Notice it doesn't store "India" or "COVID" directly.
-- It stores location_id=1 and disease_id=1 instead.
-- To get the full picture, you JOIN with the dimension tables.
--
-- WHY? Imagine 50 million rows. Storing "India" (5 chars) in
-- every row vs storing 1 (integer) saves ~200MB for this
-- column alone. Plus joins are blazing fast with integers.
-- ============================================================

CREATE TABLE fact_health_events (
    event_id     SERIAL PRIMARY KEY,

    -- REFERENCES creates a FOREIGN KEY constraint.
    -- This means: the date_id value MUST exist in dim_date.
    -- You cannot insert a fact row pointing to a date that doesn't exist.
    -- This protects data integrity — no orphaned records.
    date_id      INT REFERENCES dim_date(date_id),
    location_id  INT REFERENCES dim_location(location_id),
    disease_id   INT REFERENCES dim_disease(disease_id),
    source_id    INT REFERENCES dim_source(source_id),

    -- The actual measurements — the numbers we care about
    -- DEFAULT 0 means if not provided, store 0 (not NULL)
    cases        INT DEFAULT 0,
    deaths       INT DEFAULT 0,
    recovered    INT DEFAULT 0,

    -- Audit column: when was this row inserted into our system?
    -- Useful for debugging pipeline issues and tracking data freshness
    -- NOW() is a PostgreSQL function that returns current timestamp
    loaded_at    TIMESTAMP DEFAULT NOW()
);


-- ============================================================
-- INDEXES: Making queries fast
--
-- Without an index: PostgreSQL reads EVERY row to find matches
-- → On 50M rows this takes minutes
--
-- With an index: PostgreSQL jumps directly to matching rows
-- → Same query takes milliseconds
--
-- Think of a book index: instead of reading 500 pages to find
-- "photosynthesis", you check the index and go straight to page 342.
--
-- We create indexes on the columns we JOIN and WHERE on most often.
-- ============================================================

-- Index on date_id — we filter by date constantly
-- e.g. "give me all events from 2024" → uses this index
CREATE INDEX idx_fact_date      ON fact_health_events(date_id);

-- Index on location_id — we filter by country constantly
CREATE INDEX idx_fact_location  ON fact_health_events(location_id);

-- Index on disease_id — we filter by disease type constantly
CREATE INDEX idx_fact_disease   ON fact_health_events(disease_id);

-- Index on loaded_at — useful for finding recently added data
CREATE INDEX idx_fact_loaded_at ON fact_health_events(loaded_at);


-- ============================================================
-- STEP 5: INSERT TEST DATA
-- This is just sample data to verify everything works.
-- In Month 2, real data will come from WHO API automatically.
-- ============================================================

-- Insert test locations
-- Notice we use VALUES with multiple rows in one statement
-- This is faster than separate INSERT statements
INSERT INTO dim_location (country, region, continent, iso_code)
VALUES
    ('India',          'South Asia',      'Asia',     'IND'),
    ('United States',  'North America',   'Americas', 'USA'),
    ('Germany',        'Western Europe',  'Europe',   'DEU'),
    ('UAE',            'Middle East',     'Asia',     'ARE'),
    ('Brazil',         'South America',   'Americas', 'BRA'),
    ('China',          'East Asia',       'Asia',     'CHN');


-- Insert test dates
-- We manually fill in the derived fields (year, quarter, month etc.)
-- In production, a Python script will generate these automatically
INSERT INTO dim_date (full_date, year, quarter, month, month_name, week, day_of_week)
VALUES
    ('2024-01-15', 2024, 1, 1,  'January',  3,  'Monday'),
    ('2024-03-20', 2024, 1, 3,  'March',    12, 'Wednesday'),
    ('2024-06-10', 2024, 2, 6,  'June',     24, 'Monday'),
    ('2024-09-05', 2024, 3, 9,  'September',36, 'Thursday'),
    ('2024-12-01', 2024, 4, 12, 'December', 49, 'Sunday');


-- Insert test diseases
INSERT INTO dim_disease (disease_name, category, icd_code)
VALUES
    ('COVID-19',        'Infectious', 'U07.1'),
    ('Dengue Fever',    'Infectious', 'A90'),
    ('Tuberculosis',    'Infectious', 'A15'),
    ('Diabetes Type 2', 'Chronic',    'E11'),
    ('Malaria',         'Infectious', 'B50');


-- Insert test data sources
INSERT INTO dim_source (source_name, source_url, update_freq)
VALUES
    ('WHO',             'https://www.who.int/data',         'Daily'),
    ('World Bank',      'https://data.worldbank.org',       'Monthly'),
    ('NASA',            'https://data.nasa.gov',            'Daily'),
    ('Our World in Data','https://ourworldindata.org',      'Weekly');


-- Insert test fact rows
-- Read these as: "On date 1 (Jan 15), in location 1 (India),
-- disease 1 (COVID), from source 1 (WHO), 15000 cases, 120 deaths..."
INSERT INTO fact_health_events (date_id, location_id, disease_id, source_id, cases, deaths, recovered)
VALUES
    (1, 1, 1, 1,  15000, 120,  14200),  -- India, COVID, Jan 2024
    (1, 2, 1, 1,  45000, 380,  43000),  -- USA, COVID, Jan 2024
    (2, 1, 2, 1,  8500,  45,   8100),   -- India, Dengue, Mar 2024
    (3, 4, 1, 1,  2300,  18,   2200),   -- UAE, COVID, Jun 2024
    (4, 3, 3, 1,  4200,  62,   3900),   -- Germany, Tuberculosis, Sep 2024
    (5, 5, 5, 1,  12000, 95,   11500);  -- Brazil, Malaria, Dec 2024


-- ============================================================
-- STEP 6: VERIFY EVERYTHING — Run these queries one by one
-- Each one confirms a different part of your warehouse works
-- ============================================================

-- Basic check: see all your test data
SELECT * FROM dim_location;
SELECT * FROM dim_date;
SELECT * FROM dim_disease;
SELECT * FROM dim_source;
SELECT * FROM fact_health_events;

-- THE MOST IMPORTANT QUERY: JOIN all tables together
-- This is the "star schema in action" — connecting all 5 tables
-- to produce a human-readable result from ID numbers
SELECT
    dd.full_date          AS event_date,
    dl.country,
    dl.continent,
    dds.disease_name,
    dds.category          AS disease_type,
    fhe.cases,
    fhe.deaths,
    fhe.recovered,

    -- Calculate death rate as a percentage
    -- ROUND(..., 2) rounds to 2 decimal places: 0.856734 → 0.86
    -- ::NUMERIC casts integer to decimal so division works correctly
    -- NULLIF(cases, 0) prevents "division by zero" error if cases=0
    ROUND(fhe.deaths::NUMERIC / NULLIF(fhe.cases, 0) * 100, 2) AS death_rate_pct

FROM fact_health_events fhe

-- JOIN pulls in data from dimension tables using matching IDs
-- fhe.date_id = dd.date_id means: "find the dim_date row whose
-- date_id matches this fact row's date_id"
JOIN dim_date     dd  ON fhe.date_id     = dd.date_id
JOIN dim_location dl  ON fhe.location_id = dl.location_id
JOIN dim_disease  dds ON fhe.disease_id  = dds.disease_id

-- Show highest case counts first
ORDER BY fhe.cases DESC;


-- ============================================================
-- STEP 7: PRACTICE SQL PATTERNS FOR MAANG INTERVIEWS
-- Learn these patterns — they come up in almost every
-- Data Engineer interview at Amazon, Google, Meta
-- ============================================================


-- ── PATTERN 1: WINDOW FUNCTIONS ──
-- Normal GROUP BY loses individual row details
-- Window functions let you CALCULATE ACROSS ROWS while keeping each row
--
-- RANK() OVER (PARTITION BY disease_id ORDER BY cases DESC)
-- means: "rank each row by cases, but RESET the ranking for each disease"
-- So COVID countries get ranked 1,2,3 separately from Dengue countries
--
-- This is the #1 most asked SQL pattern in MAANG interviews

SELECT
    dl.country,
    dds.disease_name,
    fhe.cases,

    -- RANK within each disease group by number of cases
    -- PARTITION BY = "reset rank for each disease"
    -- ORDER BY cases DESC = "highest cases = rank 1"
    RANK() OVER (
        PARTITION BY fhe.disease_id
        ORDER BY fhe.cases DESC
    ) AS rank_within_disease,

    -- LAG gets the value from the PREVIOUS row
    -- Useful for comparing current vs previous period
    -- Here we compare each country's cases to the previous country's cases
    LAG(fhe.cases) OVER (
        PARTITION BY fhe.disease_id
        ORDER BY fhe.cases DESC
    ) AS prev_country_cases

FROM fact_health_events fhe
JOIN dim_location dl  ON fhe.location_id = dl.location_id
JOIN dim_disease  dds ON fhe.disease_id  = dds.disease_id;


-- ── PATTERN 2: CTEs (Common Table Expressions) ──
-- WITH clause lets you write a query in NAMED STEPS
-- Instead of one massive nested query, you break it into readable pieces
-- Think of it as writing notes before writing an essay
--
-- This is heavily used in production SQL and asked in every MAANG interview

WITH
-- Step 1: Calculate total cases per continent per disease
disease_by_continent AS (
    SELECT
        dl.continent,
        dds.disease_name,
        SUM(fhe.cases)  AS total_cases,
        SUM(fhe.deaths) AS total_deaths
    FROM fact_health_events fhe
    JOIN dim_location dl  ON fhe.location_id = dl.location_id
    JOIN dim_disease  dds ON fhe.disease_id  = dds.disease_id
    GROUP BY dl.continent, dds.disease_name
),

-- Step 2: Rank each disease within its continent
ranked AS (
    SELECT
        *,
        -- ROW_NUMBER is like RANK but never ties — always 1,2,3,4
        ROW_NUMBER() OVER (
            PARTITION BY continent
            ORDER BY total_cases DESC
        ) AS row_num
    FROM disease_by_continent
)

-- Step 3: Only show the #1 disease per continent
SELECT continent, disease_name, total_cases, total_deaths
FROM ranked
WHERE row_num = 1
ORDER BY total_cases DESC;


-- ── PATTERN 3: UPSERT (INSERT or UPDATE) ──
-- In ETL pipelines, you run the same data load multiple times
-- Without upsert: running twice = duplicate rows
-- With upsert: running twice = second run UPDATES existing rows
--
-- ON CONFLICT (iso_code) = "if iso_code already exists..."
-- DO UPDATE SET = "...then UPDATE these fields instead of inserting"
-- EXCLUDED refers to the row that was TRYING to be inserted

INSERT INTO dim_location (country, region, continent, iso_code)
VALUES ('Japan', 'East Asia', 'Asia', 'JPN')
ON CONFLICT (iso_code)                    -- if JPN already exists
DO UPDATE SET                             -- update instead of error
    region    = EXCLUDED.region,          -- EXCLUDED = the new values
    continent = EXCLUDED.continent;

-- Run this twice — first time inserts Japan, second time updates it
-- No error, no duplicate. This is safe to run 1000 times.


-- ── PATTERN 4: EXPLAIN ANALYZE ──
-- Shows you HOW PostgreSQL executes your query
-- Reveals if it's slow and WHY it's slow
-- ALWAYS use this before optimising a slow query
--
-- Look for:
--   "Seq Scan" on large tables = BAD (reads every row)
--   "Index Scan" = GOOD (jumps directly to matching rows)

EXPLAIN ANALYZE
SELECT
    dl.country,
    SUM(fhe.cases) AS total_cases
FROM fact_health_events fhe
JOIN dim_location dl ON fhe.location_id = dl.location_id
WHERE dl.continent = 'Asia'
GROUP BY dl.country;


-- ── PATTERN 5: AGGREGATE + HAVING ──
-- WHERE filters INDIVIDUAL ROWS before grouping
-- HAVING filters GROUPS after aggregation
--
-- Rule: if you need to filter on a SUM(), AVG(), COUNT() → use HAVING
-- If you need to filter on a regular column → use WHERE

SELECT
    dl.country,
    SUM(fhe.cases)   AS total_cases,
    SUM(fhe.deaths)  AS total_deaths,
    ROUND(
        SUM(fhe.deaths)::NUMERIC / NULLIF(SUM(fhe.cases), 0) * 100, 2
    )                AS death_rate_pct
FROM fact_health_events fhe
JOIN dim_location dl ON fhe.location_id = dl.location_id
GROUP BY dl.country
-- HAVING filters after grouping — keep only countries where death rate > 1%
HAVING ROUND(
    SUM(fhe.deaths)::NUMERIC / NULLIF(SUM(fhe.cases), 0) * 100, 2
) > 1.0
ORDER BY death_rate_pct DESC;


-- ── PATTERN 6: SESSIONISATION ──
-- This is Meta's favourite SQL interview question
-- Given events with timestamps, group them into "sessions"
-- A new session starts when there's a gap of more than X minutes
--
-- This uses LAG() to look at the previous event's timestamp
-- and CASE WHEN to flag when a new session starts
-- We then use SUM() OVER to give each session a cumulative number

-- First create a test events table to practice on
CREATE TEMP TABLE user_events (
    user_id    INT,
    event_time TIMESTAMP,
    event_type VARCHAR(50)
);

INSERT INTO user_events VALUES
    (1, '2024-01-15 09:00:00', 'page_view'),
    (1, '2024-01-15 09:05:00', 'click'),
    (1, '2024-01-15 09:08:00', 'purchase'),
    (1, '2024-01-15 10:45:00', 'page_view'),   -- 97 min gap = new session
    (1, '2024-01-15 10:50:00', 'click'),
    (2, '2024-01-15 14:00:00', 'page_view'),
    (2, '2024-01-15 14:03:00', 'click');

-- Now sessionise: any gap > 30 minutes = new session
WITH events_with_gaps AS (
    SELECT
        user_id,
        event_time,
        event_type,
        -- LAG gets the previous row's event_time for same user
        LAG(event_time) OVER (
            PARTITION BY user_id
            ORDER BY event_time
        ) AS prev_event_time,
        -- Calculate gap in minutes from previous event
        EXTRACT(EPOCH FROM (
            event_time - LAG(event_time) OVER (
                PARTITION BY user_id ORDER BY event_time
            )
        )) / 60 AS gap_minutes
    FROM user_events
),
session_flags AS (
    SELECT
        *,
        -- Flag = 1 when gap > 30 min OR it's the first event (NULL gap)
        CASE
            WHEN gap_minutes > 30 OR gap_minutes IS NULL THEN 1
            ELSE 0
        END AS new_session_flag
    FROM events_with_gaps
)
SELECT
    user_id,
    event_time,
    event_type,
    gap_minutes,
    -- Running total of new_session_flags = session number
    -- Each time flag=1, the sum increases → new session ID
    SUM(new_session_flag) OVER (
        PARTITION BY user_id
        ORDER BY event_time
    ) AS session_id
FROM session_flags
ORDER BY user_id, event_time;

