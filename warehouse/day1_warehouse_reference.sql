-- ============================================================
-- GLOBAL RISK INTELLIGENCE PLATFORM
-- Day 1 Code Reference — PostgreSQL Star Schema Warehouse
-- Pranoydas Eranadath
-- ============================================================


-- ============================================================
-- STEP 1: CREATE THE DATABASE
-- Run this in terminal: psql -U postgres
-- Then run the line below:
-- ============================================================



-- Connect to it:
--\c risk_intelligence_db


-- ============================================================
-- STEP 2: CREATE DIMENSION TABLES
-- Dimensions = the "who, what, where, when" of your data
-- ============================================================


-- Dimension: Date
-- Every event in your fact table links to a date here
CREATE TABLE dim_date (
    date_id      SERIAL PRIMARY KEY,
    full_date    DATE NOT NULL UNIQUE,
    year         INT,
    quarter      INT,
    month        INT,
    month_name   VARCHAR(20),
    week         INT,
    day_of_week  VARCHAR(20)
);


-- Dimension: Location
-- Every country/region you track
CREATE TABLE dim_location (
    location_id  SERIAL PRIMARY KEY,
    country      VARCHAR(100) NOT NULL,
    region       VARCHAR(100),
    continent    VARCHAR(50),
    iso_code     CHAR(3) UNIQUE   -- e.g. 'IND', 'USA', 'DEU'
);


-- Dimension: Disease / Risk Type
-- The type of health event or risk
CREATE TABLE dim_disease (
    disease_id   SERIAL PRIMARY KEY,
    disease_name VARCHAR(200) NOT NULL,
    category     VARCHAR(100),    -- e.g. 'Infectious', 'Chronic'
    icd_code     VARCHAR(20)      -- International Classification code
);


-- Dimension: Data Source
-- Where the data came from (WHO, World Bank, NASA etc.)
CREATE TABLE dim_source (
    source_id    SERIAL PRIMARY KEY,
    source_name  VARCHAR(100) NOT NULL,  -- e.g. 'WHO', 'World Bank'
    source_url   VARCHAR(300),
    update_freq  VARCHAR(50)             -- e.g. 'Daily', 'Monthly'
);


-- ============================================================
-- STEP 3: CREATE FACT TABLE
-- Facts = the actual measurements/events
-- This table connects all your dimensions together
-- ============================================================

CREATE TABLE fact_health_events (
    event_id     SERIAL PRIMARY KEY,
    date_id      INT REFERENCES dim_date(date_id),
    location_id  INT REFERENCES dim_location(location_id),
    disease_id   INT REFERENCES dim_disease(disease_id),
    source_id    INT REFERENCES dim_source(source_id),
    cases        INT DEFAULT 0,
    deaths       INT DEFAULT 0,
    recovered    INT DEFAULT 0,
    loaded_at    TIMESTAMP DEFAULT NOW()   -- when was this row inserted
);


-- ============================================================
-- STEP 4: ADD INDEXES FOR QUERY PERFORMANCE
-- Without indexes, queries on large tables are very slow
-- With indexes, the same queries run 100x faster
-- ============================================================

CREATE INDEX idx_fact_date      ON fact_health_events(date_id);
CREATE INDEX idx_fact_location  ON fact_health_events(location_id);
CREATE INDEX idx_fact_disease   ON fact_health_events(disease_id);
CREATE INDEX idx_fact_loaded_at ON fact_health_events(loaded_at);


-- ============================================================
-- STEP 5: INSERT TEST DATA INTO DIMENSIONS
-- ============================================================

-- Insert locations
INSERT INTO dim_location (country, region, continent, iso_code)
VALUES
    ('India',          'South Asia',     'Asia',     'IND'),
    ('United States',  'North America',  'Americas', 'USA'),
    ('Germany',        'Western Europe', 'Europe',   'DEU'),
    ('UAE',            'Middle East',    'Asia',     'ARE'),
    ('Brazil',         'South America',  'Americas', 'BRA'),
    ('China',          'East Asia',      'Asia',     'CHN');


-- Insert dates
INSERT INTO dim_date (full_date, year, quarter, month, month_name, week, day_of_week)
VALUES
    ('2024-01-15', 2024, 1, 1, 'January',  3,  'Monday'),
    ('2024-03-20', 2024, 1, 3, 'March',    12, 'Wednesday'),
    ('2024-06-10', 2024, 2, 6, 'June',     24, 'Monday'),
    ('2024-09-05', 2024, 3, 9, 'September',36, 'Thursday'),
    ('2024-12-01', 2024, 4, 12,'December', 49, 'Sunday');


-- Insert diseases
INSERT INTO dim_disease (disease_name, category, icd_code)
VALUES
    ('COVID-19',          'Infectious', 'U07.1'),
    ('Dengue Fever',      'Infectious', 'A90'),
    ('Tuberculosis',      'Infectious', 'A15'),
    ('Diabetes Type 2',   'Chronic',    'E11'),
    ('Malaria',           'Infectious', 'B50');


-- Insert sources
INSERT INTO dim_source (source_name, source_url, update_freq)
VALUES
    ('WHO',        'https://www.who.int/data',        'Daily'),
    ('World Bank', 'https://data.worldbank.org',      'Monthly'),
    ('NASA',       'https://data.nasa.gov',           'Daily'),
    ('Our World in Data', 'https://ourworldindata.org', 'Weekly');


-- ============================================================
-- STEP 6: INSERT TEST DATA INTO FACT TABLE
-- ============================================================

INSERT INTO fact_health_events (date_id, location_id, disease_id, source_id, cases, deaths, recovered)
VALUES
    (1, 1, 1, 1, 15000, 120, 14200),   -- India, COVID, Jan 2024
    (1, 2, 1, 1, 45000, 380, 43000),   -- USA, COVID, Jan 2024
    (2, 1, 2, 1, 8500,  45,  8100),    -- India, Dengue, Mar 2024
    (3, 4, 1, 1, 2300,  18,  2200),    -- UAE, COVID, Jun 2024
    (4, 3, 3, 1, 4200,  62,  3900),    -- Germany, TB, Sep 2024
    (5, 5, 5, 1, 12000, 95,  11500);   -- Brazil, Malaria, Dec 2024


-- ============================================================
-- STEP 7: VERIFY EVERYTHING WORKS — RUN THESE QUERIES
-- ============================================================

-- Check all dimension tables have data
SELECT * FROM dim_location;
SELECT * FROM dim_date;
SELECT * FROM dim_disease;
SELECT * FROM dim_source;

-- Check fact table
SELECT * FROM fact_health_events;

-- The real test: JOIN all tables together (this is what makes a star schema)
SELECT
    dd.full_date,
    dl.country,
    dl.continent,
    dds.disease_name,
    dds.category,
    fhe.cases,
    fhe.deaths,
    fhe.recovered,
    ROUND(fhe.deaths::NUMERIC / NULLIF(fhe.cases, 0) * 100, 2) AS death_rate_pct
FROM fact_health_events fhe
JOIN dim_date     dd  ON fhe.date_id     = dd.date_id
JOIN dim_location dl  ON fhe.location_id = dl.location_id
JOIN dim_disease  dds ON fhe.disease_id  = dds.disease_id
ORDER BY fhe.cases DESC;


-- ============================================================
-- STEP 8: PRACTICE SQL PATTERNS (MAANG INTERVIEW PREP)
-- Run these after your data is loaded
-- ============================================================


-- Pattern 1: Window Function — Rank countries by cases
SELECT
    dl.country,
    dds.disease_name,
    fhe.cases,
    RANK() OVER (
        PARTITION BY fhe.disease_id
        ORDER BY fhe.cases DESC
    ) AS rank_by_cases
FROM fact_health_events fhe
JOIN dim_location dl  ON fhe.location_id = dl.location_id
JOIN dim_disease  dds ON fhe.disease_id  = dds.disease_id;


-- Pattern 2: CTE — Find the top disease per continent
WITH disease_totals AS (
    SELECT
        dl.continent,
        dds.disease_name,
        SUM(fhe.cases) AS total_cases
    FROM fact_health_events fhe
    JOIN dim_location dl  ON fhe.location_id = dl.location_id
    JOIN dim_disease  dds ON fhe.disease_id  = dds.disease_id
    GROUP BY dl.continent, dds.disease_name
),
ranked AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY continent ORDER BY total_cases DESC) AS rnk
    FROM disease_totals
)
SELECT continent, disease_name, total_cases
FROM ranked
WHERE rnk = 1;


-- Pattern 3: Upsert — Safe insert that won't break on duplicates
-- Use this in your ETL pipeline when reloading data
INSERT INTO dim_location (country, region, continent, iso_code)
VALUES ('Japan', 'East Asia', 'Asia', 'JPN')
ON CONFLICT (iso_code)
DO UPDATE SET
    region    = EXCLUDED.region,
    continent = EXCLUDED.continent;


-- Pattern 4: EXPLAIN ANALYZE — Check query performance
EXPLAIN ANALYZE
SELECT *
FROM fact_health_events fhe
JOIN dim_location dl ON fhe.location_id = dl.location_id
WHERE dl.continent = 'Asia';
-- Look for: "Seq Scan" on large tables = slow, "Index Scan" = fast


-- Pattern 5: Aggregate with HAVING — Countries with death rate > 1%
SELECT
    dl.country,
    SUM(fhe.cases)    AS total_cases,
    SUM(fhe.deaths)   AS total_deaths,
    ROUND(SUM(fhe.deaths)::NUMERIC / NULLIF(SUM(fhe.cases), 0) * 100, 2) AS death_rate_pct
FROM fact_health_events fhe
JOIN dim_location dl ON fhe.location_id = dl.location_id
GROUP BY dl.country
HAVING ROUND(SUM(fhe.deaths)::NUMERIC / NULLIF(SUM(fhe.cases), 0) * 100, 2) > 1.0
ORDER BY death_rate_pct DESC;


-- ============================================================
-- STEP 9: WHAT TO PUSH TO GITHUB TODAY
-- Save this file as: warehouse/schema.sql
-- Then in terminal:
--   git add warehouse/schema.sql
--   git commit -m "Day 1: PostgreSQL star schema warehouse"
--   git push origin main
-- ============================================================
