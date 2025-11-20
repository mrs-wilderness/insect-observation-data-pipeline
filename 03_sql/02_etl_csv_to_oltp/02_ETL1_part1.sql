-- NOTE: ETL1 uses staging tables to clean and transform raw DOPI CSV data
-- before inserting into normalized OLTP tables.
-- Update file paths before running.

-- create institutions staging
-- NOTE: we assume we now have only the UK records.
CREATE TABLE IF NOT EXISTS stg_institutions (
	institution TEXT,
	city TEXT,
	county TEXT,
	country TEXT default 'United Kingdom'
);

-- create a table for invalid rows
CREATE TABLE IF NOT EXISTS invalid_stg_institutions (
	institution TEXT,
	city TEXT,
	county TEXT,
	country TEXT,
	problem TEXT
);

-- load data from institutions csv into staging
COPY stg_institutions (institution, city, county)
FROM 'absolute\path\to\data\institutions_mock.csv'
DELIMITER ','
CSV HEADER;

-- get invalid rows (null non-nullables)
INSERT INTO invalid_stg_institutions (institution, city, county, country, problem)
SELECT institution, city, county, country, 'Missing institution name.'
FROM stg_institutions
WHERE institution IS NULL;
DELETE FROM stg_institutions
WHERE institution IS NULL;

-- insert into countries
INSERT INTO countries (country_name)
SELECT DISTINCT country
FROM stg_institutions
WHERE country IS NOT NULL
	AND country NOT IN (
		SELECT country_name FROM countries
	);

-- insert into counties
INSERT INTO counties (county_name, country_id)
SELECT DISTINCT stg.county, c.country_id
FROM stg_institutions AS stg
JOIN countries AS c
ON stg.country = c.country_name
WHERE stg.county IS NOT NULL
	AND stg.county NOT IN (
		SELECT county_name FROM counties
	);

-- insert into cities
INSERT INTO cities (city_name, county_id)
SELECT DISTINCT stg.city, co.county_id
FROM stg_institutions AS stg
JOIN counties AS co
ON stg.county = co.county_name
WHERE stg.city IS NOT NULL
	AND stg.city NOT IN (
		SELECT city_name FROM cities
	);

-- insert into institutions
-- NOTE: city_id is nullable by design
INSERT INTO institutions (institution_name, city_id)
SELECT DISTINCT stg.institution, cit.city_id
FROM stg_institutions AS stg
LEFT JOIN cities AS cit
ON stg.city = cit.city_name
WHERE stg.institution IS NOT NULL
	AND stg.institution NOT IN (
		SELECT institution_name FROM institutions
	);

-- clean up staging
DELETE FROM stg_institutions;