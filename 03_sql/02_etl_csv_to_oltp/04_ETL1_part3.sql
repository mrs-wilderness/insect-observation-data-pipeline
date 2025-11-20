-- NOTE: ETL1 uses staging tables to clean and transform raw DOPI CSV data
-- before inserting into normalized OLTP tables.
-- Update file paths before running.

-- SINCE OBSERVATION RECORDS ARE ALLOWED TO BE DUPLICATED, RERUNNING THIS SCRIPT WILL RESULT HAVING TWICE THE NUMBER OF ROWS
-- THIS IS DUE TO THE NATURE OF THE DATASET COMBINED WITH THE ORIGINAL TASK RESTRICTIONS (SEE README.MD)

-- create table to load the full csv
CREATE TEMP TABLE tmp_insect_observations_full (
	authors TEXT,
	title TEXT,
	journal TEXT,
	pub_year TEXT,
	pub_vol TEXT,
	doi TEXT,
	methodology TEXT,
	pollinator_survey TEXT,
	plant_survey TEXT,
	nbn_pollinator_code TEXT,
	col_pollinator_code TEXT,
	pollinator_species TEXT,
	caste TEXT,
	nbn_plant_code TEXT,
	col_plant_code TEXT,
	plant_species TEXT,
	interactions TEXT,
	date TEXT,
	month TEXT,
	year TEXT,
	grid_letter TEXT,
	grid_code TEXT,
	latitude TEXT,
	longitude TEXT,
	habitat TEXT,
	pollination TEXT,
	pollen TEXT,
	nectar TEXT,
	record TEXT,
	articleurl TEXT
);

-- load the full csv
COPY tmp_insect_observations_full
FROM 'absolute\path\to\data\insect_observations.csv'
DELIMITER ',' CSV HEADER
ENCODING 'LATIN1';

-- create staging for observations
--NOTE: we introduce a PK here for convenience of moving to error table
CREATE TABLE stg_insect_observations (
	raw_data_id SERIAL PRIMARY KEY,
	authors TEXT,
	nbn_pollinator_code TEXT,
	pollinator_species TEXT,
	caste TEXT,
	nbn_plant_code TEXT,
	plant_species TEXT,
	interactions TEXT,
	obs_date TEXT,
	obs_month TEXT,
	obs_year TEXT,
	latitude TEXT,
	longitude TEXT,
	habitat TEXT,
	pollination TEXT,
	pollen TEXT,
	nectar TEXT
);

-- create table for invalid rows
CREATE TABLE invalid_stg_insect_observations (
	raw_data_id INTEGER,
	error_message TEXT,
	row_data JSONB
);


-- insert data into staging
INSERT INTO stg_insect_observations (
	authors,
	nbn_pollinator_code,
	pollinator_species,
	caste,
	nbn_plant_code,
	plant_species,
	interactions,
	obs_date,
	obs_month,
	obs_year,
	latitude,
	longitude,
	habitat,
	pollination,
	pollen,
	nectar
)
SELECT
	authors,
	nbn_pollinator_code,
	pollinator_species,
	caste,
	nbn_plant_code,
	plant_species,
	interactions,
	date,
	month,
	year,
	latitude,
	longitude,
	habitat,
	pollination,
	pollen,
	nectar
FROM tmp_insect_observations_full;

-- cast NA to null
UPDATE stg_insect_observations
SET
	authors = NULLIF(authors, 'NA'),
	nbn_pollinator_code = NULLIF(nbn_pollinator_code, 'NA'),
	pollinator_species = NULLIF(pollinator_species, 'NA'),
	caste = NULLIF(caste, 'NA'),
	nbn_plant_code = NULLIF(nbn_plant_code, 'NA'),
	plant_species = NULLIF(plant_species, 'NA'),
	interactions = NULLIF(interactions, 'NA'),
	obs_date = NULLIF(obs_date, 'NA'),
	obs_month = NULLIF(obs_month, 'NA'),
	obs_year = NULLIF(obs_year, 'NA'),
	latitude = NULLIF(latitude, 'NA'),
	longitude = NULLIF(longitude, 'NA'),
	habitat = NULLIF(habitat, 'NA'),
	pollination = NULLIF(pollination, 'NA'),
	pollen = NULLIF(pollen, 'NA'),
	nectar = NULLIF(nectar, 'NA');

-- cast empty to null
UPDATE stg_insect_observations
SET
	authors = NULLIF(TRIM(authors), ''),
	nbn_pollinator_code = NULLIF(TRIM(nbn_pollinator_code), ''),
	pollinator_species = NULLIF(TRIM(pollinator_species), ''),
	caste = NULLIF(TRIM(caste), ''),
	nbn_plant_code = NULLIF(TRIM(nbn_plant_code), ''),
	plant_species = NULLIF(TRIM(plant_species), ''),
	interactions = NULLIF(TRIM(interactions), ''),
	obs_date = NULLIF(TRIM(obs_date), ''),
	obs_month = NULLIF(TRIM(obs_month), ''),
	obs_year = NULLIF(TRIM(obs_year), ''),
	latitude = NULLIF(TRIM(latitude), ''),
	longitude = NULLIF(TRIM(longitude), ''),
	habitat = NULLIF(TRIM(habitat), ''),
	pollination = NULLIF(TRIM(pollination), ''),
	pollen = NULLIF(TRIM(pollen), ''),
	nectar = NULLIF(TRIM(nectar), '');


-- check for nulls in non-nullable fields and move them to error-table
INSERT INTO invalid_stg_insect_observations (raw_data_id, error_message, row_data)
SELECT
	raw_data_id,
	'Missing required plant species, pollinator species, or author',
	to_jsonb(stg)
FROM stg_insect_observations AS stg
WHERE
	plant_species IS NULL OR plant_species = ''
	OR pollinator_species IS NULL OR pollinator_species = ''
	OR authors IS NULL OR authors = '';
DELETE FROM stg_insect_observations
WHERE raw_data_id IN (
	SELECT raw_data_id
	FROM invalid_stg_insect_observations
);

-- check validity: latitude and longitude
INSERT INTO invalid_stg_insect_observations (raw_data_id, error_message, row_data)
SELECT
	raw_data_id,
	'Invalid latitude or longitude',
	to_jsonb(stg)
FROM stg_insect_observations stg
WHERE
	(latitude IS NOT NULL AND (
		NOT (latitude ~ '^-?\d+(\.\d+)?$') OR
		CAST(latitude AS DECIMAL) < -90 OR CAST(latitude AS DECIMAL) > 90
	))
	OR
	(longitude IS NOT NULL AND (
		NOT (longitude ~ '^-?\d+(\.\d+)?$') OR
		CAST(longitude AS DECIMAL) < -180 OR CAST(longitude AS DECIMAL) > 180
	));

-- check validity: interactions
INSERT INTO invalid_stg_insect_observations (raw_data_id, error_message, row_data)
SELECT raw_data_id, 'Invalid interaction count', to_jsonb(stg)
FROM stg_insect_observations stg
WHERE
	interactions IS NOT NULL AND (
		NOT (interactions ~ '^\d+$')
	);

-- check validity: date fields
INSERT INTO invalid_stg_insect_observations (raw_data_id, error_message, row_data)
SELECT raw_data_id, 'Invalid date component (day, month, or year)', to_jsonb(stg)
FROM stg_insect_observations stg
WHERE
	(obs_date IS NOT NULL AND (
		NOT (obs_date ~ '^\d{1,2}$') OR CAST(obs_date AS INTEGER) NOT BETWEEN 1 AND 31
	))
	OR (obs_month IS NOT NULL AND (
		NOT (obs_month ~ '^\d{1,2}$') OR CAST(obs_month AS INTEGER) NOT BETWEEN 1 AND 12
	))
	OR (obs_year IS NOT NULL AND (
		NOT (obs_year ~ '^\d{4}$') OR CAST(obs_year AS INTEGER) NOT BETWEEN 1800 AND 2100
	));

-- check validity: pollination quality code
INSERT INTO invalid_stg_insect_observations (raw_data_id, error_message, row_data)
SELECT raw_data_id, 'Invalid pollination value', to_jsonb(stg)
FROM stg_insect_observations stg
WHERE
	pollination IS NOT NULL AND NOT (pollination ~ '^[1-4]$');

-- check validity: pollen and nectar
INSERT INTO invalid_stg_insect_observations (raw_data_id, error_message, row_data)
SELECT raw_data_id, 'Invalid pollen or nectar value', to_jsonb(stg)
FROM stg_insect_observations stg
WHERE
	(pollen IS NOT NULL AND UPPER(pollen) NOT IN ('Y', 'N'))
	OR (nectar IS NOT NULL AND UPPER(nectar) NOT IN ('Y', 'N'));

-- remove invalid rows from staging
DELETE FROM stg_insect_observations
WHERE raw_data_id IN (
	SELECT raw_data_id FROM invalid_stg_insect_observations
);

---------------------------------
-- HERE WE HAVE SOME MANUAL FIXES FOR CLEAR AND IDENTIFIABLE ERRORS AND INCONSISTENCIES IN THE DATA
---------------------------------
UPDATE stg_insect_observations
SET nbn_plant_code = 'NBNSYS0000004225'
WHERE UPPER(nbn_plant_code) = 'NHMSYS0000462067'
	AND LOWER(plant_species) = LOWER('Prunella vulgaris');

WITH formatted AS (
	SELECT DISTINCT
	UPPER(nbn_plant_code) AS normalized_nbn_code,
	INITCAP(SPLIT_PART(plant_species, ' ', 1)) ||
		CASE 
			WHEN POSITION(' ' IN plant_species) > 0 
			THEN ' ' || LOWER(SUBSTRING(plant_species FROM POSITION(' ' IN plant_species) + 1))
			ELSE '' 
		END AS normalized_species_name
	FROM stg_insect_observations
	WHERE plant_species IS NOT NULL
)
INSERT INTO plant_species (nbn_code, species_name)
SELECT
	normalized_nbn_code,
	normalized_species_name
FROM formatted AS stg
WHERE NOT EXISTS (
	SELECT 1 FROM plant_species ps
	WHERE LOWER(ps.species_name) = LOWER(stg.normalized_species_name)
);

UPDATE stg_insect_observations
SET nbn_pollinator_code = 'NHMSYS0000875202'
WHERE nbn_pollinator_code != 'NHMSYS0000875202'
	AND LOWER(pollinator_species) = LOWER('Andrena flavipes');
UPDATE stg_insect_observations
SET nbn_pollinator_code = 'NHMSYS0000875215'
WHERE nbn_pollinator_code != 'NHMSYS0000875215'
	AND LOWER(pollinator_species) = LOWER('Andrena haemorrhoa');
UPDATE stg_insect_observations
SET nbn_pollinator_code = 'NHMSYS0000875177'
WHERE nbn_pollinator_code != 'NHMSYS0000875177'
	AND LOWER(pollinator_species) = LOWER('Andrena cineraria');
UPDATE stg_insect_observations
SET nbn_pollinator_code = 'NHMSYS0000875423'
WHERE nbn_pollinator_code != 'NHMSYS0000875423'
	AND LOWER(pollinator_species) = LOWER('Apis mellifera');
UPDATE stg_insect_observations
SET nbn_pollinator_code = 'NBNSYS0000006866'
WHERE nbn_pollinator_code != 'NBNSYS0000006866'
	AND LOWER(pollinator_species) = LOWER('Melanostoma mellinum');
UPDATE stg_insect_observations
SET nbn_pollinator_code = 'NHMSYS0000875268'
WHERE nbn_pollinator_code != 'NHMSYS0000875268'
	AND LOWER(pollinator_species) = LOWER('Andrena scotica');
----------------------------------
----------------------------------

--insert into pollinator_species
WITH formatted AS (
	SELECT DISTINCT
	UPPER(nbn_pollinator_code) AS normalized_nbn_code,
	INITCAP(SPLIT_PART(pollinator_species, ' ', 1)) ||
		CASE 
			WHEN POSITION(' ' IN pollinator_species) > 0 
			THEN ' ' || LOWER(SUBSTRING(pollinator_species FROM POSITION(' ' IN pollinator_species) + 1))
			ELSE '' 
		END AS normalized_species_name
	FROM stg_insect_observations
	WHERE pollinator_species IS NOT NULL
)
INSERT INTO pollinator_species (nbn_code, species_name)
SELECT
	normalized_nbn_code,
	normalized_species_name
FROM formatted AS stg
WHERE NOT EXISTS (
	SELECT 1 FROM pollinator_species ps
	WHERE LOWER(ps.species_name) = LOWER(stg.normalized_species_name)
);

-- insert into castes
INSERT INTO castes (caste_name)
SELECT DISTINCT TRIM(caste)
FROM stg_insect_observations
WHERE caste IS NOT NULL
	AND TRIM(caste) NOT IN (
		SELECT caste_name FROM castes
	);

-- insert into pollinator_caste
INSERT INTO pollinator_caste (pollinator_id, caste_id)
SELECT DISTINCT
	ps.pollinator_id,
	c.caste_id
FROM stg_insect_observations AS stg
JOIN pollinator_species ps
	ON LOWER(ps.species_name) = LOWER(stg.pollinator_species)
JOIN castes c
	ON c.caste_name = TRIM(stg.caste)
WHERE stg.pollinator_species IS NOT NULL
	AND stg.caste IS NOT NULL
	AND NOT EXISTS (
		SELECT 1 FROM pollinator_caste pc
		WHERE pc.pollinator_id = ps.pollinator_id
			AND pc.caste_id = c.caste_id
	);

-- insert into locations
INSERT INTO locations (latitude, longitude)
SELECT DISTINCT
	CAST(latitude AS DECIMAL(9,6)),
	CAST(longitude AS DECIMAL(9,6))
FROM stg_insect_observations
WHERE latitude IS NOT NULL AND longitude IS NOT NULL
	AND NOT EXISTS (
		SELECT 1 FROM locations l
		WHERE l.latitude = CAST(stg_insect_observations.latitude AS DECIMAL(9,6))
			AND l.longitude = CAST(stg_insect_observations.longitude AS DECIMAL(9,6))
	);

-- insert into habitats
INSERT INTO habitats (habitat_name)
SELECT DISTINCT INITCAP(TRIM(habitat))
FROM stg_insect_observations
WHERE habitat IS NOT NULL
	AND LOWER(TRIM(habitat)) NOT IN (
		SELECT LOWER(habitat_name) FROM habitats
	);

--------------------------------
-- INSERT INTO OBSERVATIONS
--------------------------------
--fix a data error
UPDATE stg_insect_observations
SET obs_date = '30'
WHERE obs_date = '31'
	AND obs_month IN ('04', '4', '06', '6', '09', '9', '11');
UPDATE stg_insect_observations
SET obs_date = '28'
WHERE obs_date IN ('31', '30', '29')
	AND obs_month IN ('02', '2');

--NOTE: placeholder month value 01 (January) is chosen to indicate observations with missing month value, as it is important for analytics
WITH stg_with_date AS ( --build the full date
	SELECT 
		*,
		CASE
		  WHEN obs_year IS NULL THEN NULL
		  WHEN obs_month IS NULL THEN TO_DATE(obs_year || '-01-01', 'YYYY-MM-DD')
		  WHEN obs_date IS NULL THEN TO_DATE(obs_year || '-' || LPAD(obs_month, 2, '0') || '-01', 'YYYY-MM-DD')
	  	ELSE TO_DATE(obs_year || '-' || LPAD(obs_month, 2, '0') || '-' || LPAD(obs_date, 2, '0'), 'YYYY-MM-DD')
	END AS observation_date
	FROM stg_insect_observations
), stg_with_user AS (
	SELECT 
		s.*,
		u.user_id
	FROM stg_with_date s
	JOIN users u ON s.authors ILIKE '%' || u.last_name || '%'
), possible_affiliations AS (
	SELECT 
		s.*,
		ui.user_institution_id,
		ui.start_date,
		CASE
	  		WHEN s.observation_date IS NOT NULL
				AND ui.start_date <= s.observation_date
				AND (ui.end_date IS NULL OR s.observation_date <= ui.end_date)
	  		THEN 1
	  		WHEN s.observation_date IS NULL AND ui.end_date IS NULL THEN 2
	  		WHEN s.observation_date IS NOT NULL AND s.observation_date < ui.start_date THEN 3
	  		ELSE 4
			END AS affiliation_priority
	FROM stg_with_user s
	JOIN user_institution ui ON s.user_id = ui.user_id
), stg_with_affiliation AS (
	SELECT DISTINCT ON (raw_data_id)
		*
	FROM possible_affiliations
	ORDER BY raw_data_id, affiliation_priority, start_date
)
INSERT INTO observations (
	user_institution_id,
	plant_id,
	pollinator_id,
	caste_id,
	location_id,
	habitat_id,
	observation_date,
	interaction_count,
	pollination_quality_id,
	pollen_collected,
	nectar_collected
)
SELECT 
	s.user_institution_id,
	ps.plant_id,
	po.pollinator_id,
	c.caste_id,
	l.location_id,
	h.habitat_id,
	s.observation_date,
	s.interactions::INTEGER,
	s.pollination::INTEGER,
	s.pollen::BOOLEAN,
	s.nectar::BOOLEAN
FROM stg_with_affiliation s
JOIN plant_species ps
	ON s.plant_species = ps.species_name
JOIN pollinator_species po
	ON s.pollinator_species = po.species_name
LEFT JOIN castes c
	ON s.caste = c.caste_name
LEFT JOIN locations l
	ON CAST(s.latitude AS DECIMAL(9,6)) = l.latitude
	AND CAST(s.longitude AS DECIMAL(9,6)) = l.longitude
LEFT JOIN habitats h
	ON INITCAP(TRIM(s.habitat)) = h.habitat_name;

-- clean up staging
DELETE FROM stg_insect_observations;
DROP TABLE tmp_insect_observations_full;


