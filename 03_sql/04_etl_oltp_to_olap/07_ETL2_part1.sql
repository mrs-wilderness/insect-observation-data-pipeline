-- NOTE: staging.* tables come from the OLTP database via FDW.
-- NOTE: SCD data is sourced from OLTP; OLAP only stores it (see README.md).

-- INSERT INTO DIM TABLES
INSERT INTO dim_plant (plant_id_bk, nbn_code, species_name)
SELECT
	plant_id,
	nbn_code,
	species_name
FROM staging.plant_species
ON CONFLICT (species_name) DO NOTHING;

INSERT INTO dim_pollinator (pollinator_id_bk, nbn_code, species_name)
SELECT
	pollinator_id,
	nbn_code,
	species_name
FROM staging.pollinator_species
ON CONFLICT (species_name) DO NOTHING;

INSERT INTO dim_caste (caste_id_bk, caste_name)
SELECT
	caste_id,
	caste_name
FROM staging.castes
ON CONFLICT (caste_name) DO NOTHING;

INSERT INTO dim_habitat (habitat_id_bk, habitat_name)
SELECT
	habitat_id,
	habitat_name
FROM staging.habitats
ON CONFLICT (habitat_name) DO NOTHING;

INSERT INTO dim_location (location_id_bk, latitude, longitude)
SELECT
	location_id,
	latitude,
	longitude
FROM staging.locations
ON CONFLICT (latitude, longitude) DO NOTHING;

INSERT INTO dim_user (user_id_bk, username, last_name, first_name_or_initials)
SELECT
	user_id,
	username,
	last_name,
	first_name_or_initials
FROM staging.users
ON CONFLICT (username) DO NOTHING;

--dim_date requires some tweaking
INSERT INTO dim_date (year, month, month_name, month_year)
SELECT DISTINCT
	EXTRACT(YEAR FROM observation_date)::INT AS year,
	EXTRACT(MONTH FROM observation_date)::INT AS month,
	TO_CHAR(observation_date, 'TMMonth') AS month_name,
	TO_CHAR(observation_date, 'YYYY-MM') AS month_year
FROM staging.observations
WHERE observation_date IS NOT NULL
	AND (EXTRACT(YEAR FROM observation_date)::INT,
		EXTRACT(MONTH FROM observation_date)::INT)
	NOT IN (SELECT year, month FROM dim_date)
ON CONFLICT (year, month) DO NOTHING;


-------------------------------
--HANDLING SCD
-------------------------------
INSERT INTO dim_subscription_type (
	user_id_bk,
	subscription_type_id_bk,
	type_name,
	start_date,
	end_date
)
SELECT
	us.user_id,
	us.subscription_type_id,
	st.type_name,
	us.start_date,
	us.end_date
FROM staging.user_subscription us
JOIN staging.subscription_types st
	USING (subscription_type_id)
LEFT JOIN dim_subscription_type ol
	ON ol.user_id_bk = us.user_id
		AND ol.subscription_type_id_bk = us.subscription_type_id
		AND ol.start_date = us.start_date
WHERE ol.user_id_bk IS NULL;

UPDATE dim_subscription_type ol
SET end_date = us.end_date
FROM staging.user_subscription us
WHERE ol.user_id_bk = us.user_id
	AND ol.subscription_type_id_bk = us.subscription_type_id
	AND ol.start_date = us.start_date
	AND ol.end_date IS NULL
	AND us.end_date IS NOT NULL;

INSERT INTO dim_institution (
	user_id_bk,
	institution_id_bk,
	institution_name,
	start_date,
	end_date
)
SELECT
	ui.user_id,
	ui.institution_id,
	i.institution_name,
	ui.start_date,
	ui.end_date
FROM staging.user_institution ui
JOIN staging.institutions i
	USING (institution_id)
LEFT JOIN dim_institution o
	ON o.user_id_bk = ui.user_id
		AND o.institution_id_bk = ui.institution_id
		AND o.start_date = ui.start_date
WHERE o.user_id_bk IS NULL;

UPDATE dim_institution o
SET end_date = ui.end_date
FROM staging.user_institution ui
WHERE o.user_id_bk = ui.user_id
	AND o.institution_id_bk = ui.institution_id
	AND o.start_date = ui.start_date
	AND o.end_date IS NULL
	AND ui.end_date IS NOT NULL;

--load into bridge
INSERT INTO bridge_pollinator_caste (pollinator_sk, caste_sk)
SELECT DISTINCT
	p.pollinator_sk,
	c.caste_sk
FROM staging.pollinator_caste pc
JOIN dim_pollinator p 
	ON p.pollinator_id_bk = pc.pollinator_id
JOIN dim_caste c 
	ON c.caste_id_bk = pc.caste_id
LEFT JOIN bridge_pollinator_caste b
	ON b.pollinator_sk = p.pollinator_sk
		AND b.caste_sk = c.caste_sk
WHERE b.pollinator_sk IS NULL;

