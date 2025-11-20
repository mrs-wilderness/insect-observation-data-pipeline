-- DIMENSION TABLES

CREATE TABLE dim_pollinator (
	pollinator_sk SERIAL PRIMARY KEY,
	pollinator_id_bk INTEGER NOT NULL,
	nbn_code TEXT,
	species_name TEXT UNIQUE NOT NULL
);

CREATE TABLE dim_caste (
	caste_sk SERIAL PRIMARY KEY,
	caste_id_bk INTEGER NOT NULL,
	caste_name TEXT UNIQUE NOT NULL
);

CREATE TABLE dim_plant (
	plant_sk SERIAL PRIMARY KEY,
	plant_id_bk INTEGER NOT NULL,
	nbn_code TEXT,
	species_name TEXT UNIQUE NOT NULL
);

CREATE TABLE dim_habitat (
	habitat_sk SERIAL PRIMARY KEY,
	habitat_id_bk INTEGER NOT NULL,
	habitat_name TEXT UNIQUE NOT NULL
);

CREATE TABLE dim_location (
	location_sk SERIAL PRIMARY KEY,
	location_id_bk INTEGER NOT NULL,
	latitude DECIMAL(9,6) NOT NULL CHECK (latitude BETWEEN -90 AND 90),
	longitude DECIMAL(9,6) NOT NULL CHECK (longitude BETWEEN -180 AND 180),
	UNIQUE (latitude, longitude)
);

CREATE TABLE dim_date (
	date_sk SERIAL PRIMARY KEY,
	year INTEGER NOT NULL CHECK (year BETWEEN 1800 AND 2100),
	month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
	month_name TEXT NOT NULL,
	month_year TEXT NOT NULL,
	UNIQUE (year, month)
);

CREATE TABLE dim_user (
	user_sk SERIAL PRIMARY KEY,
	user_id_bk INTEGER NOT NULL,
	username TEXT NOT NULL UNIQUE,
	last_name TEXT NOT NULL,
	first_name_or_initials TEXT
);

CREATE TABLE dim_subscription_type (
	subscription_type_sk SERIAL PRIMARY KEY,
	user_id_bk INTEGER NOT NULL,
	subscription_type_id_bk INTEGER NOT NULL,
	type_name TEXT NOT NULL,
	start_date DATE NOT NULL,
	end_date DATE,
	CHECK (end_date IS NULL OR end_date > start_date)
);


CREATE TABLE dim_institution (
	institution_sk SERIAL PRIMARY KEY,
	user_id_bk INTEGER NOT NULL,
	institution_id_bk INTEGER NOT NULL,
	institution_name TEXT NOT NULL,
	start_date DATE NOT NULL,
	end_date DATE,
	CHECK (end_date IS NULL OR end_date > start_date)
);

-- BRIDGE TABLE
CREATE TABLE bridge_pollinator_caste (
	pollinator_sk INTEGER REFERENCES dim_pollinator(pollinator_sk),
	caste_sk INTEGER REFERENCES dim_caste(caste_sk),
	PRIMARY KEY (pollinator_sk, caste_sk)
);


-- FACT TABLES

CREATE TABLE fact_pollination_activity (
	pollination_activity_sk SERIAL PRIMARY KEY,
	pollinator_sk INTEGER REFERENCES dim_pollinator(pollinator_sk) NOT NULL,
	caste_sk INTEGER REFERENCES dim_caste(caste_sk),
	plant_sk INTEGER REFERENCES dim_plant(plant_sk) NOT NULL,
	habitat_sk INTEGER REFERENCES dim_habitat(habitat_sk),
	location_sk INTEGER REFERENCES dim_location(location_sk) NOT NULL,
	date_sk INTEGER REFERENCES dim_date(date_sk) NOT NULL,
	interaction_count INTEGER NOT NULL DEFAULT 0,
	observation_count INTEGER NOT NULL DEFAULT 0,
	pollen_collected INTEGER NOT NULL DEFAULT 0,
	nectar_collected INTEGER NOT NULL DEFAULT 0,
	confirmed_pollination_count INTEGER NOT NULL DEFAULT 0
);
ALTER TABLE fact_pollination_activity
ADD CONSTRAINT fact_pollination_activity_unique
UNIQUE (
	pollinator_sk,
	caste_sk,
	plant_sk,
	habitat_sk,
	location_sk,
	date_sk
);

CREATE TABLE fact_user_location_monthly (
	user_location_monthly_sk SERIAL PRIMARY KEY,
	user_sk INTEGER REFERENCES dim_user(user_sk) NOT NULL,
	location_sk INTEGER REFERENCES dim_location(location_sk) NOT NULL,
	date_sk INTEGER REFERENCES dim_date(date_sk) NOT NULL,
	institution_sk INTEGER REFERENCES dim_institution(institution_sk) NOT NULL,
	subscription_type_sk INTEGER REFERENCES dim_subscription_type(subscription_type_sk) NOT NULL,
	observation_count INTEGER NOT NULL DEFAULT 0,
	interaction_count INTEGER NOT NULL DEFAULT 0
);
ALTER TABLE fact_user_location_monthly
ADD CONSTRAINT fact_user_location_monthly_unique
UNIQUE (
	user_sk,
	location_sk,
	date_sk,
	institution_sk,
	subscription_type_sk
);

CREATE TABLE fact_user_monthly_summary (
	user_monthly_summary_sk SERIAL PRIMARY KEY,
	user_sk INTEGER REFERENCES dim_user(user_sk) NOT NULL,
	date_sk INTEGER REFERENCES dim_date(date_sk) NOT NULL,
	institution_sk INTEGER REFERENCES dim_institution(institution_sk) NOT NULL,
	subscription_type_sk INTEGER REFERENCES dim_subscription_type(subscription_type_sk) NOT NULL,
	observation_count INTEGER NOT NULL DEFAULT 0,
	interaction_count INTEGER NOT NULL DEFAULT 0,
	distinct_locations_count INTEGER NOT NULL DEFAULT 0,
	distinct_habitats_count INTEGER NOT NULL DEFAULT 0,
	distinct_pollinator_species_count INTEGER NOT NULL DEFAULT 0,
	distinct_plant_species_count INTEGER NOT NULL DEFAULT 0
);
ALTER TABLE fact_user_monthly_summary
ADD CONSTRAINT fact_user_monthly_summary_unique
UNIQUE (
	user_sk,
	date_sk,
	institution_sk,
	subscription_type_sk
);
