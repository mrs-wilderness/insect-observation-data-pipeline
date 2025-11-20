-- create tables: countries - counties - cities
CREATE TABLE countries (
	country_id SERIAL PRIMARY KEY,
	country_name TEXT NOT NULL
);

CREATE TABLE counties (
	county_id SERIAL PRIMARY KEY,
	county_name TEXT NOT NULL,
	country_id INTEGER NOT NULL REFERENCES countries(country_id)
);

CREATE TABLE cities (
	city_id SERIAL PRIMARY KEY,
	city_name TEXT NOT NULL,
	county_id INTEGER NOT NULL REFERENCES counties(county_id)
);


-- create table users
-- mock user data uses condensed first names, hence 'first_name_or_initials' field
CREATE TABLE users (
	user_id SERIAL PRIMARY KEY,
	username TEXT UNIQUE NOT NULL,
	email TEXT NOT NULL,
	first_name_or_initials TEXT,
	last_name TEXT NOT NULL,
	city_id INTEGER REFERENCES cities(city_id),
	join_date DATE NOT NULL
);


-- create table institutions
CREATE TABLE institutions (
	institution_id SERIAL PRIMARY KEY,
	institution_name TEXT NOT NULL,
	city_id INTEGER REFERENCES cities(city_id)
);

-- create table subscription_types
CREATE TABLE subscription_types (
	subscription_type_id SERIAL PRIMARY KEY,
	type_name TEXT UNIQUE NOT NULL
);

-- create link tables
CREATE TABLE user_institution (
	user_institution_id SERIAL PRIMARY KEY,
	user_id INTEGER NOT NULL REFERENCES users(user_id),
	institution_id INTEGER NOT NULL REFERENCES institutions(institution_id),
	start_date DATE NOT NULL,
	end_date DATE
);

CREATE TABLE user_subscription (
	user_subscription_id SERIAL PRIMARY KEY,
	user_id INTEGER NOT NULL REFERENCES users(user_id),
	subscription_type_id INTEGER NOT NULL REFERENCES subscription_types(subscription_type_id),
	start_date DATE NOT NULL,
	end_date DATE
);


-- create other misc tables
CREATE TABLE locations (
	location_id SERIAL PRIMARY KEY,
	latitude DECIMAL(9,6) NOT NULL CHECK (latitude BETWEEN -90 AND 90),
	longitude DECIMAL(9,6) NOT NULL CHECK (longitude BETWEEN -180 AND 180),
	UNIQUE (latitude, longitude)
);

CREATE TABLE habitats (
	habitat_id SERIAL PRIMARY KEY,
	habitat_name TEXT UNIQUE NOT NULL
);

CREATE TABLE plant_species (
	plant_id SERIAL PRIMARY KEY,
	nbn_code TEXT UNIQUE,
	species_name TEXT UNIQUE NOT NULL
);

CREATE TABLE pollinator_species (
	pollinator_id SERIAL PRIMARY KEY,
	nbn_code TEXT UNIQUE,
	species_name TEXT UNIQUE NOT NULL
);

CREATE TABLE castes (
	caste_id SERIAL PRIMARY KEY,
	caste_name TEXT UNIQUE NOT NULL
);

-- create pollinator_caste link table
CREATE TABLE pollinator_caste (
	pollinator_caste_id SERIAL PRIMARY KEY,
	pollinator_id INTEGER NOT NULL REFERENCES pollinator_species(pollinator_id),
	caste_id INTEGER NOT NULL REFERENCES castes(caste_id),
	UNIQUE (pollinator_id, caste_id)
);


-- create pollination qualities table
CREATE TABLE pollination_qualities (
	pollination_quality_id SERIAL PRIMARY KEY,
	quality_code SMALLINT UNIQUE NOT NULL,
	description TEXT NOT NULL
);

-- create observations table
CREATE TABLE observations (
	observation_id SERIAL PRIMARY KEY,
	user_institution_id INTEGER NOT NULL REFERENCES user_institution(user_institution_id),
	plant_id INTEGER NOT NULL REFERENCES plant_species(plant_id),
	pollinator_id INTEGER NOT NULL REFERENCES pollinator_species(pollinator_id),
	caste_id INTEGER REFERENCES castes(caste_id),
	location_id INTEGER REFERENCES locations(location_id),
	habitat_id INTEGER REFERENCES habitats(habitat_id),
	observation_date DATE,
	interaction_count INTEGER,
	pollination_quality_id INTEGER REFERENCES pollination_qualities(pollination_quality_id),
	pollen_collected BOOLEAN,
	nectar_collected BOOLEAN
);


----------------
-- Insert static reference data for subscription types and pollination quality categories.
----------------
INSERT INTO subscription_types (type_name)
VALUES 
    ('Free'),
    ('Pro'),
    ('HiveMind'),
    ('FieldScout'),
	('BeeWatch+')
ON CONFLICT (type_name) DO NOTHING;

INSERT INTO pollination_qualities (quality_code, description)
VALUES 
    (1, 'pollination confirmed'),
    (2, 'pollination inferred'),
    (3, 'pollination inferred from circumstantial evidence'),
    (4, 'no pollination, the visitor is not a pollinator')
ON CONFLICT (quality_code) DO NOTHING;

-- insert a row for unaffiliated into institutions
INSERT INTO institutions (institution_name)
VALUES ('Unaffiliated')
ON CONFLICT DO NOTHING;
