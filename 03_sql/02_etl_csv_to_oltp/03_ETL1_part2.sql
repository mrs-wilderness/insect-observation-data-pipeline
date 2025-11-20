-- NOTE: ETL1 uses staging tables to clean and transform raw DOPI CSV data
-- before inserting into normalized OLTP tables.
-- Update file paths before running.

-- create staging table for users
 CREATE TABLE IF NOT EXISTS stg_users (
	full_name TEXT,
	username TEXT,
	email TEXT,
	institution TEXT,
	affiliation_start TEXT,
	city TEXT,
	county TEXT,
	subscription_type TEXT,
	subscription_start TEXT,
	join_date TEXT,
	country TEXT DEFAULT 'United Kingdom'
);

-- create table for invalid rows
CREATE TABLE IF NOT EXISTS invalid_stg_users (
	full_name TEXT,
	username TEXT,
	email TEXT,
	institution TEXT,
	affiliation_start TEXT,
	city TEXT,
	county TEXT,
	subscription_type TEXT,
	subscription_start TEXT,
	join_date TEXT,
	country TEXT,
	problem TEXT
);

-- load the data from the users csv
COPY stg_users (
	full_name,
	username,
	email,
	institution,
	affiliation_start,
	city,
	county,
	subscription_type,
	subscription_start,
	join_date
)
FROM 'absolute\path\to\data\users_mock.csv'
DELIMITER ','
CSV HEADER;

-- detect invalid rows and move them to error-handling table (null/empty non-nullables)
INSERT INTO invalid_stg_users (full_name, username, email, institution, affiliation_start, city, county,
	subscription_type, subscription_start, join_date, country, problem)
SELECT full_name, username, email, institution, affiliation_start, city, county,
	subscription_type, subscription_start, join_date, country,
	'missing required field or invalid date format'
FROM stg_users
WHERE
	full_name IS NULL OR full_name = '' OR
	username IS NULL OR username = '' OR
	email IS NULL OR email = '' OR
	institution IS NULL OR institution = '' OR
	subscription_type IS NULL OR subscription_type = '' OR
	join_date IS NULL OR join_date = '' OR
	subscription_start IS NULL OR subscription_start = '' OR
	affiliation_start IS NULL OR affiliation_start = ''
	OR TO_DATE(join_date, 'YYYY-MM-DD') IS NULL
	OR TO_DATE(subscription_start, 'YYYY-MM-DD') IS NULL
	OR TO_DATE(affiliation_start, 'YYYY-MM-DD') IS NULL;
DELETE FROM stg_users
WHERE username IN (SELECT username FROM invalid_stg_users)
	OR username IS NULL;

-- insert into countries
INSERT INTO countries (country_name)
SELECT DISTINCT country
FROM stg_users
WHERE country IS NOT NULL
	AND country NOT IN (
		SELECT country_name FROM countries
	);

-- insert into counties
INSERT INTO counties (county_name, country_id)
SELECT DISTINCT stg.county, c.country_id
FROM stg_users AS stg
JOIN countries AS c
ON stg.country = c.country_name
WHERE stg.county IS NOT NULL
	AND stg.county NOT IN (
		SELECT county_name FROM counties
	);

-- insert into cities
INSERT INTO cities (city_name, county_id)
SELECT DISTINCT stg.city, co.county_id
FROM stg_users AS stg
JOIN counties AS co
ON stg.county = co.county_name
WHERE stg.city IS NOT NULL
	AND stg.city NOT IN (
		SELECT city_name FROM cities
	);

-- insert into users
WITH deduplicated AS (
	SELECT DISTINCT ON (stg.username)
		stg.username,
		stg.email,
	CASE
		WHEN stg.full_name LIKE '%.% %' THEN
			LEFT(stg.full_name, LENGTH(stg.full_name) - POSITION(' ' IN REVERSE(stg.full_name)))
		ELSE NULL
	END AS first_name_or_initials,
	CASE
		WHEN stg.full_name LIKE '%.% %' THEN
			SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(stg.full_name), ' ', 1)), ' ', 1)
		ELSE stg.full_name
	END AS last_name,
		cit.city_id,
		TO_DATE(stg.join_date, 'YYYY-MM-DD') AS join_date
	FROM stg_users AS stg
	LEFT JOIN cities AS cit ON stg.city = cit.city_name
	ORDER BY stg.username, stg.join_date
)
INSERT INTO users (username, email, first_name_or_initials, last_name, city_id, join_date)
SELECT *
FROM deduplicated
WHERE username NOT IN (
	SELECT username FROM users
);

-- insert into user_subscriptions
-- NOTE: this table is implemented as SCD2 (see README.md)
WITH deduplicated AS (
	SELECT DISTINCT ON (u.user_id, st.subscription_type_id, TO_DATE(stg.subscription_start, 'YYYY-MM-DD'))
		u.user_id,
		st.subscription_type_id,
		TO_DATE(stg.subscription_start, 'YYYY-MM-DD') AS start_date
	FROM stg_users AS stg
	JOIN users AS u
	ON stg.username = u.username
	JOIN subscription_types AS st
	ON stg.subscription_type = st.type_name
	ORDER BY u.user_id, st.subscription_type_id, TO_DATE(stg.subscription_start, 'YYYY-MM-DD')
)
INSERT INTO user_subscription (user_id, subscription_type_id, start_date)
SELECT *
FROM deduplicated
WHERE NOT EXISTS (
	SELECT 1 FROM user_subscription us
	WHERE us.user_id = deduplicated.user_id
		AND us.subscription_type_id = deduplicated.subscription_type_id
		AND us.start_date = deduplicated.start_date
);
--update end_dates for old subscriptions
WITH ordered AS (
	SELECT
		u.user_id,
		st.subscription_type_id,
		TO_DATE(stg.subscription_start, 'YYYY-MM-DD') AS start_date,
		LAG(st.subscription_type_id) OVER (
			PARTITION BY stg.username ORDER BY TO_DATE(stg.subscription_start, 'YYYY-MM-DD')
		) AS previous_type_id,
		LAG(TO_DATE(stg.subscription_start, 'YYYY-MM-DD')) OVER (
			PARTITION BY stg.username ORDER BY TO_DATE(stg.subscription_start, 'YYYY-MM-DD')
		) AS previous_start
	FROM stg_users AS stg
	JOIN users u
	ON stg.username = u.username
	JOIN subscription_types st
	ON stg.subscription_type = st.type_name
),
deduplicated AS (
	SELECT DISTINCT user_id, subscription_type_id, start_date, previous_type_id, previous_start
	FROM ordered
	WHERE previous_start IS NOT NULL
		AND (subscription_type_id != previous_type_id OR start_date != previous_start)
)
UPDATE user_subscription AS old_sub
SET end_date = d.start_date - INTERVAL '1 day'
FROM deduplicated AS d
WHERE old_sub.user_id = d.user_id
	AND old_sub.subscription_type_id = d.previous_type_id
	AND old_sub.start_date = d.previous_start
	AND old_sub.end_date IS NULL;

-- insert into user_institution
-- NOTE: this table is implemented as SCD2 (see README.md)
WITH deduplicated AS (
	SELECT DISTINCT ON (u.user_id, i.institution_id, TO_DATE(stg.affiliation_start, 'YYYY-MM-DD'))
		u.user_id,
		i.institution_id,
		TO_DATE(stg.affiliation_start, 'YYYY-MM-DD') AS start_date
	FROM stg_users AS stg
	JOIN users AS u
	ON u.username = stg.username
	JOIN institutions AS i
	ON i.institution_name = stg.institution
	ORDER BY u.user_id, i.institution_id, TO_DATE(stg.affiliation_start, 'YYYY-MM-DD')
)
INSERT INTO user_institution (user_id, institution_id, start_date)
SELECT d.user_id, d.institution_id, d.start_date
FROM deduplicated AS d
WHERE NOT EXISTS (
	SELECT 1 FROM user_institution AS ui
	WHERE ui.user_id = d.user_id
	  AND ui.institution_id = d.institution_id
	  AND ui.start_date = d.start_date
);
-- update end_dates for old affiliations
WITH ordered AS (
	SELECT
		ui.user_institution_id,
		ui.user_id,
		ui.institution_id,
		ui.start_date,
		LAG(ui.institution_id) OVER (
			PARTITION BY ui.user_id ORDER BY ui.start_date
		) AS previous_institution_id,
		LAG(ui.start_date) OVER (
			PARTITION BY ui.user_id ORDER BY ui.start_date
		) AS previous_start_date,
		LAG(ui.user_institution_id) OVER (
			PARTITION BY ui.user_id ORDER BY ui.start_date
		) AS previous_ui_id
	FROM user_institution AS ui
),
to_update AS (
	SELECT
		previous_ui_id AS user_institution_id_to_update,
		start_date AS new_start_date
	FROM ordered
	WHERE previous_start_date IS NOT NULL
		AND (
			institution_id != previous_institution_id
			OR start_date != previous_start_date
		)
)
UPDATE user_institution AS ui
SET end_date = to_update.new_start_date - INTERVAL '1 day'
FROM to_update
WHERE ui.user_institution_id = to_update.user_institution_id_to_update
	AND ui.end_date IS NULL;

-- clean up staging
DELETE FROM stg_users;