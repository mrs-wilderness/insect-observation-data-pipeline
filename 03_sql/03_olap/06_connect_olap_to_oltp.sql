-- NOTE: Replace placeholder connection credentials (dbname, user, password) before running.

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

DROP SERVER IF EXISTS oltp_srv CASCADE;
CREATE SERVER oltp_srv
	FOREIGN DATA WRAPPER postgres_fdw
	OPTIONS (host 'localhost', dbname '<YOUR_OLTP_DB_NAME>', port '5432');

CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
	SERVER oltp_srv
	OPTIONS (user '<your_user>', password '<your_password>');

DROP SCHEMA IF EXISTS staging CASCADE;
CREATE SCHEMA staging;
IMPORT FOREIGN SCHEMA public
	FROM SERVER oltp_srv
	INTO staging;

