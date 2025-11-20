# Insect Observation Data Pipeline (CSV → OLTP → OLAP + BI)

This project implements a small end-to-end data pipeline for a fictional insect-observation app, combining a real ecological dataset (DOPI) with light mock data for users and institutions. It includes normalized OLTP tables, a two-stage ETL flow (CSV → OLTP → OLAP), an OLAP warehouse, and a Power BI report.

## Background and Task-Imposed Structure

This project was originally developed as a university SQL data-processing assignment, which imposed several architectural constraints that shaped the final structure. For instance, the task required separate OLTP and OLAP PostgreSQL databases, a connection between them via FDW, and a two-stage ETL flow. The biological data (DOPI) is real; user and institution data are partially synthetic to support the fictional app scenario required by the assignment.

## Pipeline Structure

- Raw CSVs (DOPI + mock app data)  
- OLTP (normalized)  
- Two ETL stages (CSV → OLTP → OLAP)  
- OLAP warehouse  
- Power BI report  

## Limitations and Notes

- The records from DOPI dataset are allowed to be full duplicates, and the check on the surrogate record key was not implemented as the task did not allow using surrogate keys from csv files. Running the part of the ETL that loads data from `insect_observations.csv` twice will **and should** result in duplicating the full observations dataset in the database.

- The DOPI dataset contains inconsistent fields, so ETL1 includes manual corrections for specific known issues.

- The Power BI report (`04_bi/insect_observation_report.pbix`) was created by importing data from the OLAP database; they are not connected live. The report contains an embedded snapshot of the data, so it can be opened directly without any database connection.  
  The choice to have two separate slicers for month and year, rather than implementing a date hierarchy, was made because the domain is centered around seasonal trends, so it is important to be able to filter by both independently.

- Mixing real historical data with synthetic app metadata required a few pragmatic assumptions in affiliation and subscription logic. As the dates of the user subscriptions and affiliation do not cover the whole date range of the observations dataset, we allow users to enter observations that happened prior to joining.  
  The logic of mapping affiliation to an observation is as follows: dates earlier than first affiliation date map to the earliest affiliation. Mapping subscription type to observations follows the same logic.

- Both `user_institution` and `user_subscription` in the OLTP DB are designed to not overwrite values upon change and are implemented as SCD2. In our fictional app, we want to tie an observation to an institution or subscription type.

- The choice to have both `pollinator_id` and `caste_id` in observations, and create a `pollinator_caste` table was conscious. One caste can be observed in many species, so it is a separate property of an observation. Yet the caste is also a property of the species, and the information about which pollinator–caste combinations are present in our database can be considered an entity of its own.

- Locations are a separate entity as there are only 500+ locations for 18K+ rows.

## Data

This project uses a combination of real ecological data and lightweight synthetic data added for the app context.

### Insect Observation Data (DOPI – Database of Pollinator Interactions)

The insect-observation data comes from the DOPI database:  
https://www.sussex.ac.uk/lifesci/ebe/dopi/about.

It includes plant species, pollinator species, habitats, caste information, pollination evidence types, geographic coordinates, author names, etc.  
This particular subset includes observations for two types of habitat only: urban and suburban.

### User Data (hybrid)

User records are partially derived from author names present in the DOPI dataset, as a way to tie the synthetic data to the real data. The names were taken from the “Authors” column of the DOPI dataset and trimmed to resemble full names.

Additional attributes (email, location, subscription history, institutional affiliation) are synthetic and added to support the fictional app scenario required by the assignment.

### Institution Data (synthetic)

Institutions and affiliation history are synthetic and exist to support user–institution relationships required for filtering and analytics in the fictional app.

## Analytical Queries

The OLAP warehouse schema reflects a set of fictional analytical questions for the insect-observation app. The repository includes a small set of OLTP and OLAP queries used to inspect the loaded data and illustrate the types of analyses the warehouse supports.

## How to Run

1. Create PostgreSQL OLTP and OLAP databases.  
2. Run **OLTP schema → ETL1 → OLAP schema → FDW connection → ETL2** (see `03_sql/` folder).  
3. Adjust CSV paths and connection credentials where indicated.  
4. The Power BI report uses imported data and does not require a database connection.
