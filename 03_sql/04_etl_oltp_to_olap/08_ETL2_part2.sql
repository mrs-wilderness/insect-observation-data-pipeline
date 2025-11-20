-- NOTE: staging.* tables come from the OLTP database via FDW.

-- load into fact_pollination_activity

INSERT INTO fact_pollination_activity (
	pollinator_sk,
	caste_sk,
	plant_sk,
	habitat_sk,
	location_sk,
	date_sk,
	interaction_count,
	observation_count,
	pollen_collected,
	nectar_collected,
	confirmed_pollination_count
)
SELECT
	p.pollinator_sk,
	c.caste_sk,
	pl.plant_sk,
	h.habitat_sk,
	l.location_sk,
	d.date_sk,
	COALESCE(SUM(ob.interaction_count), 0) AS interaction_count,
	COALESCE(COUNT(*), 0) AS observation_count,
	COALESCE(SUM(CASE WHEN ob.pollen_collected THEN 1 ELSE 0 END), 0) AS pollen_collected,
	COALESCE(SUM(CASE WHEN ob.nectar_collected  THEN 1 ELSE 0 END), 0) AS nectar_collected,
	COALESCE(SUM(
		CASE
			WHEN stq.quality_code IS NOT NULL
				AND stq.quality_code <> 4
			THEN 1 ELSE 0
			END
	), 0) AS confirmed_pollination_count
FROM staging.observations ob
JOIN dim_pollinator p
	ON p.pollinator_id_bk = ob.pollinator_id
LEFT JOIN dim_caste c
	ON ob.caste_id IS NOT NULL
		AND c.caste_id_bk = ob.caste_id
JOIN dim_plant pl
	ON pl.plant_id_bk = ob.plant_id
LEFT JOIN dim_habitat h
	ON ob.habitat_id IS NOT NULL
		AND h.habitat_id_bk = ob.habitat_id
JOIN dim_location l
	ON ob.location_id IS NOT NULL
		AND l.location_id_bk = ob.location_id
JOIN dim_date d
	ON d.year = EXTRACT(YEAR FROM ob.observation_date)::INT
		AND d.month = EXTRACT(MONTH FROM ob.observation_date)::INT
LEFT JOIN staging.pollination_qualities stq
	ON stq.pollination_quality_id = ob.pollination_quality_id
WHERE ob.observation_date IS NOT NULL
	AND ob.location_id IS NOT NULL
GROUP BY
	p.pollinator_sk, c.caste_sk, pl.plant_sk,
	h.habitat_sk, l.location_sk, d.date_sk
ON CONFLICT (pollinator_sk, caste_sk, plant_sk, habitat_sk, location_sk, date_sk)
DO UPDATE SET
	interaction_count = EXCLUDED.interaction_count,
	observation_count = EXCLUDED.observation_count,
	pollen_collected = EXCLUDED.pollen_collected,
	nectar_collected = EXCLUDED.nectar_collected,
	confirmed_pollination_count = EXCLUDED.confirmed_pollination_count;

--------------------
-- load into fact_user_location_monthly

INSERT INTO fact_user_location_monthly (
	user_sk,
	location_sk,
	date_sk,
	institution_sk,
	subscription_type_sk,
	observation_count,
	interaction_count
)
SELECT
	du.user_sk,
	l.location_sk,
	d.date_sk,
	inst.institution_sk,
	COALESCE(sub_active.subscription_type_sk, sub_earliest.subscription_type_sk) AS subscription_type_sk,
	COALESCE(COUNT(*), 0) AS observation_count,
	COALESCE(SUM(ob.interaction_count), 0) AS interaction_count
FROM staging.observations ob
JOIN staging.user_institution ui
	ON ui.user_institution_id = ob.user_institution_id
JOIN staging.users su
	ON su.user_id = ui.user_id
JOIN dim_user du
	ON du.user_id_bk = ui.user_id
JOIN dim_institution inst
	ON inst.user_id_bk = ui.user_id
		AND inst.institution_id_bk = ui.institution_id
LEFT JOIN dim_subscription_type sub_active
	ON sub_active.user_id_bk = ui.user_id
		AND sub_active.start_date <= ob.observation_date
		AND (sub_active.end_date IS NULL OR sub_active.end_date >= ob.observation_date)
LEFT JOIN dim_subscription_type sub_earliest
	ON sub_earliest.user_id_bk = ui.user_id
		AND sub_earliest.start_date = su.join_date
JOIN dim_location l
	ON l.location_id_bk = ob.location_id
JOIN dim_date d
	ON d.year  = EXTRACT(YEAR FROM ob.observation_date)::INT
		AND d.month = EXTRACT(MONTH FROM ob.observation_date)::INT
WHERE
	ob.observation_date IS NOT NULL
	AND ob.location_id IS NOT NULL
GROUP BY
	du.user_sk,
	l.location_sk,
	d.date_sk,
	inst.institution_sk,
	COALESCE(sub_active.subscription_type_sk, sub_earliest.subscription_type_sk)
ON CONFLICT ON CONSTRAINT fact_user_location_monthly_unique
DO UPDATE SET
	observation_count = EXCLUDED.observation_count,
	interaction_count = EXCLUDED.interaction_count;

-------------------------------
-------------------------------
-- load into fact_user_monthly_summary
INSERT INTO fact_user_monthly_summary (
	user_sk,
	date_sk,
	institution_sk,
	subscription_type_sk,
	observation_count,
	interaction_count,
	distinct_locations_count,
	distinct_habitats_count,
	distinct_pollinator_species_count,
	distinct_plant_species_count
)
SELECT
	du.user_sk,
	d.date_sk,
	inst.institution_sk,
	COALESCE(sub_active.subscription_type_sk, sub_earliest.subscription_type_sk) AS subscription_type_sk,
	COALESCE(COUNT(*), 0) AS observation_count,
	COALESCE(SUM(ob.interaction_count), 0) AS interaction_count,
	COALESCE(COUNT(DISTINCT ob.location_id), 0) AS distinct_locations_count,
	COALESCE(COUNT(DISTINCT ob.habitat_id), 0) AS distinct_habitats_count,
	COALESCE(COUNT(DISTINCT ob.pollinator_id), 0) AS distinct_pollinator_species_count,
	COALESCE(COUNT(DISTINCT ob.plant_id), 0) AS distinct_plant_species_count
FROM staging.observations ob
JOIN staging.user_institution ui
	ON ui.user_institution_id = ob.user_institution_id
JOIN staging.users su
	ON su.user_id = ui.user_id
JOIN dim_user du
	ON du.user_id_bk = ui.user_id
JOIN dim_institution inst
	ON inst.user_id_bk         = ui.user_id
		AND inst.institution_id_bk = ui.institution_id
LEFT JOIN dim_subscription_type sub_active
	ON sub_active.user_id_bk = ui.user_id
		AND sub_active.start_date <= ob.observation_date
		AND (sub_active.end_date IS NULL OR sub_active.end_date >= ob.observation_date)
LEFT JOIN dim_subscription_type sub_earliest
	ON sub_earliest.user_id_bk = ui.user_id
		AND sub_earliest.start_date = su.join_date
JOIN dim_date d
	ON d.year  = EXTRACT(YEAR FROM ob.observation_date)::INT
		AND d.month = EXTRACT(MONTH FROM ob.observation_date)::INT
WHERE
	ob.observation_date IS NOT NULL
	AND ob.location_id IS NOT NULL
GROUP BY
	du.user_sk,
	d.date_sk,
	inst.institution_sk,
	COALESCE(sub_active.subscription_type_sk, sub_earliest.subscription_type_sk)
ON CONFLICT ON CONSTRAINT fact_user_monthly_summary_unique
DO UPDATE SET
	observation_count = EXCLUDED.observation_count,
	interaction_count = EXCLUDED.interaction_count,
	distinct_locations_count = EXCLUDED.distinct_locations_count,
	distinct_habitats_count = EXCLUDED.distinct_habitats_count,
	distinct_pollinator_species_count = EXCLUDED.distinct_pollinator_species_count,
	distinct_plant_species_count = EXCLUDED.distinct_plant_species_count;
	

