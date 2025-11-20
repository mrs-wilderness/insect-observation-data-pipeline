--------------
--QUERIES TO BE RUN ON THE OLTP DATABASE
--------------

--top 10 most common plant–pollinator interaction combination
SELECT 
	ps.species_name AS plant,
	po.species_name AS pollinator,
	COUNT(*) AS interaction_count
FROM observations o
JOIN plant_species ps ON o.plant_id = ps.plant_id
JOIN pollinator_species po ON o.pollinator_id = po.pollinator_id
GROUP BY plant, pollinator
ORDER BY interaction_count DESC
LIMIT 10;

--most observed habitats
SELECT 
	h.habitat_name,
	COUNT(*) AS total_observations
FROM observations o
JOIN habitats h ON o.habitat_id = h.habitat_id
GROUP BY h.habitat_name
ORDER BY total_observations DESC;

-- summary by pollination quality type
SELECT 
	o.pollination_quality_id,
	pq.description,
	COUNT(*) FILTER (WHERE pollen_collected) AS with_pollen,
	COUNT(*) FILTER (WHERE nectar_collected) AS with_nectar,
	COUNT(*) AS total
FROM observations AS o
LEFT JOIN pollination_qualities AS pq
USING(pollination_quality_id)
GROUP BY o.pollination_quality_id, pq.description
ORDER BY o.pollination_quality_id;

--observation count by user
SELECT 
	u.last_name,
	COUNT(*) AS total_observations
FROM observations o
JOIN user_institution ui ON o.user_institution_id = ui.user_institution_id
JOIN users u ON ui.user_id = u.user_id
GROUP BY u.last_name
ORDER BY total_observations DESC
LIMIT 10;