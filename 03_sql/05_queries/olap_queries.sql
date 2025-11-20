----------------
--THESE QUERIES ARE MEANT TO BE RUN ON THE OLAP DATABASE
----------------

--top-5 months with the highest total confirmed pollinations
--January excluded as it's a placeholder value
SELECT 
	d.month_name,
	SUM(fp.confirmed_pollination_count) AS total_confirmed
FROM fact_pollination_activity fp
JOIN dim_date d
	ON fp.date_sk = d.date_sk
WHERE d.month_name <> 'January'
GROUP BY d.month_name
ORDER BY total_confirmed DESC
LIMIT 5;

--top 10 locations and months based on observation count
SELECT 
	l.latitude, 
	l.longitude, 
	d.month_year,
	SUM(fp.observation_count) AS total_observations
FROM fact_pollination_activity fp
JOIN dim_location l 
	ON fp.location_sk = l.location_sk
JOIN dim_date d     
	ON fp.date_sk = d.date_sk
GROUP BY 
	l.latitude, 
	l.longitude, 
	d.month_year
ORDER BY 
	total_observations DESC
LIMIT 10;

--top monthly unique locations per user
WITH user_monthly AS (
	SELECT
		ful.user_sk,
		du.username,
		ful.date_sk,
		d.month_year,
		COUNT(DISTINCT ful.location_sk) AS distinct_location_count
	FROM fact_user_location_monthly ful
	JOIN dim_user du ON ful.user_sk = du.user_sk
	JOIN dim_date d ON ful.date_sk = d.date_sk
	GROUP BY ful.user_sk, du.username, ful.date_sk, d.month_year
)
, ranked_monthly AS (
	SELECT
		um.user_sk,
		um.username,
		um.month_year,
		um.distinct_location_count,
	ROW_NUMBER() OVER (
	  PARTITION BY um.user_sk
	  ORDER BY um.distinct_location_count DESC
	) AS rn
	FROM user_monthly um
)
SELECT
	rm.username,
	rm.month_year,
	rm.distinct_location_count
FROM ranked_monthly rm
WHERE rm.rn = 1
ORDER BY rm.distinct_location_count DESC
LIMIT 10;


