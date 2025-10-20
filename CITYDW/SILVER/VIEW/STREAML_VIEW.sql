create OR REPLACE view streaml_view as WITH hourly_avg AS (
    SELECT
        BORO,
        STREET,
        HH AS HOUR,
        AVG(VOL) AS AVG_VOLUME
    FROM SMART_CITY.TRAFFIC.TRAFFIC_VOLUME
    GROUP BY BORO, STREET, HH
),

ranked AS (
    SELECT
        BORO,
        STREET,
        HOUR,
        AVG_VOLUME,
        RANK() OVER (PARTITION BY BORO, STREET ORDER BY AVG_VOLUME DESC) AS VOL_RANK
    FROM hourly_avg
),

-- Keep only top 5 hours per street
top5_hours AS (
    SELECT *
    FROM ranked
    WHERE VOL_RANK <= 5
),

-- Compute average of top 5 hours per street
street_top5_avg AS (
    SELECT
        BORO,
        STREET,
        AVG(AVG_VOLUME) AS AVG_TOP5_VOLUME
    FROM top5_hours
    GROUP BY BORO, STREET
),

-- Get the hour with max volume among top 5 for each street
peak_hour AS (
    SELECT
        BORO,
        STREET,
        HOUR AS PEAK_HOUR,
        AVG_VOLUME
    FROM top5_hours
    QUALIFY ROW_NUMBER() OVER (PARTITION BY BORO, STREET ORDER BY AVG_VOLUME DESC) = 1
),

-- Get the hour with min volume among top 5 for each street
low_hour AS (
    SELECT
        BORO,
        STREET,
        HOUR AS LOW_HOUR,
        AVG_VOLUME
    FROM top5_hours
    QUALIFY ROW_NUMBER() OVER (PARTITION BY BORO, STREET ORDER BY AVG_VOLUME ASC) = 1
),

-- Rank streets per borough to find max/min based on average of top 5
street_ranked AS (
    SELECT
        s.BORO,
        s.STREET,
        s.AVG_TOP5_VOLUME,
        p.PEAK_HOUR,
        l.LOW_HOUR,
        ROW_NUMBER() OVER (PARTITION BY s.BORO ORDER BY s.AVG_TOP5_VOLUME DESC) AS MAX_RANK,
        ROW_NUMBER() OVER (PARTITION BY s.BORO ORDER BY s.AVG_TOP5_VOLUME ASC) AS MIN_RANK
    FROM street_top5_avg s
    JOIN peak_hour p ON s.BORO = p.BORO AND s.STREET = p.STREET
    JOIN low_hour l ON s.BORO = l.BORO AND s.STREET = l.STREET
)

-- Final result: top & bottom street per borough with hours
SELECT
    BORO,
    STREET,
    AVG_TOP5_VOLUME AS AVG_VOLUME ,
    CASE 
        WHEN MAX_RANK = 1 THEN 'Max Volume'
        WHEN MIN_RANK = 1 THEN 'Min Volume'
    END AS VOLUME_TYPE,
    PEAK_HOUR AS PEAK_HOUR,
    LOW_HOUR AS LOW_HOUR
FROM street_ranked
WHERE MAX_RANK = 1 OR MIN_RANK = 1
ORDER BY BORO, VOLUME_TYPE DESC;
