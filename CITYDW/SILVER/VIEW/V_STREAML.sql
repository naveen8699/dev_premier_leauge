create view v_streaml as WITH q AS (
    SELECT
        BORO,
        STREET,
        HH,
        AVG(VOL) AS avg_volume_street_vise
    FROM SMART_CITY.TRAFFIC.TRAFFIC_VOLUME
    GROUP BY BORO, STREET, HH
),
rush_hour_data AS (
    SELECT
        *,
        CASE
            WHEN HH BETWEEN 7 AND 13 THEN 'Morning Rush'
            WHEN HH BETWEEN 14 AND 18 THEN 'Evening Rush'
            ELSE 'Non-Rush'
        END AS rush_period
    FROM q
), 
avg_boro_street as (SELECT
    BORO,
    STREET,
    rush_period,
    AVG(avg_volume_street_vise) AS avg_rush_volume,        1 AS load_id
FROM rush_hour_data
WHERE rush_period != 'Non-Rush'
GROUP BY BORO, STREET, rush_period
ORDER BY BORO, STREET, rush_period)

SELECT
        BORO,
        rush_period,
        AVG(avg_rush_volume) AS avg_rush_volume_per_boro
    FROM avg_boro_street
    GROUP BY BORO, rush_period;
