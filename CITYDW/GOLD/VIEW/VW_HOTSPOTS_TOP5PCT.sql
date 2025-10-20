create or replace view VW_HOTSPOTS_TOP5PCT as
select *
from (
  select
    asof_day, h3_cell, borough, probability, actual_label,
    ntile(20) over (partition by asof_day order by probability desc) as vintile
  from VW_POTHOLE_PREDICTIONS_ENRICHED
)
where vintile = 1;
