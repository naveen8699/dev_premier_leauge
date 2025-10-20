create or replace view V_BASELINE_SCORE as
select
  h3_cell, asof_day,
  30*pothole_7d + 10*pothole_30d + 5*nbh_pothole_30d + 2*freeze_thaw_14d + 0.5*prcp_7d as baseline_score,
  label_14d
from FV_POTHOLE_CELL;
