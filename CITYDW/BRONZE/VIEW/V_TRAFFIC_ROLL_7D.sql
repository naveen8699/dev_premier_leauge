create or replace view V_TRAFFIC_ROLL_7D as
select
  h3_cell,
  dte,
  avg(volume_veh_day) over (
    partition by h3_cell
    order by dte
    rows between 6 preceding and current row
  ) as volume_veh_7d_avg
from SV_TRAFFIC_DAILY_CELL;
