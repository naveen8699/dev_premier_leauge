create or replace view V_311_POTHOLE_DAILY as
select h3_cell, date_trunc('day', created_ts) as dte, count(*) as pothole_ct
from CITYDW.SILVER.SV_311
group by 1,2;
