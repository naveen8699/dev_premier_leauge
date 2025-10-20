create or replace view V_311_DAILY as
select h3_cell,
       date_trunc('day', created_ts) as dte,
       sum(iff(DESCRIPTOR ilike '%POTHOLE%',1,0)) as pothole_311_ct
from SV_311
group by 1,2;
