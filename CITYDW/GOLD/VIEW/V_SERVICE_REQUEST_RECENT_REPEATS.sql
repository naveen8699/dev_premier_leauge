create or replace view GOLD.V_SERVICE_REQUEST_RECENT_REPEATS as
select
  agency_name, complaint_type, descriptor, H3_CELL,
  count(*) as recent_similar_count
from SILVER.V_SERVICE_REQUEST_WITH_SEVERITY
where created_ts >= dateadd('day', -32, current_timestamp())
group by 1,2,3,4;
