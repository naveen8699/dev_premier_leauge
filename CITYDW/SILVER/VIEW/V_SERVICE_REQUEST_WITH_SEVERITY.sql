create or replace view CITYDW.SILVER.V_SERVICE_REQUEST_WITH_SEVERITY as
with rules as (
  select * from SILVER.SERVICE_REQUEST_SEVERITY_RULES
)
select
  v.UNIQUE_KEY, v.created_ts, v.closed_ts, v.due_ts, v.resolution_action_updated_ts,
    v.agency_code, v.agency_name, v.complaint_type, v.descriptor, v.borough, v.status, v.is_open,
    v.H3_CELL, v.LATITUDE, v.LONGITUDE,
 v.age_hours,
  coalesce(r.severity, 3) as severity
from SILVER.V_SERVICE_REQUEST_INFRA v 
left join rules r
  on (r.pattern_agency is null or regexp_like(coalesce(v.agency_name,''),    r.pattern_agency, 'i'))
 and (r.pattern_type   is null or regexp_like(coalesce(v.complaint_type,''), r.pattern_type,   'i'))
 and (r.pattern_desc   is null or regexp_like(coalesce(v.descriptor,''),     r.pattern_desc,   'i'))
qualify row_number() over (
  partition by v.unique_key            -- use your row's natural key here
  order by coalesce(r.severity, 3) desc
) = 1;
