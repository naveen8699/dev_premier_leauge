create or replace view GOLD.V_SERVICE_REQUEST_PRIORITY_QUEUE as
with base as (
  select
    v.*,
    coalesce(boro.p50_close_hours,
             d.p50_close_hours,
             t.p50_close_hours,
             a.p50_close_hours,
             48) as target_hours,
    coalesce(rep.recent_similar_count, 0) as recent_similar_count
  from SILVER.V_SERVICE_REQUEST_WITH_SEVERITY v
  left join GOLD.SERVICE_REQUEST_SLA_BORO boro
    on v.agency_name=boro.agency_name and v.complaint_type=boro.complaint_type
   and v.descriptor=boro.descriptor and v.borough=boro.borough
  left join GOLD.SERVICE_REQUEST_SLA_DESC d
    on v.agency_name=d.agency_name and v.complaint_type=d.complaint_type
   and v.descriptor=d.descriptor
  left join GOLD.SERVICE_REQUEST_SLA_TYPE t
    on v.agency_name=t.agency_name and v.complaint_type=t.complaint_type
  left join GOLD.SERVICE_REQUEST_SLA_AGENCY a
    on v.agency_name=a.agency_name
  left join GOLD.V_SERVICE_REQUEST_RECENT_REPEATS rep
    on v.agency_name=rep.agency_name
   and v.complaint_type=rep.complaint_type
   and v.descriptor=rep.descriptor
   and v.H3_CELL=rep.H3_CELL
  where v.is_open=1
),
scored as (
  select
    *,
    greatest(1, target_hours) as safe_target_hours,
    (age_hours / nullif(target_hours,0)) as breach_risk_raw,
    severity as sev
  from base
),
final as (
  select
    *,
    -- Composite score (explainable)
    ( 0.6 * least(3.0, coalesce(breach_risk_raw,0))
    + 0.3 * (coalesce(sev,3) / 5.0)
    + 0.1 * least(1.0, recent_similar_count / 5.0) ) as priority_score,
    case
      when (age_hours >= safe_target_hours) or coalesce(sev,3) >= 5 then 'P1'
      when (age_hours / safe_target_hours) > 0.75 then 'P2'
      when (age_hours / safe_target_hours) > 0.5  then 'P3'
      else 'P4'
    end as priority_bucket,
    coalesce(due_ts, dateadd('hour', safe_target_hours, created_ts)) as inferred_due_ts
  from scored
)
select
  agency_name, complaint_type, descriptor, borough,
  unique_key, created_ts, status,
  age_hours, safe_target_hours as target_hours,
  round(breach_risk_raw,3) as breach_risk,
  sev as severity,
  recent_similar_count,
  priority_bucket,
  round(priority_score,3) as priority_score,
  inferred_due_ts,
  H3_CELL, latitude, longitude
from final
order by agency_name, priority_score desc, age_hours desc;
