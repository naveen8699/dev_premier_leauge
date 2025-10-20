create or replace view VW_METRICS_DAILY as
select
  asof_day::date as asof_date,
  avg(iff(predicted_label = actual_label, 1, 0)) as accuracy,
  sum(iff(predicted_label=1 and actual_label=1,1,0)) as tp,
  sum(iff(predicted_label=1 and actual_label=0,1,0)) as fp,
  sum(iff(predicted_label=0 and actual_label=1,1,0)) as fn,
  sum(iff(predicted_label=0 and actual_label=0,1,0)) as tn
from POTHOLE_PREDICTIONS
group by 1
order by 1;
