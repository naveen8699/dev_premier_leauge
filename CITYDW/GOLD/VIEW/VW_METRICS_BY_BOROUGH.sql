create or replace view VW_METRICS_BY_BOROUGH as
select
  e.borough,
  avg(iff(predicted_label = actual_label, 1, 0)) as accuracy,
  sum(iff(predicted_label=1 and actual_label=1,1,0)) as tp,
  sum(iff(predicted_label=1 and actual_label=0,1,0)) as fp,
  sum(iff(predicted_label=0 and actual_label=1,1,0)) as fn,
  sum(iff(predicted_label=0 and actual_label=0,1,0)) as tn,
  case when (sum(iff(predicted_label=1,1,0)))=0 then null
       else sum(iff(predicted_label=1 and actual_label=1,1,0)) / sum(iff(predicted_label=1,1,0)) end as precision,
  case when sum(iff(actual_label=1,1,0))=0 then null
       else sum(iff(predicted_label=1 and actual_label=1,1,0)) / sum(iff(actual_label=1,1,0)) end as recall
from VW_POTHOLE_PREDICTIONS_ENRICHED e
group by 1;
