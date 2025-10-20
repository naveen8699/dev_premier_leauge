create or replace view VW_PR_CURVE as
select
  th,
  case when sum(iff(probability>=th,1,0))=0 then null
       else 100.0*sum(iff(probability>=th and actual_label=1,1,0))/sum(iff(probability>=th,1,0)) end  as precision_pct,
  case when sum(iff(actual_label=1,1,0))=0 then null
       else 100.0*sum(iff(probability>=th and actual_label=1,1,0))/sum(iff(actual_label=1,1,0)) end    as recall_pct
from POTHOLE_PREDICTIONS, TMP_THRESHOLDS
group by th
order by th;
