create or replace view VW_HOTSPOTS_TH070 as
select *
from VW_POTHOLE_PREDICTIONS_ENRICHED
where probability >= 0.70;
