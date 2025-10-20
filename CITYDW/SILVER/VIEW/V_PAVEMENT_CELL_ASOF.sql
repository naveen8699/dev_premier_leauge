create or replace view V_PAVEMENT_CELL_ASOF as (
select
  l.h3_cell,
  l.asof_day,
  pavement_rating_avg
from (
  select
    b.h3_cell,
    b.asof_day,
    p.inspection_date,
    p.pavement_rating_avg,
    row_number() over (partition by b.h3_cell, b.asof_day order by p.inspection_date desc) as rn
  from LB_BASE b
  left join CITYDW.SILVER.SV_PAVEMENT_CELL_BY_INSPECTION p
    on p.h3_cell = b.h3_cell
   and p.inspection_date <= b.asof_day 
) l
where l.rn = 1) ;
