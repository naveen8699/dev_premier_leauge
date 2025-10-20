create or replace view FV_POTHOLE_CELL as
with cell_hist as (
  select b.h3_cell, b.asof_day,
    (select coalesce(sum(pothole_ct),0)
       from V_311_POTHOLE_DAILY d
      where d.h3_cell=b.h3_cell and d.dte between b.asof_day - interval '7 day' and b.asof_day)   as pothole_7d,
    (select coalesce(sum(pothole_ct),0)
       from V_311_POTHOLE_DAILY d
      where d.h3_cell=b.h3_cell and d.dte between b.asof_day - interval '30 day' and b.asof_day)  as pothole_30d
  from LB_BASE b
),
nbh_hist as (
  select b.h3_cell, b.asof_day,
    coalesce((
      select sum(d.pothole_ct)
      from V_CELL_NEIGHBORS n
      join V_311_POTHOLE_DAILY d on d.h3_cell = n.nbh_cell
      where n.h3_cell = b.h3_cell
        and d.dte between b.asof_day - interval '30 day' and b.asof_day
    ),0) as nbh_pothole_30d
  from LB_BASE b
),
traffic_7d as (
  select
    h3_cell,
    dte as asof_day,
    volume_veh_7d_avg
  from CITYDW.SILVER.V_TRAFFIC_ROLL_7D
)
select
  l.h3_cell,
  l.asof_day,
  ch.pothole_7d,
  ch.pothole_30d,
  nh.nbh_pothole_30d,
  wx.freeze_thaw_14d,
  wx.prcp_7d,
  wx.month_num,
  pav.pavement_rating_avg,
  tr.volume_veh_7d_avg,     -- <-- 7-day rolling average of daily sums
  y.label_14d
from LB_BASE l
left join cell_hist ch on ch.h3_cell=l.h3_cell and ch.asof_day=l.asof_day
left join nbh_hist nh  on nh.h3_cell=l.h3_cell and nh.asof_day=l.asof_day
left join V_WX_ROLL wx on wx.dte=l.asof_day
left join V_PAVEMENT_CELL_ASOF pav on pav.h3_cell=l.h3_cell and pav.asof_day=l.asof_day
left join traffic_7d tr on tr.h3_cell=l.h3_cell and tr.asof_day=l.asof_day
left join LB_POTHOLE_14D y on y.h3_cell=l.h3_cell and y.asof_day=l.asof_day;
