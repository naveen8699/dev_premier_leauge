create or replace view V_WX_ROLL as
select
  dte,
  sum(iff(tmin_c<=0 and tmax_c>0,1,0)) over (order by dte rows between 13 preceding and current row) as freeze_thaw_14d,
  sum(prcp_mm) over (order by dte rows between 6 preceding and current row) as prcp_7d,
  month(dte) as month_num
from CITYDW.SILVER.SV_WEATHER_DAILY;
