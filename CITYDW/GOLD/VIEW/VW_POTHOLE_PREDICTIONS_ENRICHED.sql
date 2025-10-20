create or replace view VW_POTHOLE_PREDICTIONS_ENRICHED as
select
  p.asof_day,
  p.h3_cell,
  b.borough,
  p.actual_label,
  p.predicted_label,
  p.probability,
  /* centroids for map pins */
  st_y(st_centroid(to_geography(h3_cell_to_boundary(p.h3_cell)))) as lat,
  st_x(st_centroid(to_geography(h3_cell_to_boundary(p.h3_cell)))) as lon
from POTHOLE_PREDICTIONS p
left join CITYDW.SILVER.SV_H3_BOROUGH b
  on b.h3_cell = p.h3_cell;
