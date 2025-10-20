create or replace view V_CELL_NEIGHBORS as
select h3_cell, value::string as nbh_cell
from CITYDW.SILVER.SV_ACTIVE_CELLS,
     lateral flatten(input => h3_grid_disk(h3_cell, 1));
