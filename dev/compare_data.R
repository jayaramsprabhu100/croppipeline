### scratch code for comparing data on the database ###

library(dplyr)
library(dbplyr)
source('R/data.R')

conn <- connect_to_db()

# data comparison
rainfall_old <- conn |>
  tbl(in_schema('dbo', 'CPC1948_Forward_mm')) |>
  collect()

rainfall_new <- conn |> 
  tbl(in_schema('dbo', 'crop_auto_rainfall')) |> 
  collect()

precision <- 4

rainfall_old |> 
  filter(yr == 2023,
         mn == 3) |>
  # arrange(lon, lat)
  # anti_join(rainfall_new |> mutate(grid_id = as.integer(grid_id)))
  full_join(
    rainfall_new |> mutate(grid_id = as.integer(grid_id)),
    by = c('lat', 'lon', 'mn', 'yr', 'grid_id')
  ) %>%
  dplyr::select(sort(colnames(.))) |> 
  mutate(all_equal = (round(maxDaily.mm.x, precision) == round(maxDaily.mm.y, precision)) 
                      & 
                      (round(precip.mm.x, precision) == round(precip.mm.y, precision))) |> 
  filter(!all_equal)

  
# note: precision for precip and maxDaily is not perfect
# Q: how precise does it need to be? Actuaries say current precision is good
# RMA only uses three decimal points 

# for a given row, should be wide data frame, one col for base and one col for updated
# move all the old data from CPC1948_Forward_mm to the new table
