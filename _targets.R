# See also
#   https://books.ropensci.org/targets/ # nolint

# Load packages required to define the pipeline:
library(targets)
# library(tarchetypes) 

# set target options
tar_option_set(
  packages = c("tibble", 'dplyr', 'dbplyr'), # packages that your targets need to run
  format = "rds" 
)

# load the R scripts with custom functions
lapply(list.files("R", full.names = TRUE, recursive = TRUE), source)

# useful targets commands
# targets::tar_visnetwork()
# targets::tar_make()
# targets::tar_meta(fields = warnings)


# pipeline ----------------------------------------------------------------

pipeline <- list()

# goal: look at the NOAA FTP site to see if it has been updated, pull down data and write to table
# timing: run hourly from 10am-2pm and check if time on ftp server is updated
pipeline$rainfall <- list(
  
  ### set environment variables
  tarchetypes::tar_force(
    name = RF_month,
    command = Sys.getenv('month'),
    force = TRUE
  ),
  tarchetypes::tar_force(
    name = RF_year,
    command = Sys.getenv('year'),
    force = TRUE
  ),
  tarchetypes::tar_force(
    name = RF_clear_cache,
    command = rnoaa::cpc_cache$delete_all(),
    force = TRUE
  ),
  
  ### check that the data is available
  # stop if not available yet
  tar_target(
    name = RF_last_modified_local,
    command = get_last_modified_rainfall(conn),
  ),
  tar_target(
    name = RF_check_for_update,
    command = check_for_update_rainfall_base(RF_year, RF_last_modified_local)
  ),
  
  ### download, join, and write out the data
  tar_target(
    name = RF_grid_mapping,
    command = read_grid_mapping(conn)
  ),
  tar_target(
    name = RF_dt_ranges,
    command = create_dates(RF_year, RF_month)
  ),
  tar_target(
    name = RF_new_data,
    command = download_gridded_rainfall_base(RF_dt_ranges)
  ),
  tar_target(
    name = RF_check_if_duplicate,
    command = check_for_duplicates(RF_new_data, conn)
  ),
  tar_target(
    name = RF_joined_data,
    command = join_data_rainfall_base(RF_new_data, RF_grid_mapping, RF_month)
  ),
  tar_target(
    name = RF_upload_data,
    command = upload_gridded_rainfall_base(RF_joined_data, conn),
    format = 'file'
  )
)

# goal: update process looks at different FTP folder, if updated then overwrite rainfall table
# timing: run once daily on the 12th-31st once a month, 2pm+ CDT
pipeline$rainfall_update <- list(
  ### set environment variables
  tar_target(
    name = RFU_month,
    command = Sys.getenv('month')
  ),
  tar_target(
    name = RFU_year,
    command = Sys.getenv('year')
  ),
  tarchetypes::tar_force(
    name = RFU_clear_cache,
    command = rnoaa::cpc_cache$delete_all(),
    force = TRUE
  ),
  
  ### check that the data is available
  tar_target(
    name = RFU_last_modified_local,
    command = get_last_modified_rainfall(conn),
  ),
  tar_target(
    name = RFU_check_for_update,
    command = check_for_update_rainfall_update(RFU_year, RFU_last_modified_local)
  ),
  
  ### download, join, and write out the data
  # download new updated data
  # get old data from database
  # merge the data
  # upload data to the same table in the db
  
  tar_target(
    name = RFU_new_data,
    command = download_updated_rainfall_update(RFU_year, RFU_month)
  ),
  tar_target(
    name = RFU_upload_data,
    command = upload_gridded_rainfall_updated(RFU_new_data, conn),
    format = 'file'
  )
)

# goal: pull down summary of business data
# run one year as a time
pipeline$SOB <- list(
  ### set environment variables
  tarchetypes::tar_force(
    name = SOB_year,
    command = Sys.getenv('year'),
    force = TRUE
  ),
  tarchetypes::tar_force(
    name = SOB_download_n_try,
    command = Sys.getenv('n_try'),
    force = TRUE
  ),
  tarchetypes::tar_force(
    name = SOB_download_timeout,
    command = Sys.getenv('timeout'),
    force = TRUE
  ),
  
  ### stop here if data has already been updated
  tar_target(
    name = SOB_is_updated,
    command = SOB_check_if_updated(conn)
  ),
  
  ### download and write out
  tar_target(
    name = SOB_download_data,
    command = SOB_download(SOB_year, n_try = SOB_download_n_try, timeout = SOB_download_timeout)
  ),
  # tar_target(
  #   name = SOB_cleaned_data,
  #   command = SOB_drop_obsolete() # deprecated per conversations
  # ),
  tar_target(
    name = SOB_upload_data,
    command = SOB_write(SOB_download_data, conn)
  )
)

# goal: pull down cause of loss data
# timing: run with summary of business
pipeline$COL <- list(
  ### set environment variables
  tarchetypes::tar_force(
    name = COL_year,
    command = Sys.getenv('year'),
    force = TRUE
  ),
  tarchetypes::tar_force(
    name = COL_download_timeout,
    command = Sys.getenv('timeout'),
    force = TRUE
  ),
  
  # download and write out
  tar_target(
    name = COL_download_data,
    command = COL_download(COL_year, timeout = COL_download_timeout)
  ),
  tar_target(
    name = COL_upload_data,
    command = COL_write(COL_download_data, conn)
  )
)

# return the pipeline
# this must be the last item in the script
pipeline |> 
  # this allows us to put all the database connections here instead of in each target
  tarchetypes::tar_hook_before(
    {
      use_db <- isTRUE(config::get('use_db'))
      if (use_db){
        conn <- connect_to_db()
        on.exit(DBI::dbDisconnect(conn))
      } else {
        conn <- NULL
      }
    },
    # each of these targets needs a connection to the database
    names = c(
      RF_last_modified_local, 
      RF_grid_mapping, 
      RF_upload_data, 
      RFU_last_modified_local, 
      RFU_grid_mapping, 
      SOB_is_updated, 
      SOB_upload_data,
      COL_upload_data
    )
  )
