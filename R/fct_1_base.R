


# exported functions from rnoaa package -----------------------------------
# these are functions utilized by Joe Voye that were manually extracted from 
# the rnoaa R package
# Some may have been modified by Joe Voye

cpc_read <- function(path, us, drop_undefined) {
  conn <- if (grepl("\\.gz$", path)) gzfile(path, "rb") else file(path, "rb")
  on.exit(close(conn))
  
  if (us) {
    bites <- 120 * 300 * 2
    lats <- seq(from = 20.125, to = 49.875, by = 0.25)
    longs <- seq(from = 230.125, to = 304.875, by = 0.25)
  } else {
    bites <- 360 * 720 * 2
    lats <- seq(from = 0.25, to = 89.75, by = 0.5)
    lats <- c(rev(lats * -1), lats)
    longs <- seq(from = 0.25, to = 359.75, by = 0.5)
  }
  
  # read data
  tmp <- readBin(conn, numeric(), n = bites, size = 4, endian = "little")
  tmp <- tmp[seq_len(bites/2)] * 0.1
  
  # make data.frame
  df <- tibble::as_tibble(
    stats::setNames(
      cbind(expand.grid(longs, lats), tmp),
      c('lon', 'lat', 'precip')
    )
  )
  # remove undefined values
  if (drop_undefined) df <- subset(df, precip >= 0)
  return(df)
}


cpc_base_ftp <- function(us) {
  base <- "https://ftp.cpc.ncep.noaa.gov/precip/CPC_UNI_PRCP"
  if (us) file.path(base, "GAUGE_CONUS") else file.path(base, "GAUGE_GLB")
}

cpc_base_file <- function(us) {
  base <- "PRCP_CU_GAUGE_V1.0%sdeg.lnx."
  if (us) sprintf(base, "CONUS_0.25") else sprintf(base, "GLB_0.50")
}

cpc_key <- function(year, month, day, us, type = c('base', 'updated')){
  
  type <- rlang::arg_match(type, values = c('base', 'updated'))
  us <- checkmate::assert_logical(us)
  
  if (type == 'base'){
    rt_label <- 'RT'
  } else {
    rt_label <- 'UPDATED'
  }
  
  if (us) {
    rt_or_v1 <- if (year < 2007) "V1.0" else rt_label  
  } else {
    rt_or_v1 <- if (year < 2006) "V1.0" else rt_label
  }
  
  sprintf("%s/%s/%s/%s%s%s",
          cpc_base_ftp(us),
          rt_or_v1,
          year,
          cpc_base_file(us),
          paste0(year, month, day),
          if (year < 2006) {
            ".gz"
          } else if (year > 2005 && year < 2009) {
            if (us && year == 2006) {
              ".gz"
            } else if (!us && year == 2006) {
              glue::glue('{rt_label}.gz')
              # "UPDATED.gz"
            } else {
              glue::glue('{rt_label}.gz')
              # ".UPDATED.gz"
            }
          } else {
            # ".UPDATED"
            glue::glue('.{rt_label}')
          }
  )
}

cpc_get <- function(year, month, day, us, cache = FALSE, overwrite = TRUE, type = c('base', 'updated'), ...) {
  
  # validate input types
  type <- match.arg(type, choices = c('base', 'updated'))
  us <- checkmate::assert_logical(us)
  
  rnoaa::cpc_cache$mkdir()
  key <- cpc_key(year, month, day, us, type)
  file <- file.path(rnoaa::cpc_cache$cache_path_get(), basename(key))
  if (!file.exists(file)) {
    res <- suppressMessages(cpc_GET_write(sub("/$", "", key), file, overwrite, ...))
    file <- res$content
  } else {
    #cache_mssg(file)
  }
  return(file)
}

cpc_prcp <- function(date, us = FALSE, drop_undefined = FALSE, type = c('base', 'updated'), ...) {
  
  # validate input types
  date <- checkmate::assert_date(date)
  us <- checkmate::assert_logical(us)
  type <- match.arg(type, choices = c('base', 'updated'))
  stopifnot(length(us) == 1)
  
  dates <- stringr::str_extract_all(date, "[0-9]+")[[1]]
  if (us) {
    assert_range(dates[1], 1948:format(Sys.Date(), "%Y"))
  } else {
    assert_range(dates[1], 1979:format(Sys.Date(), "%Y"))
  }
  assert_range(as.numeric(dates[2]), 1:12)
  assert_range(as.numeric(dates[3]), 1:31)
  
  path <- cpc_get(year = dates[1], month = dates[2], day = dates[3],
                  us = us, type = type, ...)
  cpc_read(path, us, drop_undefined)
}

cpc_GET_write <- function(url, path, overwrite = TRUE, ...) {
  cli <- crul::HttpClient$new(url = url)
  if (!overwrite) {
    if (file.exists(path)) {
      stop("file exists and overwrite != TRUE", call. = FALSE)
    }
  }
  res <- tryCatch(cli$get(disk = path, ...), error = function(e) e)
  if (inherits(res, "error")) {
    unlink(path)
    stop(res$message, call. = FALSE)
  }
  return(res)
}

#' Get the updated data for a specific date
#'
#' @param dt datetime
#'
#' @return
#' @export
#' @author Hudson
fgetUpdated <- function(dt, type = c('base', 'updated')){
  
  type <- match.arg(type, choices = c('base', 'updated'))
  
  rainfall <- cpc_prcp(date = dt, us = TRUE, type = type) %>% 
    filter(precip > -99) %>%
    mutate(date = dt) 
  
  return(rainfall)
}

#' Get the updated data for a range of dates
#'
#' @param dtRange datetime range
#'
#' @return
#' @export
#' @author Hudson
fmonthlyPrecip <- function(dtRange, type = c('base', 'updated')){
  
  type <- match.arg(type, choices = c('base', 'updated'))
  
  dtMn <- lubridate::month(dtRange[1])
  dtYr <- lubridate::year(dtRange[1])
  
  tempPrecip <- lapply(dtRange, fgetUpdated, type = type)
  
  precipUpdated <- tempPrecip %>% 
    dplyr::bind_rows() %>%
    dplyr::mutate(precip = precip/1) %>% # this is the original code from Joe Voye. Convert from mm to inches using precip/25.4
    dplyr::group_by(lon, lat) %>%
    dplyr::summarise(
      'precip.mm' = sum(precip), # this is the original code from Joe Voye
      'maxDaily.mm' = max(precip), 
      .groups = 'keep'
    ) %>%
    dplyr::mutate(
      mn = dtMn,
      yr = dtYr
    )
  
  return(precipUpdated)
}


#' Create a vector a dates for a given month/year
#'
#' @param mnToGet 
#' @param yrToGet 
#'
#' @return
#' @export
#' @author Hudson, Lander
#' @examples
#' fdaysInMn(1, 2020)
fdaysInMn <- function(mnToGet, yrToGet) {
  
  startDt <- lubridate::mdy(paste(mnToGet, 1, yrToGet, sep = "/"))
  endDt <- lubridate::ceiling_date(startDt, unit = "month") - lubridate::days(1)
  
  dtRange <- seq(startDt, endDt, by = "days")
  
  return(dtRange)
}

#' Create a list of vectors where each vector is a month of dates 
#'
#' @param yrToGet 
#'
#' @return
#' @export
#' @author Hudson, Lander
#'
#' @examples
#' fdtRanges(2020)
fdtRanges <- function(yrToGet) {
  
  dtRanges <- purrr::pmap(list(1:12, yrToGet), fdaysInMn)
  
  return(dtRanges)
}


# pipeline functions ------------------------------------------------------

#' Get the last date of the stored data
#'
#' @param con Database connection
#'
#' @return
#' @export
get_last_modified_rainfall <- function(conn){
  
  use_db <- isTRUE(config::get('use_db'))
  
  if(use_db){
    # get db info
    table_names <- config::get('db_tables')
    schema <- table_names$schema
    table_name <- table_names$rainfall #CPC1948_Forward_mm
    
    # bring sql table into memory
    current_table <- conn |> 
      dplyr::tbl(dbplyr::in_schema(schema, table_name)) |> 
      dplyr::collect()
    
    # for table initialization, use arbitrary old date
    if (!is_truthy(current_table)) {
      last_modified_date <- as.Date('1900-01-01')
    } else {
      last_modified_date <- current_table |> 
        dplyr::pull(date.downloaded) |>  
        max() |> 
        as.Date()
    }
    
  } else {
    last_modified_date <- Sys.time() - lubridate::days(10) #as.Date('2022-07-17') # for testing
  }
  
  return(last_modified_date)
}

#' Check if the base rainfall data is available
#'
#' Raises error if data is not available.
#'
#' @param year 
#' @param last_modified_date_local date of latest local data to check remote data against
#' @param timeout timeout for cURL request in seconds 
#'
#' @return logical
#' @export
#' @author Joe Marlo (Lander Analytics)
#' 
#' @examples
#' check_for_update_rainfall_base()
check_for_update_rainfall_base <- function(year, last_modified_date_local, timeout = 120){
  
  # get ftp link
  base_ftp <- cpc_base_ftp(us = TRUE)
  base_ftp <- file.path(base_ftp, 'RT', year)
  
  # use httr and rvest to get the html table
  doc_table <- base_ftp %>% 
    httr::GET(httr::timeout(timeout)) %>%
    httr::content() %>% 
    rvest::html_table() %>% 
    dplyr::first()
  last_modified_date <- doc_table %>% 
    mutate(last_modified = lubridate::dmy_hm(`Last modified`)) %>% 
    na.omit() %>% 
    pull(last_modified) %>% 
    max()
  
  # check if max value in the table is greater than last modified date on file
  is_updated <- last_modified_date > last_modified_date_local
  
  # is_updated <- TRUE
  if (!isTRUE(is_updated)) cli::cli_abort('Stopping: Rainfall data not yet available')
  return(is_updated)
}

#' Get the table that maps grid_id to lat, lon
#'
#' @param con 
#'
#' @return
#' @export
#' @author Joe Marlo (Lander Analytics)
#'
#' @examples
read_grid_mapping <- function(conn){
  
  
  use_db <- isTRUE(config::get('use_db'))
  
  if(use_db){
    # get db info
    table_names <- config::get('db_tables')
    schema <- table_names$schema
    table_name <- table_names$rain_grids #'RainData_Grids'
    
    # write to db table
    mapping <- conn |> 
      dplyr::tbl(
        dbplyr::in_schema(
          schema,
          table_name
        )
      ) |> 
      dplyr::transmute(
        lat = Lat, 
        lon = Long + 360, # per original code
        grid_id = Grid
      ) |>
      dplyr::collect()

  } else {
    # read from csv
    mapping <- readr::read_csv('inputs/RainData_Grids.csv')
  }

  if (!isTRUE(inherits(mapping, 'tbl'))) cli::cli_abort('read_grid_mapping() should return a tbl')
  return(mapping)
}

#' Create a list of dates
#'
#' @param yr year
#' @param mt month
#'
#' @return list
#'
#' @examples
#' create_dates(2023, 03)
create_dates <- function(yr, mt){

  dates <- unlist(lapply(yr, fdtRanges), recursive = FALSE)
  dates_filtered <- dates[1:mt]

  return(dates_filtered)
}

#' Download and format the gridded rainfall data
#'
#' @param dt_ranges 
#'
#' @return
#' @export
#' @author Joe Marlo (Lander Analytics)
#'
#' @examples
download_gridded_rainfall_base <- function(dt_ranges, type = 'base'){
  precip_by_cord <- purrr::map_dfr(dt_ranges, fmonthlyPrecip, type = type)
  
  # add column when last update was made
  precip_by_cord$date.downloaded <- Sys.time()
  
  # add marker for update type
  # precip_by_cord$type <- 'base'
  
  if (!isTRUE(inherits(precip_by_cord, 'tbl'))) cli::cli_abort('download_gridded_rainfall_base() should return a tbl')
  return(precip_by_cord)
}

#' Join the gridded rainfall data to the database data
#'
#' @param precip_by_cord 
#' @param tbl_grids_latlon 
#' @param month 
#'
#' @return data.frame
#' @export
#' @author Joe Marlo (Lander Analytics)
#'
#' @examples
join_data_rainfall_base <- function(precip_by_cord, tbl_grids_latlon, month){
  precip_by_grid <- inner_join(precip_by_cord, tbl_grids_latlon, by = c('lat' , 'lon'))
  precip_by_grid_filt <- dplyr::filter(precip_by_grid, mn >= month)
  
  if (!isTRUE(inherits(precip_by_grid_filt, 'tbl'))) cli::cli_abort('join_data_rainfall_base() should return a tbl')
  return(precip_by_grid_filt)
}


#' Add metrics to the data
#'
#' Deprecated
#'
#' @param .data 
#'
#' @return data.frame
#' @export
#' @author Joe Marlo (Lander Analytics)
#'
#' @examples
add_metrics_base <- function(.data){
  # deprecated
  # add column with the date of the max daily rainfall for the base data
  # add column with the max daily rainfall amount for the base data
  # add column with the number of days per month where precip > 0
  # some of this is already handled in fmonthlyPrecip
  
  return(.data)
}

check_for_duplicates <- function(.data, conn){
  
  use_db <- isTRUE(config::get('use_db'))
  
  if (use_db){
    # get db info
    table_names <- config::get('db_tables')
    schema <- table_names$schema
    table_name <- table_names$rainfall #'CPC1948_Forward_mm'
    
    # is this monthly data already in the database?
    # if so, then stop here
    cols_to_check <- c('yr', 'mn', 'lon', 'lat')
    current_table_latest <- conn |> 
      dplyr::tbl(dbplyr::in_schema(schema, table_name)) |> 
      dplyr::arrange(desc(yr), desc(mn)) |> 
      head(1e6) |> 
      dplyr::collect() |> 
      dplyr::select(tidyselect::all_of(cols_to_check))
    
    n_prec_digits <- 2
    .data |> 
      dplyr::select(tidyselect::all_of(cols_to_check)) |> 
      dplyr::mutate(lat = round(lat, n_prec_digits),
                    lon = round(lon, n_prec_digits)) |> 
      dplyr::anti_join(current_table_latest |> dplyr::mutate(lat = round(lat, n_prec_digits), lon = round(lon, n_prec_digits)), 
                       by = cols_to_check)
    
    return(TRUE)
    
  } else {
    return(TRUE) 
  }
  
}

#' Append the new data into the database table or write to csv
#'
#' Destination is controlled by config::get('use_db')
#'
#' @param precip_by_grid_filt 
#' @param conn database connection
#'
#' @return string of the output location
#' @export
#' @author Joe Marlo (Lander Analytics)
upload_gridded_rainfall_base <- function(precip_by_grid_filt, conn){
  
  use_db <- isTRUE(config::get('use_db'))
  
  if(use_db){
    # get db info
    table_names <- config::get('db_tables')
    schema <- table_names$schema
    table_name <- table_names$rainfall #'CPC1948_Forward_mm'
    
    # write to db table
    DBI::dbAppendTable(
      conn, 
      DBI::Id(
        schema = schema,
        table = table_name
      ),
      precip_by_grid_filt
    )
    
    return(NULL)
  } else {
    # write to csv
    file_out <- file.path(config::get('output'), 'rainfall_base.csv')
    readr::write_csv(precip_by_grid_filt, file_out)
    return(file_out)
  }
}
