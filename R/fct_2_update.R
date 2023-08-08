
#' Get the last date of the stored data
#'
#' @param con 
#'
#' @return
#' @export
#'
#' @examples
get_last_modified <- function(conn){

  use_db <- isTRUE(config::get('use_db'))

  if(use_db){
    # get db info
    table_names <- config::get('db_tables')
    schema <- table_names$schema
    table_name <- table_names$rainfall #CPC1948_Forward_mm

    last_modified_date <- conn |>
      dplyr::tbl(dbplyr::in_schema(schema, table_name)) |>
      dplyr::pull(date.downloaded) |> 
      dplyr::max() |>
      dplyr::collect()
  } else {
    last_modified_date <- Sys.time() - lubridate::days(10) # this is dummy code for testing
  }

  return(last_modified_date)
}

#' Check to see if new UPDATE data is available on the FTP server
#'
#' @param year the year to check
#' @param last_modified_date_local the date of the current data
#'
#' @return
#' @export logical
#' @author Joe Marlo (Lander Analytics)
#'
#' @examples
check_for_update_rainfall_update <- function(year, last_modified_date_local){
  
  year <- checkmate::assert_character(year)
  last_modified_date_local <- checkmate::assert_date(last_modified_date_local)
  
  # get ftp link
  base_ftp <- cpc_base_ftp(us = TRUE)
  base_ftp <- file.path(base_ftp, 'UPDATED', year)
  
  # use httr and rvest to get the html table
  doc_table <- base_ftp %>% 
    httr::GET() %>% 
    httr::content() %>% 
    rvest::html_table() %>% 
    dplyr::first()
  last_modified_date <- doc_table %>% 
    dplyr::mutate(last_modified = lubridate::dmy_hm(`Last modified`)) %>% 
    na.omit() %>% 
    dplyr::pull(last_modified) %>% 
    max()
  
  # check if max value in the table is greater than last modified date on file
  is_updated <- isTRUE(last_modified_date > last_modified_date_local)
  
  # is_updated <- TRUE
  if (!isTRUE(is_updated)) cli::cli_abort('Stopping: Updated rainfall data not yet available')
  return(is_updated)
}

download_updated_rainfall_update <- function(year, month){
  # pull down the updated data
  dt_ranges <- create_dates(year, month) %>% dplyr::last()
  updated_data <- download_gridded_rainfall_base(dt_ranges, type = 'updated')
  
  return(updated_data)
}

upload_gridded_rainfall_updated <- function(.data, conn){
  use_db <- isTRUE(config::get('use_db'))
  
  if(use_db){
    # get db info
    table_names <- config::get('db_tables')
    schema <- table_names$schema
    table_name <- table_names$rainfall #'CPC1948_Forward_mm'
    
    # write .data to a temp file on the db so the join can be conducted on the db
    new_data <- .data |> dplyr::select(lon, lat, mn, yr, precip.mm.update = precip.mm, maxDaily.mm.update = maxDaily.mm)
    temp_table_name <- DBI::Id(schema = schema, table = 'temp_rainfall_update')
    DBI::dbWriteTable(conn, temp_table_name, new_data, overwrite = TRUE)
    
    # join the tables on the db
    full_tbl <- dplyr::tbl(con, DBI::Id(schema = schema, table = table_name))
    temp_tbl <- dplyr::tbl(con, temp_table_name)
    joined_tbl <- full_tbl %>%
      dplyr::left_join(temp_tbl, by = c('lon', 'lat', 'mn', 'yr')) |> 
      dplyr::collect()
    
    # write to db table
    DBI::dbWriteTable(
      conn, 
      DBI::Id(
        schema = schema,
        table = table_name
      ),
      joined_tbl,
      overwrite = TRUE
    )
    
    # alt method: do not pull into memory 
    # query representing the joined table
    # sql_query <- dbplyr::sql_render(joined_tbl)
    # sql_command <- glue::glue("CREATE TABLE {schema}.{table_name} AS {sql_query}")
    # DBI::dbExecute(conn, sql_command)
    
    return(NULL)
  } else {
    # write just the updated data to a csv
    file_out <- file.path(config::get('output'), 'rainfall_update.csv')
    readr::write_csv(.data, file_out)
    return(file_out)
  }
}
