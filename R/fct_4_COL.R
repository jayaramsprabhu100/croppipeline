
# download cause of loss (COL) files from website
# unzip and then INSERT into DB
# need to check if URLs have changed
# run weekly alongside SOB


#' Download cause-of-loss zip file and extract first file to dataframe
#'
#' This relies on the website continuing to using the following url format. https://www.rma.usda.gov/en/Information-Tools/Summary-of-Business/Cause-of-Loss/-/media/RMA/Cause-Of-Loss/Summary-of-Business-with-Month-of-Loss/colsom_{year}.ashx?la=en
#' 
#' If this format changes, will need to switch to an rvest approach.
#'
#' @param year 
#' @param timeout timeout in seconds
#'
#' @return
#' @export
#'
#' @examples
#' \dontrun{
#' data_2022 <- COL_download('2022')
#' }
COL_download <- function(year, timeout = 120){
  
  timeout <- as.numeric(timeout)
  
  # set URL
  url <- config::get('base_urls')$COL
  url_file <- glue::glue(
    '{url}/-/media/RMA/Cause-Of-Loss/Summary-of-Business-with-Month-of-Loss/colsom_{year}.ashx?la=en'
  )
  
  # download file
  tmp <- tempfile()
  httr::GET(url_file,
            httr::write_disk(tmp, overwrite = TRUE),
            httr::timeout(timeout))
  
  # extract first file only
  filename <- utils::unzip(tmp, list = TRUE)
  if (nrow(filename) < 1) cli::cli_abort('No files found in the downloaded zip. Download likely failed.')
  .data <- suppressWarnings( #suppress colnames wanrings
    readr::read_delim(unz(tmp, filename$Name[[1]]), col_names = FALSE, delim = '|')
  )
  unlink(tmp)
  
  # data quality checks
  if (!is_truthy(.data)){
    cli::cli_abort('Successfully downloaded Cause of Loss report but data is empty')
  }
  if (!isTRUE(is.data.frame(.data))){
    cli::cli_abort('Successfully downloaded Cause of Loss report but could not coerce it to data.frame')
  } 
  n_cols_expected <- 30 
  if (ncol(.data) != n_cols_expected){
    cli::cli_abort('Successfully downloaded Cause of Loss report but data has incorrect number of columns')
  }
  
  # add column names
  col_names <- c(
    'CommodityYear',
    'StateCode',
    'StateAbbrv',
    'CountyCode',
    'CountyName',
    'CommodityCode',
    'CommodityName',
    'InsurancePlanCode',
    'InsurancePlan',
    'CoverageCategory',
    'StageCode',
    'COLCode',
    'COLDescription',
    'MonthOfLoss',
    'MonthOfLossName',
    'YearOfLoss',
    'PoliciesEarningPremium',
    'PoliciesIndemnified',
    'NetPlantedAcres',
    'NetEndorsedAcres',
    'Liability',
    'TotalPremium',
    'ProducerPaidSubsidy',
    'SubsidyAmount',
    'StatePrivateSubsidy',
    'AdditionalSubsidy',
    'EFAPremiumDiscount',
    'NetDeterminedAcres',
    'IndemnityAmount',
    'LossRatio'
  )
  
  colnames(.data) <- col_names
  
  # add data update column
  .data$DateUpdated <- Sys.Date()
  
  return(.data)
}


COL_write <- function(.data, conn){
  
  use_db <- isTRUE(config::get('use_db'))
  
  if(use_db){
    # get db info
    table_names <- config::get('db_tables')
    schema <- table_names$schema
    table_name <- table_names$COL #see "IndustryCOL Bulk Insert.sql" file
    
    # write to db table
    DBI::dbWriteTable(
      conn, 
      DBI::Id(
        schema = schema,
        table = table_name
      ),
      .data, 
      append = TRUE
    )
    
    return(NULL)
  } else {
    # write to csv
    file_out <- file.path(config::get('output'), 'cause_of_loss.csv')
    readr::write_csv(.data, file_out)
    return(file_out)
  }
}
