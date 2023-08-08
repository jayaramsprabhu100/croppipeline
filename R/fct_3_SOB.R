# summary of business
# download new files from site
# ensure it doesn't time out
# drop old data from the DB
# push new data to DB
# run weekly


#' Check if the Summary of Business data is already downloaded
#'
#' @param conn Connection to the database where tables are stored
#' @return
#' @export
#'
#' @examples
SOB_check_if_updated <- function(conn){
  
  use_db <- isTRUE(config::get('use_db'))
  
  if(use_db){
    # get db info
    table_names <- config::get('db_tables')
    schema <- table_names$schema
    table_name <- table_names$SOB #Ind_Exp_1989_Forward?
    
    # pull latest table
    date_updated <- conn |> 
      dplyr::tbl(dbplyr::in_schema(schema, table_name)) |>  
      dplyr::select(DateUpdated) |> 
      dplyr::collect() |> 
      dplyr::pull(DateUpdated)
    date_updated <- max(as.Date(date_updated))
    
  } else {
    return(TRUE)
  }
  
  # stop if data has been updated in the last 7 days
  if (date_updated >= (Sys.Date() - 7)){
    cli::cli_abort('Stopping: Summary of Business data already downloaded')
  }
  
  return(TRUE)
}

#' Download the custom Summary of Business report from the USDA
#'
#' @param year year of report
#' @param n_try number of attempts to make to the server
#' @param timeout the timeout in seconds
#'
#' @return
#' @author Joe Marlo (Lander Analytics)
#' @export
#'
#' @examples
#' \dontrun{
#' .data <- SOB_download(2021)
#' }
SOB_download <- function(year, n_try = 5, timeout = 180){
  
  n_try <- as.numeric(n_try)
  timeout <- as.numeric(timeout)
  
  url_base <- config::get('base_urls')$SOB
  url_arg <- glue::glue('?RY={year}&ORD=RY,CM,ST,CT,DT,IP,CVL&CC=S&VisibleColumns=ReinsuranceYear,CommodityCode,CommodityName,LocationStateCode,LocationStateName,LocationStateAbbreviation,LocationCountyCode,LocationCountyName,DeliveryTypeCode,DeliveryTypeName,InsurancePlanCode,InsurancePlanAbbreviation,CoverageLevelPercent,PolicySoldCount,PolicyPremiumCount,PolicyIndemnityCount,UnitPremiumCount,UnitIndemnityCount,CommodityReportingLevelAmount,CommodityReportingLevelType,EndorsedReportingLevelAmount,LiabilityAmount,TotalPremiumAmount,SubsidyAmount,IndemnityAmount,EFAPremiumAmount,AdditionalSubsidyAmount,StatePrivateSubsidyAmount,EarnPremiumRate,loss_ratio&SortField=&SortDir=')
  url_full <- glue::glue('{url_base}{url_arg}')
  
  # try downloading the summary of business; stop if exceed n_trys or is successful
  n <- 1
  success <- list()
  success$status <- FALSE
  while(n <= n_try & !success$status){
    
    cli::cli_alert_info("Trying to download Summary of Business report: On try {n}")
    
    success <- tryCatch({
      # download file
      tmp <- tempfile(fileext = '.xlsx')
      httr::GET(url_full,
                httr::write_disk(tmp, overwrite = TRUE),
                httr::timeout(timeout))
      
      # read in downloaded data into memory
      .data <- readxl::read_excel(tmp, skip = 1)
      unlink(tmp)
      
      list(status = TRUE, data = .data)
    },
    error = function(e) list(status = FALSE)
    )
    
    n <- n + 1
  }
  
  # data quality checks
  if (!success$status) {
    cli::cli_abort('Could not download Summary of Business report')
  }
  if (!is_truthy(success$data)){
    cli::cli_abort('Successfully downloaded Summary of Business report but data is empty')
  }
  n_cols_expected <- 30
  if (ncol(success$data) != n_cols_expected){
    cli::cli_abort('Successfully downloaded Summary of Business report but data has incorrect number of columns')
  }

  # add column names
  col_names <- c(
    "ReinsuranceYear",
    "CommodityCode",
    "CommodityName",
    "LocationStateCode",
    "LocationStateName",
    "LocationStateAbbreviation",
    "LocationCountyCode",
    "LocationCountyName",
    "DeliveryTypeCode",
    "DeliveryTypeName",
    "InsurancePlanCode",
    "InsurancePlanAbbreviation",
    "CoverageLevelPercent",
    "PolicySoldCount",
    "PolicyPremiumCount",
    "PolicyIndemnityCount",
    "UnitPremiumCount",
    "UnitIndemnityCount",
    "CommodityReportingLevelAmount",
    "CommodityReportingLevelType",
    "EndorsedReportingLevelAmount",
    "LiabilityAmount",
    "TotalPremiumAmount",
    "SubsidyAmount",
    "IndemnityAmount",
    "EFAPremiumAmount",
    "AdditionalSubsidyAmount",
    "StatePrivateSubsidyAmount",
    "EarnPremiumRate",
    "LossRatio"
  )
  colnames(success$data) <- col_names
  
  # add data update column
  success$data$DateUpdated <- Sys.Date()
  
  return(success$data)
}

SOB_drop_obsolete <- function(.data){
  # deprecated
  
  return(.data)
}

SOB_write <- function(.data, conn){
  
  use_db <- isTRUE(config::get('use_db'))
  
  if(use_db){
    # get db info
    table_names <- config::get('db_tables')
    schema <- table_names$schema
    table_name <- table_names$SOB #'Ind_Exp_1989_Forward'
    
    # write to db table
    DBI::dbAppendTable(
      conn, 
      DBI::Id(
        schema = schema,
        table = table_name
      ),
      .data
    )
    
    return(NULL)
  } else {
    # write to csv
    file_out <- file.path(config::get('output'), 'summary_of_business.csv')
    readr::write_csv(.data, file_out)
    return(file_out)
  }
}
