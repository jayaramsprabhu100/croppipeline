### This script initializes the tables on the database ###

source('R/data.R')

library(dbplyr)
library(dplyr)
conn <- connect_to_db()

# check table
conn |>
  tbl(dbplyr::in_schema('dbo', 'crop_auto_rainfall'))
conn |>
  tbl(dbplyr::in_schema('dbo', 'crop_auto_SOB'))
conn |>
  tbl(dbplyr::in_schema('dbo', 'crop_auto_COL'))
# conn |>
#   tbl(dbplyr::in_schema('dbo', 'IndustrySOB')) |> 
#   head(100) |> 
#   collect() |> 
#   View()


# get table names from the config
table_names <- config::get('db_tables')
table_names$schema
table_names$rainfall
table_names$SOB
table_names$COL

# create the new tables

# rainfall table
local({
  
  # set table schema
  cols <- c(
    'grid_id' = 'int',
    'yr' = 'real',
    'mn' = 'real',
    'lon' = 'real',
    'lat' = 'real',
    'precip.mm' = 'real', 
    'maxDaily.mm' = 'real',
    'precip.mm.update' = 'real', 
    'maxDaily.mm.update' = 'real',
    'date.downloaded' = 'datetime',
    'date.downloaded.update' = 'datetime'
  )
  
  # create dummy table template
  template_df <- data.frame(matrix(ncol = length(cols), nrow = 0))
  colnames(template_df) <- names(cols)
  
  # create table on database
  DBI::dbWriteTable(
    conn,
    DBI::Id(
      schema = table_names$schema,
      table = table_names$rainfall
    ),
    value = template_df,
    field.types = cols
    # overwrite = TRUE
  )
  # DBI::dbRemoveTable(conn,  DBI::Id(
  #   schema = table_names$schema,
  #   table = table_names$rainfall
  # ))
})

# summary of business
local({
  
  # set table schema
  cols <- c(
    'ReinsuranceYear' = 'real',
    'CommodityCode' = 'text',
    'CommodityName' = 'text',
    'LocationStateCode' = 'text',
    'LocationStateName' = 'text',
    'LocationStateAbbreviation' = 'text',
    'LocationCountyCode' = 'text',
    'LocationCountyName' = 'text',
    'DeliveryTypeCode' = 'text',
    'DeliveryTypeName' = 'text',
    'InsurancePlanCode' = 'text',
    'InsurancePlanAbbreviation' = 'text',
    'CoverageLevelPercent' = 'real',
    'PolicySoldCount' = 'int',
    'PolicyPremiumCount' = 'int',
    'PolicyIndemnityCount' = 'int',
    'UnitPremiumCount' = 'int',
    'UnitIndemnityCount' = 'int',
    'CommodityReportingLevelAmount' = 'int',
    'CommodityReportingLevelType' = 'text',
    'EndorsedReportingLevelAmount' = 'int',
    'LiabilityAmount' = 'int',
    'TotalPremiumAmount' = 'int',
    'SubsidyAmount' = 'int',
    'IndemnityAmount' = 'int',
    'EFAPremiumAmount' = 'int',
    'AdditionalSubsidyAmount' = 'int',
    'StatePrivateSubsidyAmount' = 'int',
    'EarnPremiumRate' = 'real',
    'LossRatio' = 'real',
    'DateUpdated' = 'datetime'
  )
  
  # create dummy table template
  template_df <- data.frame(matrix(ncol = length(cols), nrow = 0))
  colnames(template_df) <- names(cols)
  
  # create table on database
  DBI::dbWriteTable(
    conn,
    DBI::Id(
      schema = table_names$schema,
      table = table_names$SOB
    ),
    value = template_df,
    field.types = cols
    # overwrite = TRUE
  )
  # conn |>  tbl(dbplyr::in_schema('dbo', 'crop_auto_SOB'))
})

# cause of loss
local({
  
  # set table schema
  cols <- c(
    'CommodityYear' = 'text',
    'StateCode' = 'text',
    'StateAbbrv' = 'text',
    'CountyCode' = 'text',
    'CountyName' = 'text',
    'CommodityCode' = 'text',
    'CommodityName' = 'text',
    'InsurancePlanCode' = 'text',
    'InsurancePlan' = 'text',
    'CoverageCategory' = 'text',
    'StageCode' = 'text',
    'COLCode' = 'text',
    'COLDescription' = 'text',
    'MonthOfLoss' = 'text',
    'MonthOfLossName' = 'text',
    'YearOfLoss' = 'text',
    'PoliciesEarningPremium' = 'int',
    'PoliciesIndemnified' = 'int',
    'NetPlantedAcres' = 'real',
    'NetEndorsedAcres' = 'real',
    'Liability' = 'real',
    'TotalPremium' = 'real',
    'ProducerPaidSubsidy' = 'real',
    'SubsidyAmount' = 'real',
    'StatePrivateSubsidy' = 'real',
    'AdditionalSubsidy' = 'real',
    'EFAPremiumDiscount' = 'real',
    'NetDeterminedAcres' = 'real',
    'IndemnityAmount' = 'real',
    'LossRatio' = 'real',
    'DateUpdated' = 'datetime'
  )
  
  # create dummy table template
  template_df <- data.frame(matrix(ncol = length(cols), nrow = 0))
  colnames(template_df) <- names(cols)
  
  # create table on database
  DBI::dbWriteTable(
    conn,
    DBI::Id(
      schema = table_names$schema,
      table = table_names$COL
    ),
    value = template_df,
    field.types = cols
    # overwrite = TRUE
  )
})

# free db connection
DBI::dbDisconnect(conn)
