
connect_to_db <- function(){
  
  # NOTE: this can be updated to use the Rstudio Pro driver
  # the 'ODBC Driver 17 for SQL Server' should be replaced with the driver name in the odbcinst.ini file
  sandbox <- 'driver=ODBC Driver 17 for SQL Server; server=stm-actdb-19; database=Sandbox; trusted_connection=yes'
  conn <- DBI::dbConnect(odbc::odbc(), .connection_string = sandbox)
  
  return(conn)
}
