### various utility functions ###

`%notin%` <- Negate(`%in%`)

is_truthy <- function(x){
  
  ##### additions
  if (inherits(x, "data.frame") && nrow(x) == 0)
    return(FALSE)
  
  ##### shiny::isTruthy
  if (inherits(x, "try-error")) 
    return(FALSE)
  if (!is.atomic(x)) 
    return(TRUE)
  if (is.null(x)) 
    return(FALSE)
  if (length(x) == 0) 
    return(FALSE)
  if (all(is.na(x))) 
    return(FALSE)
  if (is.character(x) && !any(nzchar(stats::na.omit(x)))) 
    return(FALSE)
  if (inherits(x, "shinyActionButtonValue") && x == 0) 
    return(FALSE)
  if (is.logical(x) && !any(stats::na.omit(x))) 
    return(FALSE)
  return(TRUE)
}

#' Require x to be within range y
#'
#' @param x 
#' @param y 
#'
#' @return
#' @export
#' @author Hudson
#' @examples
assert_range <- function(x, y) {
  if (!x %in% y) {
    stop(sprintf("%s must be between %s and %s",
                 deparse(substitute(x)), min(y), max(y)), call. = FALSE)
  }
}
