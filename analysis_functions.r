library(tidyverse)
library(cansim)
library(readr)
library(vroom)
library(lubridate)
library(tseries)
library(strucchange)

library(purrr) # For the reduce() function


gdp_vec <- get_cansim_vector_for_latest_periods("v1000000673", periods = 300)
cpi_vec <- get_cansim_vector_for_latest_periods("v41690914", periods = 300)


transform_quarterly <- function(abc_vec) {

  vec_name <- deparse(substitute(abc_vec))
  base_name <- sub("_vec$", "", vec_name)

  previous_col_name <- paste0(base_name, "_previous_quarter")
  delta_col_name <- paste0("delta_", base_name)
  
  abc_vec |>
    select(Date, VALUE) |>
    mutate(quarter = quarter(Date, with_year = TRUE)) |>
    group_by(quarter) |>
    summarise(VALUE = mean(VALUE, na.rm = TRUE)) |>
    ungroup() |>
    mutate(
      !!previous_col_name := lag(VALUE, 1),
      !!delta_col_name := (log(VALUE) - log(.data[[previous_col_name]])) * 100
    ) |>
     select(quarter, !!delta_col_name)|>
    drop_na()
}

gdp_quarterly <- transform_quarterly(gdp_vec)
cpi_quarterly <- transform_quarterly(cpi_vec)


combine_processed_dfs <- function(...) {
  # Capture all the data frames passed as arguments into a list
  list_of_dfs <- list(...)
  
  # Use reduce() to apply inner_join cumulatively to the list of data frames
  # It will join the first two, then join that result with the third, and so on.
  combined_df <- reduce(list_of_dfs, inner_join, by = "quarter")
  
  return(combined_df)
}

full_quarterly <- combine_processed_dfs(gdp_quarterly, cpi_quarterly)