# Hudson crop automation pipeline 

Project to automate crop data updates on a regular basis. This pulls data from US Government websites, formats the data, and then publishes it to the sandbox database. It is scheduled to run on Posit Connect.

## Directory structure

    .
    ├── /_targets                               # Objects created automatically by `targets`
    ├── /dev                                    # R scripts outside of the pipelines 
    |    ├── compare_data.R                     # R code for testing
    |    └── initialize_tables.R                # R code to initialize SQL tables
    ├── /R                                      # R functions 
    ├── /renv                                   # Files for `renv` R package dependency management
    ├── /tests                                  # Unit tests for R code 
    ├── config.yml
    ├── _targets.yml
    ├── run.sh                                  # Run all the pipelines from shell 
    ├── .Rprofile
    ├── _run_pipeline_COL.Rmd                   # File to kickoff individiual cause-of-loss pipeline 
    ├── _run_pipeline_rainfall.Rmd              # File to kickoff individiual rainfall-base pipeline
    ├── _run_pipeline_rainfall_update.Rmd       # File to kickoff individiual rainfall-update pipeline
    ├── _run_pipeline_SOB.Rmd                   # File to kickoff individiual summary of business pipeline
    ├── _run_test.Rmd                           # File to test the Connect server's access to the database
    ├── _targets.R                              # R code defining the pipeline
    ├── renv.lock                               # manifest stipulating R package dependencies
    └── README.md

<br>

## Running the pipeline 

### Local developer environment

Set up your directory and open the `crop_automation.Rproj` file to launch RStudio. If the code is put into git, clone the repository to your machine use `git clone` in terminal or within RStudio Workbench. Run `renv::restore()` in R to install the R dependencies. 

### `targets` R package

The pipelines are built using the R package `targets`. `targets` is similar to make files. It ensures the code is run in the right order. It organizes the code into a Directed Acyclic Graph (DAG), establishing dependencies and ensuring efficient execution. It helps you avoid redundancy in your process. It also expertly moves objects in and out of memory when needed, which means memory is implicitly handled more efficiently. Many of these features are not fully utilized in this project as the ETL process is linear.

The project is structure as one pipeline that comprises of four sub pipelines that are handled independently:
- Cause of Loss (CoL)
- Rainfall
- Rainfall update
- Summary of Business (SoB)

This sub-pipelines are executed within their respective Rmarkdown files (.Rmd) via codechunk similar to:

```
targets::tar_make(
  names = tidyselect::starts_with('COL_'),
  callr_arguments = list(
    env = c(year = params$year, timeout = params$timeout)
  )
)
```

This chunk executes just the CoL pipeline via the `tidyselect::starts_with('COL')` argument. This works as the individual steps (or targets) within the CoL pipeline are prefixed with 'COL'. 

More details on `targets` can be found here: [https://books.ropensci.org/targets/]( https://books.ropensci.org/targets/)

<br>

## Deploying to Posit Connect

The RMarkdown files can be re/deployed to Connect by opening the individual .Rmd files within Workbench and clicking the blue button in the top right. Please upload the package as source when prompted.

The files are currently published under the JMarlo user. Ideally, the new code-owner will re-publish the files under their user.

### Scheduling

The files are scheduled to run on Posit Connect. On the right side of the Connect UI there is a "Schedule" button. The original schedules are noted within their respective .Rmd files.

<br>

## Data output

The data is written to the tables specified in the `config.yml` file under `db_tables`.  These can be adjusted but historical data may need to be manually transferred over.  

The Connect server needs to have read/write access to these tables. Currently the https://rpublishdev/ server has a service account that has readr/write access to the database. The R code connects to the database via the `connect_to_db()` function within `R/data.R`. 

The Connect server's access to the database can be tested by publishing the `_run_test.Rmd` document.

<br>

## Dependencies

[`renv`](https://rstudio.github.io/renv/index.html) is used to manage dependencies. You can use `renv::restore()` to install all the necessary packages. Do not add `ADRC` as a dependency even if `renv` suggests it. `renv.lock` contains a manifest of the R version, the packages and their versions, and the repositories.

<br>

## Code guidelines

The aim is to adhere to [`tidyverse`](https://www.tidyverse.org/) style for the majority of the code, and [tidyverse style guide](https://style.tidyverse.org/) for code formatting. The easiest way to to format ~90% of your R code is `Code -> Reformat Code` within Rstudio.

<br>

## Testing

- [`roxygen2`](https://roxygen2.r-lib.org/) is used for documenting functions  
- [`testthat`](https://testthat.r-lib.org/) for unit testing  

### Unit tests

Unit tests are written using `testthat` and can be found in the `/tests/testhat` directory. Run `source('tests/testthat.R')` to run the tests at will. Add new tests by either manually adding to the current scripts or adding a new script within the `/tests/testhat` directory. Note that many functions are not included in these tests because they directly retrieve data from the internet.


<br>

## Suggestions for future enchancements

- Place this codebase within a git hosting service for version tracking -- in progess
- Educate a subject matter expert on `targets` so any future adjustments can be made. I foresee the government URLs may change.  
- Find a static source of data such as an FTP source instead of the report generators. The generator is quite fickle. 

<br>

## Support

Please contact support@landeranalytics.com with any questions.
