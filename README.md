
# Template for DBT pipelines.
A simple way to replace `Deploy` feature of DBT cloud. Fork and customize for your DBT project.

## .github/workflows/prod.yml
Pipeline for running DBT project on schedule and on merge to `main` branch. Uses GitHub action `molar-volume/dbt-snowflake`. 

## .github/workflows/uat.yml
Pipeline for running DBT project on PR opened. Uses GitHub action `molar-volume/dbt-snowflake`.
