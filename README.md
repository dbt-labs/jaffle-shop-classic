
# Template for DBT pipelines.
A simple way to replace `Deploy` feature of DBT cloud. Fork and customize for your DBT project.

## [prod.yml](.github/workflows/prod.yml)
Pipeline for running DBT project on schedule and on the merge to `main` branch. 

## [uat.yml](.github/workflows/uat.yml)
Pipeline for running DBT project on the PR opened to `main`.
