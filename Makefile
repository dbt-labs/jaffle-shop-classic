init:
	pipenv update
	pipenv run dbt deps

clean-env:
	:> .env

env-development-salt:
	echo SALT=maldon >> .env

env-target:
	echo DATABRICKS_TARGET=$$(git symbolic-ref --short HEAD | tr /- _) >> .env

package-project:
	for PACKAGE in dbt_project_evaluator ; do \
		cp package_projects/$$PACKAGE.yml dbt_packages/$$PACKAGE/dbt_project.yml ; \
	done

build-env: clean-env env-development-salt env-target package-project

dbt-deps:
	pipenv run dbt deps

dbt-build: build-env
	pipenv run dbt build --selector jaffle_shop

run-dbt-project-evaluator: dbt-deps build-env
	pipenv run dbt --warn-error build --select package:dbt_project_evaluator dbt_project_evaluator_exceptions

lint: build-env
	pipenv run sqlfluff lint

format: build-env
	pipenv run sqlfluff fix

run-python-tests:
	pipenv run pytest --quiet --show-capture=no --tb=no

run-python-tests-detailed:
	pipenv run pytest