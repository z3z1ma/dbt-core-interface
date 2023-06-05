#!/usr/bin/env bash
# This is a simple script for testing the SQLFluff /lint server endpoint.
curl -X POST -H "Content-Type: application/json" -H "X-dbt-Project: dbt_project" -d '{"project_dir":"/app/tests/sqlfluff_templater/fixtures/dbt/dbt_project/","profiles_dir":"/app/tests/sqlfluff_templater/fixtures/dbt/profiles_yml/","target":"dev"}' http://localhost:8581/register
curl -X POST -H "Content-Type: application/json" -H "X-dbt-Project: dbt_project" -G -d 'sql_path=/app/tests/sqlfluff_templater/fixtures/dbt/dbt_project/models/my_new_project/issue_1608.sql' http://localhost:8581/lint
