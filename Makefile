# Makes it easy to create virtual environments for different versions of dbt
ADAPTERS = sqlite duckdb

everything: .venv-dbt18/bin/python
.PHONY: everything

.venv-dbt18/bin/python:
	python -m venv .venv-dbt18
	.venv-dbt18/bin/pip install --upgrade wheel setuptools pip
	.venv-dbt18/bin/pip install '.[dev,test]'

clean:
	rm -rf .venv-dbt18
.PHONY: clean

test-dbt1.8: .venv-dbt18/bin/python
	.venv-dbt18/bin/python -m pytest tests/test_main.py
.PHONY: test-dbt1.8

test: test-dbt1.8
.PHONY: test
