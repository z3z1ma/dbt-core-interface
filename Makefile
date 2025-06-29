# Makes it easy to create virtual environments for different versions of dbt
ADAPTERS = sqlite duckdb

everything: .venv-dbt18/bin/python
.PHONY: everything

# SQLITE adapter is not supported in dbt 1.8+ yet: https://github.com/codeforkjeff/dbt-sqlite/issues
ADAPTERS = duckdb

.venv-dbt18/bin/python:
	python -m venv .venv-dbt18
	.venv-dbt18/bin/pip install --upgrade wheel setuptools pip
	.venv-dbt18/bin/pip install pytest WebTest rich .
	for adapter in $(ADAPTERS); do \
		.venv-dbt18/bin/pip install "dbt-core>=1.8.0,<1.9.0"; \
		.venv-dbt18/bin/pip install "dbt-$$adapter"; \
	done
	.venv-dbt18/bin/pip install --force-reinstall dbt-adapters dbt-common;

clean:
	rm -rf .venv-dbt10 .venv-dbt11 .venv-dbt12 .venv-dbt13 .venv-dbt14 .venv-dbt15 .venv-dbt16 .venv-dbt17 .venv-dbt18
.PHONY: clean

test-dbt1.8: .venv-dbt18/bin/python
	.venv-dbt18/bin/python -m pytest tests/test_main.py
.PHONY: test-dbt1.8

test: test-dbt1.8
.PHONY: test
