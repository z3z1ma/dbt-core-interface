# Makes it easy to create virtual environments for different versions of dbt
ADAPTERS = bigquery sqlite duckdb
ADAPTERS_WITHOUT_DUCKDB = bigquery sqlite

everything: .venv-dbt10/bin/python .venv-dbt11/bin/python .venv-dbt12/bin/python .venv-dbt13/bin/python .venv-dbt14/bin/python
.PHONY: everything

.venv-dbt10/bin/python:
	python -m venv --upgrade-deps .venv-dbt10
	.venv-dbt10/bin/pip install --upgrade wheel
	.venv-dbt10/bin/pip install -e .
	for adapter in $(ADAPTERS); do \
		VIRTUAL_ENV=.venv-dbt10 poetry install; \
		.venv-dbt10/bin/pip install "dbt-$$adapter>=1.0.0,<1.1.0"; \
	done

.venv-dbt11/bin/python:
	python -m venv --upgrade-deps .venv-dbt11
	.venv-dbt11/bin/pip install --upgrade wheel
	.venv-dbt11/bin/pip install -e .
	for adapter in $(ADAPTERS); do \
		VIRTUAL_ENV=.venv-dbt11 poetry install; \
		.venv-dbt11/bin/pip install "dbt-$$adapter>=1.1.0,<1.2.0"; \
	done

.venv-dbt12/bin/python:
	python -m venv --upgrade-deps .venv-dbt12
	.venv-dbt12/bin/pip install --upgrade wheel
	.venv-dbt12/bin/pip install -e .
	for adapter in $(ADAPTERS); do \
		VIRTUAL_ENV=.venv-dbt12 poetry install; \
		.venv-dbt12/bin/pip install "dbt-$$adapter>=1.2.0,<1.3.0"; \
	done

.venv-dbt13/bin/python:
	python -m venv --upgrade-deps .venv-dbt13
	.venv-dbt13/bin/pip install --upgrade wheel
	.venv-dbt13/bin/pip install -e .
	for adapter in $(ADAPTERS); do \
		VIRTUAL_ENV=.venv-dbt13 poetry install; \
		.venv-dbt13/bin/pip install "dbt-$$adapter>=1.3.0,<1.4.0"; \
	done

# DuckDB is not supported in dbt 1.4.0 yet
.venv-dbt14/bin/python:
	python -m venv --upgrade-deps .venv-dbt14
	.venv-dbt14/bin/pip install --upgrade wheel
	.venv-dbt14/bin/pip install -e .
	for adapter in $(ADAPTERS_WITHOUT_DUCKDB); do \
		VIRTUAL_ENV=.venv-dbt14 poetry install; \
		.venv-dbt14/bin/pip install "dbt-$$adapter>=1.4.0,<1.5.0"; \
	done

clean:
	rm -rf .venv-dbt10 .venv-dbt11 .venv-dbt12 .venv-dbt13 .venv-dbt14
.PHONY: clean

test-dbt10: .venv-dbt10/bin/python
	.venv-dbt10/bin/python -m pytest tests
.PHONY: test-dbt10

test-dbt11: .venv-dbt11/bin/python
	.venv-dbt11/bin/python -m pytest tests
.PHONY: test-dbt11

test-dbt12: .venv-dbt12/bin/python
	.venv-dbt12/bin/python -m pytest tests
.PHONY: test-dbt12

test-dbt13: .venv-dbt13/bin/python
	.venv-dbt13/bin/python -m pytest tests
.PHONY: test-dbt13

test-dbt14: .venv-dbt14/bin/python
	.venv-dbt14/bin/python -m pytest tests
.PHONY: test-dbt14

test: test-dbt10 test-dbt11 test-dbt12 test-dbt13 test-dbt14
.PHONY: test