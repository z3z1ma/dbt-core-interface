# Makes it easy to create virtual environments for different versions of dbt
ADAPTERS = sqlite duckdb
ADAPTERS_WITHOUT_DUCKDB = sqlite

everything: .venv-dbt10/bin/python .venv-dbt11/bin/python .venv-dbt12/bin/python .venv-dbt13/bin/python .venv-dbt14/bin/python
.PHONY: everything

.venv-dbt10/bin/python:
	python -m venv .venv-dbt10
	.venv-dbt10/bin/pip install --upgrade wheel setuptools pip
	.venv-dbt10/bin/pip install pytest WebTest .
	for adapter in $(ADAPTERS); do \
		.venv-dbt10/bin/pip install "dbt-$$adapter>=1.0.0,<1.1.0"; \
	done

.venv-dbt11/bin/python:
	python -m venv .venv-dbt11
	.venv-dbt11/bin/pip install --upgrade wheel setuptools pip
	.venv-dbt11/bin/pip install pytest WebTest .
	for adapter in $(ADAPTERS); do \
		.venv-dbt11/bin/pip install "dbt-$$adapter>=1.1.0,<1.2.0"; \
	done

.venv-dbt12/bin/python:
	python -m venv .venv-dbt12
	.venv-dbt12/bin/pip install --upgrade wheel setuptools pip
	.venv-dbt12/bin/pip install pytest WebTest .
	for adapter in $(ADAPTERS); do \
		.venv-dbt12/bin/pip install "dbt-$$adapter>=1.2.0,<1.3.0"; \
	done

.venv-dbt13/bin/python:
	python -m venv .venv-dbt13
	.venv-dbt13/bin/pip install --upgrade wheel setuptools pip
	.venv-dbt13/bin/pip install pytest WebTest .
	for adapter in $(ADAPTERS); do \
		.venv-dbt13/bin/pip install "dbt-$$adapter>=1.3.0,<1.4.0"; \
	done

.venv-dbt14/bin/python:
	python -m venv .venv-dbt14
	.venv-dbt14/bin/pip install --upgrade wheel setuptools pip
	.venv-dbt14/bin/pip install pytest WebTest .
	for adapter in $(ADAPTERS); do \
		.venv-dbt14/bin/pip install "dbt-$$adapter>=1.4.0,<1.5.0"; \
	done

# Adapters have not been updated for dbt-core 1.5.0 so we need to
# install the adapters first and then install dbt-core 1.5.0
.venv-dbt15/bin/python:
	python -m venv .venv-dbt15
	.venv-dbt15/bin/pip install --upgrade wheel setuptools pip
	.venv-dbt15/bin/pip install pytest WebTest .
	for adapter in $(ADAPTERS); do \
		.venv-dbt15/bin/pip install "dbt-$$adapter>=1.4.0"; \
		.venv-dbt15/bin/pip install "dbt-core==1.5.0b5"; \
	done

clean:
	rm -rf .venv-dbt10 .venv-dbt11 .venv-dbt12 .venv-dbt13 .venv-dbt14 .venv-dbt15
.PHONY: clean

test-dbt1.0: .venv-dbt10/bin/python
	.venv-dbt10/bin/python -m pytest tests
.PHONY: test-dbt1.0

test-dbt1.1: .venv-dbt11/bin/python
	.venv-dbt11/bin/python -m pytest tests
.PHONY: test-dbt1.1

test-dbt1.2: .venv-dbt12/bin/python
	.venv-dbt12/bin/python -m pytest tests
.PHONY: test-dbt1.2

test-dbt1.3: .venv-dbt13/bin/python
	.venv-dbt13/bin/python -m pytest tests
.PHONY: test-dbt1.3

test-dbt1.4: .venv-dbt14/bin/python
	.venv-dbt14/bin/python -m pytest tests
.PHONY: test-dbt1.4

test-dbt1.5: .venv-dbt15/bin/python
	.venv-dbt15/bin/python -m pytest tests
.PHONY: test-dbt1.5

test: test-dbt1.0 test-dbt1.1 test-dbt1.2 test-dbt1.3 test-dbt1.4 test-dbt1.5
.PHONY: test
