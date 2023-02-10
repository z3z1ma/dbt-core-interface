# Makes it easy to create virtual environments for different versions of dbt

everything: .venv-dbt10/bin/python .venv-dbt11/bin/python .venv-dbt12/bin/python .venv-dbt13/bin/python .venv-dbt14/bin/python
.PHONY: everything

.venv-dbt10/bin/python:
	python -m venv --upgrade-deps .venv-dbt10
	.venv-dbt10/bin/pip install --upgrade wheel
	.venv-dbt10/bin/pip install -e .
	.venv-dbt10/bin/pip install "dbt-core>=1.0.0,<1.1.0"

.venv-dbt11/bin/python:
	python -m venv --upgrade-deps .venv-dbt11
	.venv-dbt11/bin/pip install --upgrade wheel
	.venv-dbt11/bin/pip install -e .
	.venv-dbt11/bin/pip install "dbt-core>=1.1.0,<1.2.0"

.venv-dbt12/bin/python:
	python -m venv --upgrade-deps .venv-dbt12
	.venv-dbt12/bin/pip install --upgrade wheel
	.venv-dbt12/bin/pip install -e .
	.venv-dbt12/bin/pip install "dbt-core>=1.2.0,<1.3.0"

.venv-dbt13/bin/python:
	python -m venv --upgrade-deps .venv-dbt13
	.venv-dbt13/bin/pip install --upgrade wheel
	.venv-dbt13/bin/pip install -e .
	.venv-dbt13/bin/pip install "dbt-core>=1.3.0,<1.4.0"

.venv-dbt14/bin/python:
	python -m venv --upgrade-deps .venv-dbt14
	.venv-dbt14/bin/pip install --upgrade wheel
	.venv-dbt14/bin/pip install -e .
	.venv-dbt14/bin/pip install "dbt-core>=1.4.0,<1.5.0"

clean:
	rm -rf .venv-dbt10 .venv-dbt11 .venv-dbt12 .venv-dbt13 .venv-dbt14
.PHONY: clean
