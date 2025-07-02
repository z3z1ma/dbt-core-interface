<div id="top">

<!-- HEADER STYLE: CLASSIC -->

<div align="left">

<img src="https://chatgpt.com/backend-api/public_content/enc/eyJpZCI6Im1fNjg2NDkwZjkzNjU4ODE5MTgyYTg0ZDA4YTJmZDU4ZGI6ZmlsZV8wMDAwMDAwMDk4Nzg2MWY1YTAyNGVkOGZjZDQ4MDIyOCIsInRzIjoiNDg2NTA1IiwicCI6InB5aSIsInNpZyI6IjgwM2NlMDg1NzM1YTNjMjEzMzVhYWFhMzg5NDcxMThmYTgzYTE4MzhiNGVkMzRjNTNkNjMzZTVlOTg0NGU3NTAiLCJ2IjoiMCIsImdpem1vX2lkIjpudWxsfQ==" width="30%" style="position: relative; top: 0; right: 0;" alt="Project Logo"/>

# DBT-CORE-INTERFACE

<em>Lightweight, thread-safe, multi-project Python interface to dbt-core</em>

<!-- BADGES -->

<img src="https://img.shields.io/github/license/z3z1ma/dbt-core-interface?style=flat-square&logo=opensourceinitiative&logoColor=white&color=0080ff" alt="license">
<img src="https://img.shields.io/github/last-commit/z3z1ma/dbt-core-interface?style=flat-square&logo=git&logoColor=white&color=0080ff" alt="last-commit">
<img src="https://img.shields.io/github/languages/top/z3z1ma/dbt-core-interface?style=flat-square&color=0080ff" alt="repo-top-language">
<img src="https://img.shields.io/github/languages/count/z3z1ma/dbt-core-interface?style=flat-square&color=0080ff" alt="repo-language-count">

<em>Built with the tools and technologies:</em>

<img src="https://img.shields.io/badge/TOML-9C4121.svg?style=flat-square&logo=TOML&logoColor=white" alt="TOML">
<img src="https://img.shields.io/badge/Rich-FAE742.svg?style=flat-square&logo=Rich&logoColor=black" alt="Rich">
<img src="https://img.shields.io/badge/Ruff-D7FF64.svg?style=flat-square&logo=Ruff&logoColor=black" alt="Ruff">
<img src="https://img.shields.io/badge/GNU%20Bash-4EAA25.svg?style=flat-square&logo=GNU-Bash&logoColor=white" alt="GNU%20Bash">
<img src="https://img.shields.io/badge/FastAPI-009688.svg?style=flat-square&logo=FastAPI&logoColor=white" alt="FastAPI">
<br>
<img src="https://img.shields.io/badge/Pytest-0A9EDC.svg?style=flat-square&logo=Pytest&logoColor=white" alt="Pytest">
<img src="https://img.shields.io/badge/Docker-2496ED.svg?style=flat-square&logo=Docker&logoColor=white" alt="Docker">
<img src="https://img.shields.io/badge/Python-3776AB.svg?style=flat-square&logo=Python&logoColor=white" alt="Python">
<img src="https://img.shields.io/badge/GitHub%20Actions-2088FF.svg?style=flat-square&logo=GitHub-Actions&logoColor=white" alt="GitHub%20Actions">
<img src="https://img.shields.io/badge/uv-DE5FE9.svg?style=flat-square&logo=uv&logoColor=white" alt="uv">
</div>

---

## Overview

`dbt-core-interface` is a lightweight, high-performance Python interface for working directly with `dbt-core` (v1.8+). It allows developers to manage and run dbt projects entirely in memory using an intuitive Python API—enabling runtime SQL compilation, macro evaluation, SQLFluff linting/formatting, and more, all through FastAPI or local usage.

It supports dynamic multi-project environments, automatic re-parsing, file watchers, and asynchronous usage. It is the foundation for more complex interfaces such as `dbt-fastapi` and is designed to rapidly prototype ideas outside the constraints of the dbt-core repo itself.

---

## Features

* 🧐 In-memory dbt-core 1.8+ interface with full `RuntimeConfig` hydration
* ⚡ Fast, thread-safe SQL compilation and execution via FastAPI
* 🔬 Interactive linting and formatting with SQLFluff
* 🌐 Live REST API server via FastAPI
* 🌍 Supports multiple projects simultaneously using `DbtProjectContainer`
* 🚀 Dynamic macro parsing, Jinja rendering, manifest manipulation
* 🔄 Background file watching for auto-reparsing
* ⚖ Direct dbt command passthrough (e.g. `run`, `test`, `docs serve`, etc.)

---

## Requirements

* Python 3.9+
* `dbt-core >= 1.8.0`

Install via PyPI:

```bash
pip install dbt-core-interface
```

---

## Usage

### Programmatic

```python
from dbt_core_interface import DbtProject

# Load your project
project = DbtProject(project_dir="/path/to/dbt_project")

# Run a simple SQL query
res = project.execute_sql("SELECT current_date AS today")
print(res.table)

# Compile SQL (but don't run it)
compiled = project.compile_sql("SELECT * FROM {{ ref('my_model') }}")
print(compiled.compiled_code)

# Execute a ref() lookup
node = project.ref("my_model")
print(node.resource_type, node.name)

# Load a source node
source = project.source("my_source", "my_table")
print(source.description)

# Incrementally parse the project
project.parse_project(write_manifest=True)

# Re-parse a specific path
project.parse_paths("models/my_model.sql")

# Compile a node from path
node = project.get_node_by_path("models/my_model.sql")
compiled = project.compile_node(node)
print(compiled.compiled_code)

# Run a dbt command programmatically
project.run()
project.test()

# SQLFluff linting
lint_result = project.lint("models/my_model.sql")
print(lint_result)

# SQLFluff formatting
success, formatted_sql = project.format("models/my_model.sql")
print(formatted_sql)

# Use the DbtProjectContainer to manage multiple projects
from dbt_core_interface import DbtProjectContainer

container = DbtProjectContainer()
container.create_project(project_dir="/path/to/dbt_project_1")
container.create_project(project_dir="/path/to/dbt_project_2")
print(container.registered_projects())
```

### Server Mode (FastAPI)

Run:

```bash
python -m dbt_core_interface.server --host 0.0.0.0 --port 8581
```

Register a project:

```bash
curl -X POST 'http://localhost:8581/register?project_dir=/your/dbt_project'
```

Compile SQL:

```bash
curl -X POST 'http://localhost:8581/compile' -H 'X-dbt-Project: /your/dbt_project' -d 'select * from my_model'
```

---

## Documentation

* 📚 [Read the Docs](https://dbt-core-interface.readthedocs.io/)
* 📖 [API Reference](https://dbt-core-interface.readthedocs.io/en/latest/reference.html)
* 🚑 [Health Check](http://localhost:8581/health)

---

## License

This project is licensed under the MIT License. See the [LICENSE](https://github.com/z3z1ma/dbt-core-interface/blob/main/LICENSE) file for more info.

---

## Acknowledgments

Thanks to the dbt-core maintainers and contributors whose work makes this project possible.
