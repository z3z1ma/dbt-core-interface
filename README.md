# Dbt Core Interface

[![PyPI](https://img.shields.io/pypi/v/dbt-core-interface.svg)][pypi_]
[![Status](https://img.shields.io/pypi/status/dbt-core-interface.svg)][status]
[![Python Version](https://img.shields.io/pypi/pyversions/dbt-core-interface)][python version]
[![License](https://img.shields.io/pypi/l/dbt-core-interface)][license]

[![Read the documentation at https://dbt-core-interface.readthedocs.io/](https://img.shields.io/readthedocs/dbt-core-interface/latest.svg?label=Read%20the%20Docs)][read the docs]
[![Tests](https://github.com/z3z1ma/dbt-core-interface/workflows/Tests/badge.svg)][tests]

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)][pre-commit]
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)][black]

[pypi_]: https://pypi.org/project/dbt-core-interface/
[status]: https://pypi.org/project/dbt-core-interface/
[python version]: https://pypi.org/project/dbt-core-interface
[read the docs]: https://dbt-core-interface.readthedocs.io/
[tests]: https://github.com/z3z1ma/dbt-core-interface/actions?workflow=Tests
[codecov]: https://app.codecov.io/gh/z3z1ma/dbt-core-interface
[pre-commit]: https://github.com/pre-commit/pre-commit
[black]: https://github.com/psf/black

## Features

An extremely simplified interface is provided to accomplish all of the following with no dependencies outside dbt-core:

- Parse dbt project on disk loading dbt core classes into memory from a single class/interface

- Automatic management of the adapter and thread-safe efficient connection pool reuse

- Run SQL and get results in python fully independent of the dbt adapter which automatically enables support for many databases

- Run SQL with dbt SQL from a single method call

- Load macros at runtime enabling custom functionality in third party extensions without requiring the dbt packaging system to be managed in userland

- Compile dbt jinja extremely fast and efficiently, thread-safe and stress tested at load via a Bottle server which live compiles SQL

- Manage multiple dbt projects in a single process using the DbtProjectContainer class

`dbt-core-interface` is a wrapper that allows developers to rapidly develop features and integrations for dbt. This project aims to serve as a place for the community to aggregate the best ways to interface with dbt. It is afforded a much faster iteration cycle and much more freedom due to it's independence from the dbt codebase. It is intended to act as an common library to dbt's existing APIs for developers. Implementations can land here and prove themselves out before landing in the dbt-core codebase and benefit all developers involved. Sqlfluff dbt templater, dbt-osmosis, dbt-fastapi which I am ripping out of dbt-osmosis, an impending metadata manager, a testing framework will all leverage this library. As dbt core evolves and stabilizes its python API, this project will evolve with it. This may manifest in simplification of certain methods but our goal is to maintain the API and focus on driving efficient, innovative/creative, and agile community driven integration patterns.

## Requirements

- The **only** requirement is dbt-core, tested with versions `1.0.*`, `1.1.*`, `1.2.*`, `1.3.*`

## Installation

You can install _Dbt Core Interface_ via [pip] from [PyPI]:

```console
$ pip install dbt-core-interface
```

## Usage

Please see the [Api Reference] for details.

To launch the Bottle server for live compiling dbt jinja:

    python -m dbt_core_interface.project

This will launch the server on port 8581. You can then make requests to the server, e.g.:

    curl -X POST -H "Content-Type: application/json" -H "X-dbt-Project: dbt_project" -d '{"project_dir":"/app/tests/sqlfluff_templater/fixtures/dbt/dbt_project/","profiles_dir":"/app/tests/sqlfluff_templater/fixtures/dbt/profiles_yml/","target":"dev"}' http://localhost:8581/register

You can change the server hostname and port using the `--host` and `--port` arguments.

## Contributing

Contributions are very welcome.
To learn more, see the [Contributor Guide].

## License

Distributed under the terms of the [MIT license][license],
_Dbt Core Interface_ is free and open source software.

## Issues

If you encounter any problems,
please [file an issue] along with a detailed description.

## Credits

This project was generated from [@cjolowicz]'s [Hypermodern Python Cookiecutter] template.

[@cjolowicz]: https://github.com/cjolowicz
[pypi]: https://pypi.org/
[hypermodern python cookiecutter]: https://github.com/cjolowicz/cookiecutter-hypermodern-python
[file an issue]: https://github.com/z3z1ma/dbt-core-interface/issues
[pip]: https://pip.pypa.io/

<!-- github-only -->

[license]: https://github.com/z3z1ma/dbt-core-interface/blob/main/LICENSE
[contributor guide]: https://github.com/z3z1ma/dbt-core-interface/blob/main/CONTRIBUTING.md
[api reference]: https://dbt-core-interface.readthedocs.io/en/latest/reference.html
