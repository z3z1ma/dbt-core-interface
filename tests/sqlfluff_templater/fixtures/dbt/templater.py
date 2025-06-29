"""Fixtures for dbt templating tests."""

import typing as t

import pytest
from sqlfluff.core import FluffConfig

from dbt_core_interface.dbt_templater.templater import DbtTemplater

DBT_FLUFF_CONFIG: dict[str, t.Any] = {
    "core": {
        "templater": "dbt",
        "dialect": "postgres",
    },
    "templater": {
        "dbt": {
            "profiles_dir": ("tests/sqlfluff_templater/fixtures/dbt/profiles_yml"),
            "project_dir": ("tests/sqlfluff_templater/fixtures/dbt/dbt_project"),
        },
    },
}


@pytest.fixture()
def profiles_dir() -> str:
    """Return the dbt profile directory."""
    return t.cast(str, DBT_FLUFF_CONFIG["templater"]["dbt"]["profiles_dir"])


@pytest.fixture()
def project_dir() -> str:
    """Return the dbt project directory."""
    return t.cast(str, DBT_FLUFF_CONFIG["templater"]["dbt"]["project_dir"])


@pytest.fixture()
def sqlfluff_config_path() -> str:
    """Return the path to the sqlfluff configuration file."""
    return "tests/sqlfluff_templater/fixtures/dbt/dbt_project/.sqlfluff"


@pytest.fixture()
def dbt_templater() -> DbtTemplater:
    """Return an instance of the DbtTemplater."""
    return t.cast(DbtTemplater, FluffConfig(overrides={"dialect": "ansi"}).get_templater("dbt"))
