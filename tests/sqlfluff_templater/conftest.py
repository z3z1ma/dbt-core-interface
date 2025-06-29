"""Conftest file for dbt_core_interface tests."""

import pytest


@pytest.fixture(scope="session", autouse=True)
def register_dbt_project() -> None:
    """Register dbt projects for testing."""
    import dbt_core_interface.project

    container = dbt_core_interface.project.DbtProjectContainer()

    for name_override, project_dir in [
        ("dbt_project", "tests/sqlfluff_templater/fixtures/dbt/dbt_project"),
        ("dbt_project2", "tests/sqlfluff_templater/fixtures/dbt/dbt_project2"),
    ]:
        _ = container.add_project(
            name_override=name_override,
            project_dir=project_dir,
            profiles_dir="tests/sqlfluff_templater/fixtures/dbt/profiles_yml",
            target="dev",
        )
