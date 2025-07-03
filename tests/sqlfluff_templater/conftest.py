# pyright: reportAttributeAccessIssue=false, reportUnknownMemberType=false
"""Conftest file for dbt_core_interface tests."""

import pytest


@pytest.fixture(scope="session", autouse=True)
def register_dbt_project(_worker_id: str = "master") -> None:
    """Register dbt projects for testing."""
    import dbt_core_interface.container

    container = dbt_core_interface.container.DbtProjectContainer()
    container._REFS = []  # ensure we don't gc weak references to projects

    for _, project_dir in [
        ("dbt_project", "tests/sqlfluff_templater/fixtures/dbt/dbt_project"),
        ("dbt_project2", "tests/sqlfluff_templater/fixtures/dbt/dbt_project2"),
    ]:
        proj = container.create_project(
            project_dir=project_dir,
            profiles_dir="tests/sqlfluff_templater/fixtures/dbt/profiles_yml",
            target="dev",
        )
        container._REFS.append(proj)
