import pytest


@pytest.fixture(scope="session", autouse=True)
def register_dbt_project():
    import dbt_core_interface.project
    dbt = dbt_core_interface.project.DbtProjectContainer()
    for name_override, project_dir in [
        ("dbt_project", "tests/sqlfluff_templater/fixtures/dbt/dbt_project"),
        # This is currently not used, but it helps test that things still work
        # when multiple projects are registered.
        ("dbt_project2", "tests/sqlfluff_templater/fixtures/dbt/dbt_project2"),
    ]:
        dbt.add_project(
            name_override=name_override,
            project_dir=project_dir,
            profiles_dir="tests/sqlfluff_templater/fixtures/dbt/profiles_yml",
            target="dev",
        )

        # dbt.add_project(
        #     name_override="dbt_project",
        #     project_dir="tests/sqlfluff_templater/fixtures/dbt/dbt_project/",
        #     profiles_dir="tests/sqlfluff_templater/fixtures/dbt/profiles_yml/",
        #     target="dev",
        # )
