import json

import pytest


@pytest.fixture(scope="session", autouse=True)
def register_dbt_project():
    import dbt_core_interface.project
    import dbt_core_interface.state
    dbt_core_interface.project.ServerPlugin = dbt_core_interface.project.DbtInterfaceServerPlugin()
    dbt_core_interface.project.install(dbt_core_interface.project.ServerPlugin)
    dbt_core_interface.project.install(dbt_core_interface.project.JSONPlugin(
        json_dumps=lambda body: json.dumps(body, default=dbt_core_interface.project.server_serializer)))

    for name_override, project_dir in [
        ("dbt_project", "tests/sqlfluff_templater/fixtures/dbt/dbt_project"),
        # This is currently not used, but it helps test that things still work
        # when multiple projects are registered.
        ("dbt_project2", "tests/sqlfluff_templater/fixtures/dbt/dbt_project2"),
    ]:
        dbt_core_interface.state.dbt_project_container.add_project(
            name_override=name_override,
            project_dir=project_dir,
            profiles_dir="tests/sqlfluff_templater/fixtures/dbt/profiles_yml",
            target="dev",
        )
