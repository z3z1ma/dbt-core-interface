"""Defines the hook endpoints for the dbt templater plugin."""
from sqlfluff.core.plugin import hookimpl

#import dbt_osmosis.core.server_v2
from dbt_core_interface.dbt_templater.templater import OsmosisDbtTemplater


@hookimpl
def get_templaters():
    """Get templaters."""

    def create_templater(**kwargs):
        import dbt_core_interface.state
        assert dbt_core_interface.state.dbt_project_container is not None, "dbt_project_container is None"
        return OsmosisDbtTemplater(
            #dbt_project_container=dbt_osmosis.core.server_v2.app.state.dbt_project_container,
            dbt_project_container=dbt_core_interface.state.dbt_project_container,
            **kwargs
        )

    create_templater.name = OsmosisDbtTemplater.name
    return [create_templater]
