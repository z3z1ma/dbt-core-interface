"""Defines the hook endpoints for the dbt templater plugin."""
import importlib
import logging
import sys

from sqlfluff.core.plugin import hookimpl

from dbt_core_interface.dbt_templater.templater import DCIDbtTemplater


LOGGER = logging.getLogger(__name__)


@hookimpl
def get_templaters():
    """Get templaters."""
    if importlib.util.find_spec('sqlfluff_templater_dbt'):
        LOGGER.error("sqlfluff-templater-dbt is not compatible with dbt-core-interface server. "
                       "Please uninstall it to continue.")
        sys.exit(1)


    def create_templater(**kwargs):
        import dbt_core_interface.state
        assert dbt_core_interface.state.dbt_project_container is not None, "dbt_project_container is None"
        return DCIDbtTemplater(
            dbt_project_container=dbt_core_interface.state.dbt_project_container,
            **kwargs
        )

    create_templater.name = DCIDbtTemplater.name
    return [create_templater]
