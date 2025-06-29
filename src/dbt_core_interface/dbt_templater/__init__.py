"""Defines the hook endpoints for the dbt templater plugin."""
# pyright: reportAny=false

import typing as t

from sqlfluff.core.plugin import hookimpl

from dbt_core_interface.dbt_templater.templater import DbtTemplater


@hookimpl
def get_templaters() -> list[t.Callable[..., DbtTemplater]]:
    """Register the dbt-core-interface templater."""
    return [DbtTemplater]
