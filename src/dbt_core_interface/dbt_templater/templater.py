# pyright: reportImplicitOverride=false, reportAny=false, reportMissingTypeStubs=false, reportUnknownMemberType=false
"""Specialized dbt templater for embedding in the dbt-core-interface server."""

from __future__ import annotations

import os
import os.path
import typing as t
from contextlib import suppress
from functools import cached_property

from sqlfluff.core.errors import SQLFluffSkipFile, SQLTemplaterError
from sqlfluff.core.templaters.base import (
    TemplatedFile,
    large_file_check,
)
from sqlfluff.core.templaters.jinja import JinjaTemplater

if t.TYPE_CHECKING:
    from dbt_common.semver import VersionSpecifier
    from sqlfluff.core import FluffConfig

__all__ = ["DbtTemplater"]


@t.final
class DbtTemplater(JinjaTemplater):
    """dbt templater for dbt-core-interface, based on sqlfluff-templater-dbt."""

    name: str = "dbt"

    def config_pairs(self) -> list[tuple[str, str]]:
        """Return info about the given templater for output by the cli."""
        return [("templater", self.name), ("dbt", self.dbt_version)]

    @cached_property
    def _dbt_version(self) -> VersionSpecifier:
        """Fetches the installed dbt version."""
        from dbt.version import get_installed_version

        return get_installed_version()

    @cached_property
    def dbt_version(self) -> str:
        """Gets the dbt version."""
        return self._dbt_version.to_version_string()

    @large_file_check
    def process(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        *,
        fname: str,
        in_str: str = "",
        config: FluffConfig | None = None,
        **kwargs: t.Any,
    ) -> tuple[TemplatedFile | None, list[SQLTemplaterError]]:
        """Compile a dbt model and return the compiled SQL."""
        from dbt_common.exceptions import CompilationError as DbtCompilationException

        fname_absolute_path = (
            os.path.abspath(fname) if fname not in ("stdin", "<string input>") else fname
        )
        try:
            return self._unsafe_process(fname_absolute_path, in_str, config)
        except DbtCompilationException as e:
            if e.node:
                return None, [
                    SQLTemplaterError(
                        f"dbt compilation error on file '{e.node.original_file_path}', {e.msg}"
                    )
                ]
            else:
                raise e
        except SQLTemplaterError as e:
            return None, [e]

    def _unsafe_process(  # noqa: C901
        self,
        fname: str,
        in_str: str = "",
        config: FluffConfig | None = None,
    ) -> tuple[TemplatedFile, list[SQLTemplaterError]]:
        from dbt_core_interface.container import CONTAINER

        project_path = (
            config.get_section((self.templater_selector, self.name, "project_dir"))
            if config
            else os.getcwd()
        )
        dbt_project = CONTAINER.find_project_in_tree(os.path.abspath(project_path))

        if not dbt_project:
            dbt_project = CONTAINER.create_project(project_dir=project_path)

        try:
            if fname and fname not in ("stdin", "<string input>"):
                resolved_path = os.path.relpath(
                    fname, start=os.path.abspath(dbt_project.project_root)
                )

                skip_reason = None
                for macro in dbt_project.manifest.macros.values():
                    if macro.original_file_path == resolved_path:
                        skip_reason = "a macro"

                for nodes in dbt_project.manifest.disabled.values():
                    for node in nodes:
                        if node.original_file_path == resolved_path:
                            skip_reason = "disabled"

                if skip_reason:
                    raise SQLFluffSkipFile(f"Skipped file {fname} because it is {skip_reason}")

                node = dbt_project.get_node_by_path(fname)
                if node is None:
                    raise SQLFluffSkipFile(f"Skipped file {fname} because it is not a dbt node.")

                with suppress(Exception):
                    config = dbt_project.get_sqlfluff_configuration(fname)

                def render_func(in_str: str) -> str:
                    """Render a dbt node."""
                    node_sql = node.raw_code
                    try:
                        node.raw_code = in_str
                        compile_result = dbt_project.compile_node(node)
                        return compile_result.compiled_code
                    finally:
                        node.raw_code = node_sql

            else:

                def render_func(in_str: str) -> str:
                    """Render a dbt str."""
                    compile_result = dbt_project.compile_sql(in_str)
                    return compile_result.compiled_code

        except Exception as err:
            raise SQLFluffSkipFile(
                f"Skipped file {fname} because dbt raised a fatal exception during compilation: {err!s}"
            ) from err

        if fname and fname not in ("stdin", "<string input>"):
            with open(fname) as source_dbt_model:
                source_dbt_sql = source_dbt_model.read()
        else:
            source_dbt_sql = in_str

        if not source_dbt_sql.rstrip().endswith("-%}"):
            n_trailing_newlines = len(source_dbt_sql) - len(source_dbt_sql.rstrip("\n"))
        else:
            n_trailing_newlines = 0

        raw_sliced, sliced_file, templated_sql = self.slice_file(
            source_dbt_sql,
            config=config,
            render_func=render_func,
            append_to_templated="\n" if n_trailing_newlines else "",
        )

        max_end = max(s.templated_slice.stop for s in sliced_file)
        if len(templated_sql) > max_end:
            templated_sql = templated_sql[:max_end]

        return (
            TemplatedFile(
                source_str=source_dbt_sql,
                templated_str=templated_sql,
                fname=fname,
                sliced_file=sliced_file,
                raw_sliced=raw_sliced,
            ),
            [],
        )
