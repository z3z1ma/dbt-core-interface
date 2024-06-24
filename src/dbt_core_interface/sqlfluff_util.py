import atexit
import logging
import os
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

from sqlfluff.cli.outputstream import FileOutput
from sqlfluff.core import SQLLintError, SQLTemplaterError
from sqlfluff.core.config import ConfigLoader, FluffConfig
from sqlfluff.core.errors import SQLFluffUserError

LOGGER = logging.getLogger(__name__)

class DatacovesConfigLoader(ConfigLoader):
    def load_extra_config(self, extra_config_path: str) -> Dict[str, Any]:
        """Load specified extra config."""
        if not os.path.exists(extra_config_path):
            raise SQLFluffUserError(
                f"Extra config '{extra_config_path}' does not exist."
            )

        configs: Dict[str, Any] = {}
        if extra_config_path.endswith("pyproject.toml"):
            elems = self._get_config_elems_from_toml(extra_config_path)
        else:
            elems = self._get_config_elems_from_file(extra_config_path)
        configs = self._incorporate_vals(configs, elems)

        # Store in the cache
        self._config_cache[str(extra_config_path)] = configs
        return configs

# Cache linters (up to 50 though its arbitrary)
@lru_cache(maxsize=50)
def get_linter(
    config: FluffConfig,
    stream: FileOutput,
):
    """Get linter."""
    from sqlfluff.cli.commands import get_linter_and_formatter
    return get_linter_and_formatter(config, stream)

# Cache config to prevent wasted frames
@lru_cache(maxsize=50)
def get_config(
    dbt_project_root: Path,
    extra_config_path: Optional[Path] = None,
    extra_config_mtime: Optional[datetime] = None,
    ignore_local_config: bool = False,
    require_dialect: bool = True,
    **kwargs,
) -> FluffConfig:
    """Similar to the get_config() function used by SQLFluff command line.

    The main difference (an important one!) is that it loads configuration
    starting from the dbt_project_root, rather than the current working
    directory.
    """
    overrides = {k: kwargs[k] for k in kwargs if kwargs[k] is not None}
    loader = DatacovesConfigLoader().get_global()

    # Load config at project root
    base_config = loader.load_config_up_to_path(
        path=str(dbt_project_root),
        extra_config_path=str(extra_config_path) if extra_config_path else None,
        ignore_local_config=ignore_local_config,
    )

    # Main config with overrides
    config = FluffConfig(
        configs=base_config,
        extra_config_path=str(extra_config_path) if extra_config_path else None,
        ignore_local_config=ignore_local_config,
        overrides=overrides,
        require_dialect=require_dialect,
    )

    # Silence output
    stream = FileOutput(config, os.devnull)
    atexit.register(stream.close)
    return config, stream


def lint_command(
    project_root: Path,
    sql: Union[Path, str],
    extra_config_path: Optional[Path] = None,
    ignore_local_config: bool = False,
) -> Optional[Dict]:
    """Lint specified file or SQL string.

    This is essentially a streamlined version of the SQLFluff command-line lint
    function, sqlfluff.cli.commands.lint().

    This function uses a few SQLFluff internals, but it should be relatively
    stable. The initial plan was to use the public API, but that was not
    behaving well initially. Small details about how SQLFluff handles .sqlfluff
    and dbt_project.yaml file locations and overrides generate lots of support
    questions, so it seems better to use this approach for now.

    Eventually, we can look at using SQLFluff's public, high-level APIs,
    but for now this should provide maximum compatibility with the command-line
    tool. We can also propose changes to SQLFluff to make this easier.
    """
    # Get extra_config_path last modification stamp
    extra_config_mtime = os.path.getmtime(str(extra_config_path)) if os.path.exists(str(extra_config_path)) else None
    lnt, formatter = get_linter(
        *get_config(
            project_root,
            extra_config_path,
            extra_config_mtime,
            ignore_local_config,
            require_dialect=False,
            nocolor=True,
        )
    )

    if isinstance(sql, str):
        # Lint SQL passed in as a string
        result = lnt.lint_string_wrapped(sql)
    else:
        # Lint a SQL file
        result = lnt.lint_paths(
            tuple([str(sql)]),
            ignore_files=False,
        )
    records = result.as_records()
    return records[0] if records else None


def format_command(
    project_root: Path,
    sql: Union[Path, str],
    extra_config_path: Optional[Path] = None,
    ignore_local_config: bool = False,
) -> Tuple[bool, Optional[str]]:
    """Format specified file or SQL string.

    This is essentially a streamlined version of the SQLFluff command-line
    format function, sqlfluff.cli.commands.cli_format().

    This function uses a few SQLFluff internals, but it should be relatively
    stable. The initial plan was to use the public API, but that was not
    behaving well initially. Small details about how SQLFluff handles .sqlfluff
    and dbt_project.yaml file locations and overrides generate lots of support
    questions, so it seems better to use this approach for now.

    Eventually, we can look at using SQLFluff's public, high-level APIs,
    but for now this should provide maximum compatibility with the command-line
    tool. We can also propose changes to SQLFluff to make this easier.
    """
    LOGGER.info(f"""format_command(
    {project_root},
    {str(sql)[:100]},
    {extra_config_path},
    {ignore_local_config})
""")
    extra_config_mtime = os.path.getmtime(str(extra_config_path)) if os.path.exists(str(extra_config_path)) else None

    lnt, formatter = get_linter(
        *get_config(
            project_root,
            extra_config_path,
            extra_config_mtime,
            ignore_local_config,
            require_dialect=False,
            nocolor=True,
            rules=(
                # All of the capitalisation rules
                "capitalisation,"
                # All of the layout rules
                "layout,"
                # Safe rules from other groups
                "ambiguous.union,"
                "convention.not_equal,"
                "convention.coalesce,"
                "convention.select_trailing_comma,"
                "convention.is_null,"
                "jinja.padding,"
                "structure.distinct,"
            )
        )
    )

    if isinstance(sql, str):
        # Lint SQL passed in as a string
        LOGGER.info(f"Formatting SQL string: {sql[:100]}")
        result = lnt.lint_string_wrapped(sql, fname="stdin", fix=True)
        total_errors, num_filtered_errors = result.count_tmp_prs_errors()
        result.discard_fixes_for_lint_errors_in_files_with_tmp_or_prs_errors()
        success = not num_filtered_errors
        num_fixable = result.num_violations(types=SQLLintError, fixable=True)
        if num_fixable > 0:
            LOGGER.info(f"Fixing {num_fixable} errors in SQL string")
            result_sql = result.paths[0].files[0].fix_string()[0]
            LOGGER.info(f"Result string has changes? {result_sql != sql}")
        else:
            LOGGER.info("No fixable errors in SQL string")
            result_sql = sql
    else:
        # Format a SQL file
        LOGGER.info(f"Formatting SQL file: {sql}")
        before_modified = datetime.fromtimestamp(sql.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
        LOGGER.info(f"Before fixing, modified: {before_modified}")
        result_sql = None
        lint_result = lnt.lint_paths(
            paths=[str(sql)],
            fix=True,
            ignore_non_existent_files=False,
            #processes=processes,
            # If --force is set, then apply the changes as we go rather
            # than waiting until the end.
            apply_fixes=True,
            #fixed_file_suffix=fixed_suffix,
            fix_even_unparsable=False,
        )
        total_errors, num_filtered_errors = lint_result.count_tmp_prs_errors()
        lint_result.discard_fixes_for_lint_errors_in_files_with_tmp_or_prs_errors()
        success = not num_filtered_errors
        if success:
            num_fixable = lint_result.num_violations(types=SQLLintError, fixable=True)
            if num_fixable > 0:
                LOGGER.info(f"Fixing {num_fixable} errors in SQL file")
                res = lint_result.persist_changes(
                    formatter=formatter, fixed_file_suffix=""
                )
                after_modified = datetime.fromtimestamp(sql.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                LOGGER.info(f"After fixing, modified: {after_modified}")
                LOGGER.info(f"File modification time has changes? {before_modified != after_modified}")
                success = all(res.values())
            else:
                LOGGER.info("No fixable errors in SQL file")
    LOGGER.info(f"format_command returning success={success}, result_sql={result_sql[:100] if result_sql is not None else 'n/a'}")
    return success, result_sql


def test_lint_command():
    """Quick and dirty functional test for lint_command().

    Handy for seeing SQLFluff logs if something goes wrong. The automated tests
    make it difficult to see the logs.
    """
    logging.basicConfig(level=logging.DEBUG)
    from dbt_core_interface.project import DbtProjectContainer
    dbt = DbtProjectContainer()
    dbt.add_project(
        name_override="dbt_project",
        project_dir="tests/sqlfluff_templater/fixtures/dbt/dbt_project/",
        profiles_dir="tests/sqlfluff_templater/fixtures/dbt/profiles_yml/",
        target="dev",
    )
    sql_path = Path(
        "tests/sqlfluff_templater/fixtures/dbt/dbt_project/models/my_new_project/issue_1608.sql"
    )
    result = lint_command(
        Path("tests/sqlfluff_templater/fixtures/dbt/dbt_project"),
        sql=sql_path,
    )
    print(f"{'*'*40} Lint result {'*'*40}")
    print(result)
    print(f"{'*'*40} Lint result {'*'*40}")
    result = lint_command(
        Path("tests/sqlfluff_templater/fixtures/dbt/dbt_project"),
        sql=sql_path.read_text(),
    )
    print(f"{'*'*40} Lint result {'*'*40}")
    print(result)
    print(f"{'*'*40} Lint result {'*'*40}")


def test_format_command():
    """Quick and dirty functional test for format_command().

    Handy for seeing SQLFluff logs if something goes wrong. The automated tests
    make it difficult to see the logs.
    """
    logging.basicConfig(level=logging.DEBUG)
    from dbt_core_interface.project import DbtProjectContainer
    dbt = DbtProjectContainer()
    dbt.add_project(
        name_override="dbt_project",
        project_dir="tests/sqlfluff_templater/fixtures/dbt/dbt_project/",
        profiles_dir="tests/sqlfluff_templater/fixtures/dbt/profiles_yml/",
        target="dev",
    )
    sql_path = Path(
        "tests/sqlfluff_templater/fixtures/dbt/dbt_project/models/my_new_project/issue_1608.sql"
    )

    # Test formatting a string
    success, result_sql = format_command(
        Path("tests/sqlfluff_templater/fixtures/dbt/dbt_project"),
        sql=sql_path.read_text(),
    )
    print(f"{'*'*40} Formatting result {'*'*40}")
    print(success, result_sql)

    # Test formatting a file
    result = format_command(
        Path("tests/sqlfluff_templater/fixtures/dbt/dbt_project"),
        sql=sql_path,
    )
    print(f"{'*'*40} Formatting result {'*'*40}")
    print(result)
    print(f"{'*'*40} Formatting result {'*'*40}")


if __name__ == "__main__":
    #test_lint_command()
    test_format_command()
