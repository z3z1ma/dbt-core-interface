"""Tests for the dbt templater."""
# pyright: reportUnusedImport=false, reportAny=false, reportUnknownMemberType=false

import logging
import os
import shutil
import typing as t
from pathlib import Path

import pytest
from sqlfluff.core import FluffConfig, Lexer, Linter

from dbt_core_interface.dbt_templater.templater import DbtTemplater
from tests.sqlfluff_templater.fixtures.dbt.templater import (  # noqa: F401
    DBT_FLUFF_CONFIG,
    dbt_templater,
    project_dir,
)


def test__templater_dbt_missing(dbt_templater: DbtTemplater, project_dir: str) -> None:  # noqa: F811
    """Check that a nice error is returned when dbt module is missing."""
    try:
        import dbt  # noqa: F401

        pytest.skip("dbt is installed")
    except ModuleNotFoundError:
        pass

    with pytest.raises(ModuleNotFoundError, match=r"pip install sqlfluff\[dbt\]"):
        _ = dbt_templater.process(
            in_str="",
            fname=os.path.join(project_dir, "models/my_new_project/test.sql"),
            config=FluffConfig(configs=DBT_FLUFF_CONFIG),
        )


@pytest.mark.parametrize(
    "fname",
    [
        # dbt_utils
        "use_dbt_utils.sql",
        # macro calling another macro
        "macro_in_macro.sql",
        # config.get(...)
        "use_headers.sql",
        # var(...)
        "use_var.sql",
        # {# {{ 1 + 2 }} #}
        "templated_inside_comment.sql",
        # {{ dbt_utils.last_day(
        "last_day.sql",
        # Many newlines at end, tests templater newline handling
        "trailing_newlines.sql",
        # Ends with whitespace stripping, so trailing newline handling should
        # be disabled
        "ends_with_whitespace_stripping.sql",
    ],
)
def test__templater_dbt_templating_result(
    project_dir: str,  # noqa: F811
    dbt_templater: DbtTemplater,  # noqa: F811
    fname: str,
) -> None:
    """Test that input sql file gets templated into output sql file."""
    _run_templater_and_verify_result(dbt_templater, project_dir, fname)


@pytest.mark.skip(reason="This test works when run standalone, but fails when run with all tests.")
def test_dbt_profiles_dir_env_var_uppercase(
    project_dir: str,  # noqa: F811
    dbt_templater: DbtTemplater,  # noqa: F811
    tmpdir: t.Any,
    monkeypatch: t.Any,  # noqa: F811
) -> None:
    """Tests specifying the dbt profile dir with env var."""
    profiles_dir = tmpdir.mkdir("SUBDIR")  # Use uppercase to test issue 2253
    monkeypatch.setenv("DBT_PROFILES_DIR", str(profiles_dir))
    _ = shutil.copy(os.path.join(project_dir, "../profiles_yml/profiles.yml"), str(profiles_dir))
    _run_templater_and_verify_result(dbt_templater, project_dir, "use_dbt_utils.sql")


def _run_templater_and_verify_result(
    dbt_templater: DbtTemplater,  # noqa: F811
    project_dir: str,  # noqa: F811
    fname: str,
) -> None:
    templated_file, _ = dbt_templater.process(
        in_str="",
        fname=os.path.join(project_dir, "models/my_new_project/", fname),
        config=FluffConfig(configs=DBT_FLUFF_CONFIG),
    )
    template_output_folder_path = Path("tests/sqlfluff_templater/fixtures/dbt/templated_output/")
    fixture_path = _get_fixture_path(template_output_folder_path, fname)
    assert str(templated_file) == fixture_path.read_text()


def _get_fixture_path(template_output_folder_path: Path, fname: str) -> Path:
    fixture_path = template_output_folder_path / fname
    version_specific_path = template_output_folder_path / "dbt_utils_0.8.0" / fname
    if version_specific_path.is_file():
        return version_specific_path
    return fixture_path


@pytest.mark.parametrize(
    "raw_file,templated_file,result",
    [
        (
            "select * from a",
            """
with dbt__CTE__INTERNAL_test as (
select * from a
)select count(*) from dbt__CTE__INTERNAL_test
""",
            # The unwrapper should trim the ends.
            [
                ("literal", slice(0, 15, None), slice(0, 15, None)),
            ],
        )
    ],
)
def test__templater_dbt_slice_file_wrapped_test(
    raw_file: str,
    templated_file: str,
    result: list[tuple[str, slice, slice]],
    dbt_templater: DbtTemplater,  # noqa: F811
    caplog: t.Any,  # noqa: F811
) -> None:
    """Test that wrapped queries are sliced safely using _check_for_wrapped()."""

    def _render_func(in_str: str) -> str:
        """Create a dummy render func.

        Importantly one that does actually allow different content to be added.
        """
        # find the raw location in the template for the test case.
        location = templated_file.find(raw_file)
        # replace the new content at the previous position.
        # NOTE: Doing this allows the tracer logic to do what it needs to do.
        return templated_file[:location] + in_str + templated_file[location + len(raw_file) :]

    with caplog.at_level(logging.DEBUG, logger="sqlfluff.templater"):
        _, resp, _ = dbt_templater.slice_file(
            raw_file,
            render_func=_render_func,
        )
    assert resp == result


@pytest.mark.parametrize(
    "fname",
    [
        "tests/test.sql",
        "models/my_new_project/single_trailing_newline.sql",
        "models/my_new_project/multiple_trailing_newline.sql",
    ],
)
def test__templater_dbt_templating_test_lex(
    project_dir: str,  # noqa: F811
    dbt_templater: DbtTemplater,  # noqa: F811
    fname: str,
) -> None:
    """Demonstrate the lexer works on both dbt models and dbt tests.

    Handle any number of newlines.
    """
    path = Path(project_dir) / fname
    config = FluffConfig(configs=DBT_FLUFF_CONFIG)
    source_dbt_sql = path.read_text()

    n_trailing_newlines = len(source_dbt_sql) - len(source_dbt_sql.rstrip("\n"))
    print(
        f"Loaded {path!r} (n_newlines: {n_trailing_newlines}): {source_dbt_sql!r}",
    )

    templated_file, _ = dbt_templater.process(
        in_str=source_dbt_sql,
        fname=str(path),
        config=config,
    )
    assert templated_file

    lexer = Lexer(config=config)

    _, _ = lexer.lex(templated_file)
    assert templated_file.source_str == "select a\nfrom table_a" + "\n" * n_trailing_newlines
    assert templated_file.templated_str == "select a\nfrom table_a" + "\n" * n_trailing_newlines


@pytest.mark.parametrize(
    "fname",
    [
        "use_var.sql",
        "incremental.sql",
        "single_trailing_newline.sql",
        "L034_test.sql",
    ],
)
def test__dbt_templated_models_do_not_raise_lint_error(project_dir: str, fname: str) -> None:  # noqa: F811
    """Test that templated dbt models do not raise a linting error."""
    lntr = Linter(config=FluffConfig(configs=DBT_FLUFF_CONFIG))
    lnt = lntr.lint_path(path=os.path.join(project_dir, "models/my_new_project/", fname))
    violations = t.cast(list[t.Any], lnt.check_tuples())
    assert len(violations) == 0


def test__templater_dbt_templating_absolute_path(
    project_dir: str,  # noqa: F811
    dbt_templater: DbtTemplater,  # noqa: F811
) -> None:
    """Test that absolute path of input path does not cause RuntimeError."""
    try:
        _ = dbt_templater.process(
            in_str="",
            fname=os.path.abspath(os.path.join(project_dir, "models/my_new_project/use_var.sql")),
            config=FluffConfig(configs=DBT_FLUFF_CONFIG),
        )
    except Exception as e:
        pytest.fail(f"Unexpected RuntimeError: {e}")
