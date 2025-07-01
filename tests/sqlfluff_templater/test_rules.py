"""Tests for the standard set of rules."""
# pyright: reportUnusedImport=false

import os

import pytest
from sqlfluff.core.config import FluffConfig
from sqlfluff.utils.testing.rules import (
    assert_rule_raises_violations_in_file,  # pyright: ignore[reportUnknownVariableType]
)

from tests.sqlfluff_templater.fixtures.dbt.templater import (  # noqa
    DBT_FLUFF_CONFIG,
    dbt_templater,
    project_dir,
)


@pytest.mark.parametrize(
    "rule,path,violations",
    [
        # Group By
        ("AM01", "models/my_new_project/select_distinct_group_by.sql", [(1, 8)]),
        # Multiple trailing newline
        ("LT12", "models/my_new_project/multiple_trailing_newline.sql", [(3, 1)]),
    ],
)
def test__rules__std_file_dbt(
    rule: str,
    path: str,
    violations: list[tuple[int, int]],
    project_dir: str,  # noqa: F811
) -> None:
    """Test linter finds the given errors in (and only in) the right places (DBT)."""
    assert_rule_raises_violations_in_file(
        rule=rule,
        fpath=os.path.join(project_dir, path),
        violations=violations,
        fluff_config=FluffConfig(configs=DBT_FLUFF_CONFIG, overrides={"rules": rule}),
    )
