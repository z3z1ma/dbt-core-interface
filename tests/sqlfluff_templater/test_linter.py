"""The Test file for the linter class."""
# pyright: reportUnusedImport=false

import os
import os.path

from sqlfluff.core import FluffConfig, Linter

from tests.sqlfluff_templater.fixtures.dbt.templater import (
    DBT_FLUFF_CONFIG,
    project_dir,  # noqa: F401
)


def test__linter__lint_ephemeral_3_level(project_dir: str) -> None:  # noqa: F811
    """Test linter can lint a project with 3-level ephemeral dependencies."""
    # This was previously crashing inside dbt, in a function named
    # inject_ctes_into_sql(). (issue 2671).
    conf = FluffConfig(configs=DBT_FLUFF_CONFIG)
    lntr = Linter(config=conf)
    model_file_path = os.path.join(project_dir, "models/ephemeral_3_level")
    _ = lntr.lint_path(path=model_file_path)
