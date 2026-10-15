"""Test cases for the project module."""

import logging


def test_import_succeeds() -> None:
    """A simple test to ensure that the project module can be imported.

    This implicitly guarantees both that none of the surface area of dbt-core
    imports have changed and that our import side-effects are succeeding.
    It exits with a status code of zero.
    """
    from dbt_core_interface import project

    _ = project


def test_logging_not_configured_at_import() -> None:
    """Verify logging is NOT configured at import time.

    This is critical for issue #136 - importing dbt-core-interface
    should not cause side effects like setting DEBUG log level or
    adding RichHandler to dbt's logger.
    """
    logger = logging.getLogger("dbt_core_interface.project")

    assert logger.level == 0 or logger.level == logging.NOTSET, (
        f"Logger should NOT be configured at import, but level is {logger.level}"
    )
    assert len(logger.handlers) == 0, (
        f"Logger should have NO handlers at import, but has {len(logger.handlers)}"
    )


def test_setup_logging_works() -> None:
    """Verify DbtProject.setup_logging() correctly configures logging."""
    from dbt_core_interface import DbtProject

    DbtProject.setup_logging(level=logging.DEBUG)

    logger = logging.getLogger("dbt_core_interface.project")

    assert logger.level == logging.DEBUG, (
        f"Logger level should be DEBUG after setup_logging, but is {logger.level}"
    )
    assert len(logger.handlers) > 0, "Logger should have handlers after setup_logging"
    assert any(h.__class__.__name__ == "RichHandler" for h in logger.handlers), (
        "Logger should have RichHandler after setup_logging"
    )


def test_setup_logging_idempotent() -> None:
    """Verify DbtProject.setup_logging() can be called multiple times safely."""
    from dbt_core_interface import DbtProject

    DbtProject.setup_logging(level=logging.DEBUG)
    handler_count_after_first_call = len(logging.getLogger("dbt_core_interface.project").handlers)

    DbtProject.setup_logging(level=logging.INFO)

    logger = logging.getLogger("dbt_core_interface.project")

    handler_count_after_second_call = len(logger.handlers)

    assert handler_count_after_second_call == handler_count_after_first_call, (
        "setup_logging should be idempotent - should not add duplicate handlers"
    )


def test_write_manifest_serializes_datetimes(tmp_path) -> None:
    """write_manifest must handle datetimes in the manifest (issue #142).

    A microbatch model's `begin` config is parsed into a datetime, which plain
    json.dump cannot serialize.
    """
    import datetime
    import json
    from unittest import mock

    from dbt_core_interface import DbtProject

    project = object.__new__(DbtProject)
    manifest = mock.Mock()
    manifest.writable_manifest.return_value.to_dict.return_value = {
        "nodes": {"model.p.events": {"config": {"begin": datetime.datetime(2024, 3, 1)}}}
    }
    project._manifest = manifest

    project.write_manifest(tmp_path)

    written = json.loads((tmp_path / "manifest.json").read_text())
    assert written["nodes"]["model.p.events"]["config"]["begin"] == "2024-03-01T00:00:00"
