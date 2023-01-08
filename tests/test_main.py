"""Test cases for the project module."""
import pytest


def test_import_succeeds() -> None:
    """It exits with a status code of zero."""
    from dbt_core_interface import project

    _ = project
