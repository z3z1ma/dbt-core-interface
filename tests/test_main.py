"""Test cases for the project module."""


def test_import_succeeds() -> None:
    """A simple test to ensure that the project module can be imported.

    This implicitly guarantees both that none of the surface area of dbt-core
    imports have changed and that our import side-effects are succeeding.
    It exits with a status code of zero.
    """
    from dbt_core_interface import project

    _ = project
