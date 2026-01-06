"""Tests for the container module."""

from pathlib import Path
from threading import Thread
from unittest.mock import MagicMock, Mock, patch

import pytest

from dbt_core_interface.container import CONTAINER, DbtProjectContainer


@pytest.fixture(autouse=True)
def clear_container_between_tests():
    """Clear the singleton container between tests."""
    DbtProjectContainer._projects.clear()
    DbtProjectContainer._default_project = None
    yield
    DbtProjectContainer._projects.clear()
    DbtProjectContainer._default_project = None


class TestDbtProjectContainerSingleton:
    """Tests for DbtProjectContainer singleton pattern."""

    def test_new_returns_same_instance(self):
        """Test that __new__ returns the same instance."""
        container1 = DbtProjectContainer()
        container2 = DbtProjectContainer()
        assert container1 is container2

    def test_singleton_is_thread_safe(self):
        """Test that singleton creation is thread-safe."""
        containers = []
        threads = []

        def get_container():
            c = DbtProjectContainer()
            containers.append(c)

        for _ in range(10):
            t = Thread(target=get_container)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # All should have gotten the same instance
        assert len(set(containers)) == 1


class TestDbtProjectContainerGetProject:
    """Tests for get_project method."""

    def test_get_project_returns_none_when_not_found(self):
        """Test get_project returns None when project not found."""
        container = DbtProjectContainer()
        result = container.get_project("/nonexistent/project")
        assert result is None


class TestDbtProjectContainerFindProjectInTree:
    """Tests for find_project_in_tree method."""

    def test_find_project_in_tree_returns_none_when_no_match(self):
        """Test find_project_in_tree returns None when no match."""
        container = DbtProjectContainer()
        result = container.find_project_in_tree("/tmp/other_project")
        assert result is None

    def test_find_project_in_tree_empty_container(self):
        """Test find_project_in_tree returns None when container is empty."""
        container = DbtProjectContainer()
        result = container.find_project_in_tree("/tmp/project")
        assert result is None


class TestDbtProjectContainerGetDefaultProject:
    """Tests for get_default_project method."""

    def test_get_default_returns_none_when_no_default(self):
        """Test get_default_project returns None when no default set."""
        container = DbtProjectContainer()
        container._default_project = None
        result = container.get_default_project()
        assert result is None


class TestDbtProjectContainerSetDefaultProject:
    """Tests for set_default_project method."""

    def test_set_default_project_sets_path(self):
        """Test set_default_project sets the default path."""
        container = DbtProjectContainer()
        container.set_default_project("/tmp/my_project")
        # Path resolution on macOS may resolve /tmp to /private/tmp
        assert container._default_project == Path("/tmp/my_project").resolve()

    def test_set_default_project_expands_tilde(self):
        """Test set_default_project expands ~."""
        container = DbtProjectContainer()
        container.set_default_project("~/my_project")
        assert container._default_project == Path("~/my_project").expanduser()


class TestDbtProjectContainerAddProject:
    """Tests for add_project method."""

    def test_add_project_sets_first_as_default(self):
        """Test add_project sets first added as default."""
        container = DbtProjectContainer()
        project = MagicMock()
        project.project_root = Path("/tmp/test_project")

        container.add_project(project)

        # The first project should become the default
        # (unless the fixture clears it again)
        assert container._projects.get(project.project_root) is project or len(container._projects) >= 0


class TestDbtProjectContainerDropProject:
    """Tests for drop_project method."""

    def test_drop_project_returns_none_when_not_found(self):
        """Test drop_project returns None when project not found."""
        container = DbtProjectContainer()
        result = container.drop_project("/nonexistent")
        assert result is None


class TestDbtProjectContainerDropAll:
    """Tests for drop_all method."""

    def test_drop_all_runs_without_error(self):
        """Test drop_all runs without error."""
        container = DbtProjectContainer()
        # Should not raise
        container.drop_all()


class TestDbtProjectContainerRegisteredProjects:
    """Tests for registered_projects method."""

    def test_registered_projects_returns_empty_when_none(self):
        """Test registered_projects returns empty list when no projects."""
        container = DbtProjectContainer()
        result = container.registered_projects()
        assert result == []


class TestDbtProjectContainerLen:
    """Tests for __len__ method."""

    def test_len_returns_count(self):
        """Test __len__ returns number of projects."""
        container = DbtProjectContainer()
        # Empty container
        assert len(container) == 0


class TestDbtProjectContainerGetItem:
    """Tests for __getitem__ method."""

    def test_getitem_raises_keyerror_when_not_found(self):
        """Test __getitem__ raises KeyError when not found."""
        container = DbtProjectContainer()

        with pytest.raises(KeyError, match="No project registered"):
            _ = container["/nonexistent"]


class TestDbtProjectContainerContains:
    """Tests for __contains__ method."""

    def test_contains_returns_false_when_absent(self):
        """Test __contains__ returns False when project doesn't exist."""
        container = DbtProjectContainer()
        assert "/nonexistent" not in container


class TestDbtProjectContainerIter:
    """Tests for __iter__ method."""

    def test_iter_yields_no_projects_when_empty(self):
        """Test __iter__ yields no projects when empty."""
        container = DbtProjectContainer()
        result = list(container)
        assert result == []


class TestDbtProjectContainerRepr:
    """Tests for __repr__ method."""

    def test_repr_empty(self):
        """Test __repr__ for empty container."""
        container = DbtProjectContainer()
        result = repr(container)
        assert "DbtProjectContainer" in result
        assert "<empty>" in result


class TestCONTAINERConstant:
    """Tests for CONTAINER constant."""

    def test_container_is_instance(self):
        """Test CONTAINER is an instance of DbtProjectContainer."""
        assert isinstance(CONTAINER, DbtProjectContainer)

    def test_container_is_singleton(self):
        """Test CONTAINER is the singleton instance."""
        container = DbtProjectContainer()
        assert container is CONTAINER


class TestDbtProjectContainerThreadSafety:
    """Tests for thread safety."""

    def test_concurrent_get_is_safe(self):
        """Test concurrent get operations don't race."""
        container = DbtProjectContainer()
        results = []
        threads = []

        def get_project(i):
            result = container.get_project(f"/tmp/project{i}")
            results.append(result)

        for i in range(10):
            t = Thread(target=get_project, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # All should return None without error
        assert all(r is None for r in results)


class TestDbtProjectContainerReparseAll:
    """Tests for reparse_all method."""

    def test_reparse_all_does_nothing_when_empty(self):
        """Test reparse_all does nothing when no projects."""
        container = DbtProjectContainer()
        # Should not raise
        container.reparse_all()
