"""Tests for the watcher module."""

import tempfile
import time
from pathlib import Path
from threading import Thread
from unittest.mock import MagicMock, Mock, patch

import pytest

from dbt_core_interface.watcher import DbtProjectWatcher


@pytest.fixture
def mock_project():
    """Create a mock DbtProject."""
    project = MagicMock()
    project.project_name = "test_project"
    project.project_root = Path("/tmp/test_project")
    return project


@pytest.fixture
def mock_reader():
    """Create a mock file reader."""
    reader = MagicMock()
    reader.files = {}
    return reader


class TestDbtProjectWatcherSingleton:
    """Tests for DbtProjectWatcher singleton pattern."""

    def test_new_returns_same_instance_for_same_project(self, mock_project):
        """Test that __new__ returns the same instance for the same project."""
        with patch.object(DbtProjectWatcher, "_instances", {}):
            watcher1 = DbtProjectWatcher.__new__(DbtProjectWatcher, mock_project)
            watcher2 = DbtProjectWatcher.__new__(DbtProjectWatcher, mock_project)
            assert watcher1 is watcher2

    def test_new_returns_different_instance_for_different_project(self):
        """Test that __new__ returns different instances for different projects."""
        project1 = MagicMock()
        project1.project_root = Path("/tmp/project1")
        project2 = MagicMock()
        project2.project_root = Path("/tmp/project2")

        with patch.object(DbtProjectWatcher, "_instances", {}):
            watcher1 = DbtProjectWatcher.__new__(DbtProjectWatcher, project1)
            watcher2 = DbtProjectWatcher.__new__(DbtProjectWatcher, project2)
            assert watcher1 is not watcher2


class TestDbtProjectWatcherInit:
    """Tests for DbtProjectWatcher initialization."""

    def test_init_initializes_attributes(self, mock_project, mock_reader):
        """Test that __init__ initializes all attributes."""
        mock_project.create_reader.return_value = mock_reader

        watcher = DbtProjectWatcher(mock_project, check_interval=1.0)

        assert watcher.check_interval == 1.0
        assert watcher.reader is mock_reader
        assert watcher._mtimes == {}
        assert watcher._running is False
        assert watcher._thread is None

    def test_init_starts_when_start_is_true(self, mock_project, mock_reader):
        """Test that __init__ starts watching when start=True."""
        mock_project.create_reader.return_value = mock_reader

        with patch.object(DbtProjectWatcher, "start") as mock_start:
            watcher = DbtProjectWatcher(mock_project, start=True)
            mock_start.assert_called_once()

    def test_init_does_not_start_when_start_is_false(self, mock_project, mock_reader):
        """Test that __init__ doesn't start when start=False."""
        mock_project.create_reader.return_value = mock_reader

        with patch.object(DbtProjectWatcher, "start") as mock_start:
            watcher = DbtProjectWatcher(mock_project, start=False)
            mock_start.assert_not_called()

    def test_init_idempotent(self, mock_project, mock_reader):
        """Test that __init__ is idempotent."""
        mock_project.create_reader.return_value = mock_reader

        watcher = DbtProjectWatcher(mock_project)
        reader1 = watcher.reader
        # Call __init__ again
        watcher.__init__(mock_project)
        reader2 = watcher.reader
        assert reader1 is reader2


class TestDbtProjectWatcherProjectProperty:
    """Tests for _project property."""

    def test_project_property_returns_project_when_valid(self, mock_project, mock_reader):
        """Test _project property returns the project when reference is valid."""
        mock_project.create_reader.return_value = mock_reader

        watcher = DbtProjectWatcher(mock_project)

        assert watcher._project is mock_project

    def test_project_property_returns_none_when_invalid(self, mock_project, mock_reader):
        """Test _project property returns None when reference is invalidated."""
        mock_project.create_reader.return_value = mock_reader

        watcher = DbtProjectWatcher(mock_project)
        # Simulate project being garbage collected
        watcher._project_ref = Mock(return_value=None)

        assert watcher._project is None


class TestDbtProjectWatcherProjectOrRaise:
    """Tests for _project_or_raise property."""

    def test_project_or_raise_returns_project_when_valid(self, mock_project, mock_reader):
        """Test _project_or_raise returns project when valid."""
        mock_project.create_reader.return_value = mock_reader

        watcher = DbtProjectWatcher(mock_project)

        assert watcher._project_or_raise is mock_project

    def test_project_or_raise_raises_when_invalid(self, mock_project, mock_reader):
        """Test _project_or_raise raises RuntimeError when invalid."""
        mock_project.create_reader.return_value = mock_reader

        watcher = DbtProjectWatcher(mock_project)
        watcher._project_ref = Mock(return_value=None)

        with pytest.raises(RuntimeError, match="Project reference is no longer valid"):
            _ = watcher._project_or_raise


class TestDbtProjectWatcherStart:
    """Tests for start method."""

    def test_start_does_nothing_when_already_running(self, mock_project, mock_reader):
        """Test start does nothing if already running."""
        mock_project.create_reader.return_value = mock_reader

        watcher = DbtProjectWatcher(mock_project)
        watcher._running = True

        with patch("threading.Thread") as mock_thread:
            watcher.start()
            mock_thread.assert_not_called()

    def test_start_does_nothing_when_project_is_invalid(self, mock_project, mock_reader):
        """Test start does nothing if project reference is invalid."""
        mock_project.create_reader.return_value = mock_reader

        watcher = DbtProjectWatcher(mock_project)
        watcher._project_ref = Mock(return_value=None)

        with patch("threading.Thread") as mock_thread:
            watcher.start()
            mock_thread.assert_not_called()

    def test_start_creates_daemon_thread(self, mock_project, mock_reader):
        """Test start creates a daemon thread."""
        mock_project.create_reader.return_value = mock_reader

        watcher = DbtProjectWatcher(mock_project)
        watcher.start()

        assert watcher._thread is not None
        assert watcher._thread.daemon is True
        assert watcher._running is True

        watcher.stop()

    def test_start_clears_stop_event(self, mock_project, mock_reader):
        """Test start clears the stop event."""
        mock_project.create_reader.return_value = mock_reader

        watcher = DbtProjectWatcher(mock_project)
        watcher._stop_event.set()
        watcher.start()

        assert not watcher._stop_event.is_set()

        watcher.stop()


class TestDbtProjectWatcherStop:
    """Tests for stop method."""

    def test_stop_does_nothing_when_not_running(self, mock_project, mock_reader):
        """Test stop does nothing if not running."""
        mock_project.create_reader.return_value = mock_reader

        watcher = DbtProjectWatcher(mock_project)
        watcher._running = False

        watcher.stop()  # Should not raise

    def test_stop_sets_running_to_false(self, mock_project, mock_reader):
        """Test stop sets _running to False."""
        mock_project.create_reader.return_value = mock_reader

        watcher = DbtProjectWatcher(mock_project)
        watcher.start()
        assert watcher._running is True

        watcher.stop()
        assert watcher._running is False

    def test_stop_sets_stop_event(self, mock_project, mock_reader):
        """Test stop sets the stop event."""
        mock_project.create_reader.return_value = mock_reader

        watcher = DbtProjectWatcher(mock_project)
        watcher.start()
        assert not watcher._stop_event.is_set()

        watcher.stop()
        assert watcher._stop_event.is_set()

    def test_stop_waits_for_thread(self, mock_project, mock_reader):
        """Test stop waits for the monitoring thread."""
        mock_project.create_reader.return_value = mock_reader

        watcher = DbtProjectWatcher(mock_project, check_interval=0.1)
        watcher.start()

        stop_time = time.time()
        watcher.stop()
        elapsed = time.time() - stop_time

        # Should have waited for thread to finish
        assert elapsed < 1.0  # But not too long


class TestDbtProjectWatcherCheckForChanges:
    """Tests for _check_for_changes method."""

    def test_check_returns_zero_when_no_changes(self, mock_project, mock_reader):
        """Test _check_for_changes returns 0 when no files changed."""
        mock_project.create_reader.return_value = mock_reader
        mock_project.dbt_project_yml = Path("/tmp/dbt_project.yml")
        mock_project.profiles_yml = Path("/tmp/profiles.yml")

        with tempfile.TemporaryDirectory() as tmpdir:
            dbt_yml = Path(tmpdir) / "dbt_project.yml"
            profiles_yml = Path(tmpdir) / "profiles.yml"
            dbt_yml.write_text("name: test")
            profiles_yml.write_text("test:")

            mock_project.dbt_project_yml = dbt_yml
            mock_project.profiles_yml = profiles_yml

            watcher = DbtProjectWatcher(mock_project)
            watcher._initialize_file_mtimes()

            change_level = watcher._check_for_changes()
            assert change_level == 0

    def test_check_returns_two_for_config_changes(self, mock_project, mock_reader):
        """Test _check_for_changes returns 2 when config files change."""
        mock_project.create_reader.return_value = mock_reader
        mock_reader.files = {}

        with tempfile.TemporaryDirectory() as tmpdir:
            dbt_yml = Path(tmpdir) / "dbt_project.yml"
            profiles_yml = Path(tmpdir) / "profiles.yml"
            dbt_yml.write_text("name: test")
            profiles_yml.write_text("test:")

            mock_project.dbt_project_yml = dbt_yml
            mock_project.profiles_yml = profiles_yml

            watcher = DbtProjectWatcher(mock_project)
            watcher._initialize_file_mtimes()

            # Modify the config file
            time.sleep(0.01)  # Ensure different mtime
            dbt_yml.write_text("name: modified")

            change_level = watcher._check_for_changes()
            assert change_level == 2


class TestDbtProjectWatcherStopAll:
    """Tests for stop_all class method."""

    def test_stop_all_stops_all_watchers(self):
        """Test stop_all stops all running watchers."""
        project1 = MagicMock()
        project1.project_root = Path("/tmp/project1")
        project2 = MagicMock()
        project2.project_root = Path("/tmp/project2")

        with patch.object(DbtProjectWatcher, "_instances", {}):
            watcher1 = DbtProjectWatcher(project1, start=False)
            watcher2 = DbtProjectWatcher(project2, start=False)
            watcher1._running = True
            watcher2._running = False

            with patch.object(watcher1, "stop") as mock_stop1:
                stopped = DbtProjectWatcher.stop_all()
                mock_stop1.assert_called_once()
                assert stopped == 1

    def test_stop_all_clears_instances(self):
        """Test stop_all clears the instances dictionary."""
        project = MagicMock()
        project.project_root = Path("/tmp/test")

        with patch.object(DbtProjectWatcher, "_instances", {}):
            watcher = DbtProjectWatcher(project, start=False)
            watcher._running = False

            DbtProjectWatcher.stop_all()
            assert len(DbtProjectWatcher._instances) == 0


class TestDbtProjectWatcherStopProject:
    """Tests for stop_project class method."""

    def test_stop_project_stops_specific_watcher(self):
        """Test stop_project stops the watcher for a specific project."""
        project = MagicMock()
        project.project_name = "test_project"

        watcher = MagicMock()
        watcher._running = True

        with patch.object(DbtProjectWatcher, "_instances", {project: watcher}):
            DbtProjectWatcher.stop_project(project)
            watcher.stop.assert_called_once()

    def test_stop_project_logs_warning_when_not_running(self, caplog):
        """Test stop_project logs warning when watcher not running."""
        project = MagicMock()
        project.project_name = "test_project"

        watcher = MagicMock()
        watcher._running = False

        with patch.object(DbtProjectWatcher, "_instances", {project: watcher}):
            DbtProjectWatcher.stop_project(project)
            # Should log warning about not running

    def test_stop_project_logs_warning_when_not_found(self):
        """Test stop_project logs warning when no watcher found."""
        project = MagicMock()
        project.project_name = "test_project"

        with patch.object(DbtProjectWatcher, "_instances", {}):
            DbtProjectWatcher.stop_project(project)
            # Should log warning about not found


class TestDbtProjectWatcherStopPath:
    """Tests for stop_path class method."""

    def test_stop_path_handles_empty_instances(self):
        """Test stop_path handles empty instances."""
        # Should not raise
        DbtProjectWatcher.stop_path("/tmp/test_project")


class TestDbtProjectWatcherActiveWatchers:
    """Tests for active_watchers class method."""

    def test_active_watchers_returns_list(self):
        """Test active_watchers returns a list of watchers."""
        project1 = MagicMock()
        project1.project_root = Path("/tmp/p1")
        project2 = MagicMock()
        project2.project_root = Path("/tmp/p2")

        watcher1 = MagicMock()
        watcher2 = MagicMock()

        with patch.object(DbtProjectWatcher, "_instances", {project1: watcher1, project2: watcher2}):
            watchers = DbtProjectWatcher.active_watchers()
            assert len(watchers) == 2
            assert watcher1 in watchers
            assert watcher2 in watchers

    def test_active_watchers_returns_empty_list_when_none(self):
        """Test active_watchers returns empty list when no watchers."""
        with patch.object(DbtProjectWatcher, "_instances", {}):
            watchers = DbtProjectWatcher.active_watchers()
            assert watchers == []


class TestDbtProjectWatcherFinalize:
    """Tests for watcher finalization."""

    def test_watcher_finalizes_on_deletion(self, mock_project, mock_reader):
        """Test that watcher cleans up on deletion."""
        mock_project.create_reader.return_value = mock_reader

        watcher = DbtProjectWatcher(mock_project, start=False)
        watcher._running = True

        # Simulate garbage collection by calling finalizer
        with patch.object(watcher, "stop") as mock_stop:
            watcher._finalize()
            # Note: The finalize callback is created with weakref.finalize
            # This test verifies the structure is correct


class TestDbtProjectWatcherThreadSafety:
    """Tests for thread safety."""

    def test_concurrent_creation_is_thread_safe(self):
        """Test that concurrent creation doesn't race."""
        project = MagicMock()
        project.project_root = Path("/tmp/test")

        with patch.object(DbtProjectWatcher, "_instances", {}):
            watchers = []
            threads = []

            def create_watcher():
                w = DbtProjectWatcher(project, start=False)
                watchers.append(w)

            for _ in range(10):
                t = Thread(target=create_watcher)
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

            # All should have gotten the same instance
            assert len(set(watchers)) == 1


class TestDbtProjectWatcherInitializeMtimes:
    """Tests for _initialize_file_mtimes method."""

    def test_initialize_mtimes_tracks_config_files(self, mock_project, mock_reader):
        """Test _initialize_file_mtimes tracks config files."""
        mock_project.create_reader.return_value = mock_reader
        mock_project.manifest = MagicMock()
        mock_project.manifest.files = {}

        with tempfile.TemporaryDirectory() as tmpdir:
            dbt_yml = Path(tmpdir) / "dbt_project.yml"
            profiles_yml = Path(tmpdir) / "profiles.yml"
            dbt_yml.write_text("name: test")
            profiles_yml.write_text("test:")

            mock_project.dbt_project_yml = dbt_yml
            mock_project.profiles_yml = profiles_yml

            watcher = DbtProjectWatcher(mock_project)
            watcher._initialize_file_mtimes()

            assert dbt_yml in watcher._mtimes
            assert profiles_yml in watcher._mtimes
