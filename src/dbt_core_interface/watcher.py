# pyright: reportImportCycles=false, reportAny=false
"""Filesystem watcher for dbt projects."""

from __future__ import annotations

import logging
import threading
import typing as t
import weakref
from pathlib import Path
from weakref import ReferenceType, WeakKeyDictionary

if t.TYPE_CHECKING:
    from dbt_core_interface.project import DbtProject

__all__ = ["DbtProjectWatcher"]

logger = logging.getLogger(__name__)


@t.final
class DbtProjectWatcher:
    """Watch dbt files for changes and automatically update the manifest."""

    _instances: WeakKeyDictionary[DbtProject, DbtProjectWatcher] = WeakKeyDictionary()
    _instance_lock: threading.Lock = threading.Lock()

    def __new__(
        cls, project: DbtProject, check_interval: float = 2.0, start: bool = False
    ) -> DbtProjectWatcher:
        """Ensure only one instance of DbtProjectWatcher per project root."""
        with cls._instance_lock:
            watcher = cls._instances.get(project)
            if not watcher:
                watcher = super().__new__(cls)
                cls._instances[project] = watcher
        return watcher

    def __init__(
        self, project: DbtProject, check_interval: float = 2.0, start: bool = False
    ) -> None:
        """Initialize the project watcher."""
        if hasattr(self, "_project"):
            return

        self._project_ref: ReferenceType[DbtProject] = weakref.ref(project)
        self.check_interval = check_interval

        self.reader = project.create_reader()

        self._mtimes: dict[Path, float] = {}
        self._running = False
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

        if start:
            self.start()

        ref = weakref.ref(self)

        def finalizer() -> None:
            """Clean up the instance when it is no longer referenced."""
            if (instance := ref()) is not None:
                instance.stop()
                with DbtProjectWatcher._instance_lock:
                    proj = instance._project_ref()
                    if proj is not None:
                        _ = DbtProjectWatcher._instances.pop(proj, None)

        self._finalize = weakref.finalize(self, finalizer)

    @property
    def _project(self) -> DbtProject | None:
        """Unmarshal the project reference."""
        return self._project_ref()

    @property
    def _project_or_raise(self) -> DbtProject:
        """Unmarshal the project reference or raise an error if invalid."""
        project = self._project_ref()
        if not project:
            raise RuntimeError("Project reference is no longer valid.")
        return project

    def start(self) -> None:
        """Start monitoring files for changes."""
        if self._running or not self._project:
            return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()
        logger.info("Project watcher started for %s", self._project_or_raise.project_root)

    def stop(self) -> None:
        """Stop monitoring files."""
        if not self._running:
            return

        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5.0)

        logger.info("Project watcher stopped for %s", self._project_or_raise.project_root)

    def _monitor_loop(self) -> None:
        """Run the main monitoring loop."""
        self._initialize_file_mtimes()
        self._project_or_raise.set_invocation_context()

        while self._running and not self._stop_event.is_set():
            if self._project is None:
                break
            try:
                change_level = self._check_for_changes()
                if change_level:
                    self._project.parse_project(
                        write_manifest=True, reparse_configuration=change_level > 1
                    )
            except Exception as e:
                logger.error(f"Error in project watcher loop: {e}")

            _ = self._stop_event.wait(self.check_interval)

    def _initialize_file_mtimes(self) -> None:
        """Initialize the file modification time tracking."""
        for f_proxy in self._project_or_raise.manifest.files.values():
            path = Path(f_proxy.path.absolute_path)
            if path.exists():
                self._mtimes[path] = path.stat().st_mtime
        self._mtimes[self._project_or_raise.dbt_project_yml] = (
            self._project_or_raise.dbt_project_yml.stat().st_mtime
        )
        self._mtimes[self._project_or_raise.profiles_yml] = (
            self._project_or_raise.profiles_yml.stat().st_mtime
        )
        logger.debug(f"Initialized tracking for {len(self._mtimes)} files")

    def _check_for_changes(self) -> int:
        """Check for changes in tracked files.

        A return value of 0 means no changes, 1 means files were added/removed, and 2 means
        a configuration file was modified (dbt_project.yml or profiles.yml).
        """
        for path in (self._project_or_raise.dbt_project_yml, self._project_or_raise.profiles_yml):
            try:
                current_mtime = path.stat().st_mtime if path.exists() else 0.0
                stamped_mtime = self._mtimes.get(path)
                if stamped_mtime is None or current_mtime > stamped_mtime:
                    self._mtimes[path] = current_mtime
                    return 2
            except OSError as e:
                logger.warning(f"Error checking file {path}: {e}")
                continue

        self.reader.read_files()

        changes_detected = 0
        for k, f_proxy in list(self.reader.files.items()):
            path = Path(f_proxy.path.absolute_path)
            try:
                if not path.exists():  # DELETED
                    _ = self._mtimes.pop(path, None)
                    _ = self.reader.files.pop(k, None)
                    changes_detected = 1

                current_mtime = path.stat().st_mtime if path.exists() else 0.0
                stamped_mtime = self._mtimes.get(path)

                if stamped_mtime is None:  # ADDED
                    changes_detected = 1
                elif current_mtime > stamped_mtime:  # CHANGED
                    changes_detected = 1

                self._mtimes[path] = current_mtime

            except OSError as e:
                logger.warning(f"Error checking file {path}: {e}")
                continue

        return changes_detected

    @classmethod
    def stop_all(cls) -> int:
        """Stop all active watchers and clear the instances."""
        stopped = 0
        with cls._instance_lock:
            for watcher in list(cls._instances.values()):
                if watcher._running:
                    watcher.stop()
                    stopped += 1
            cls._instances.clear()
            logger.info("All project watchers stopped")
        return stopped

    @classmethod
    def stop_project(cls, project: DbtProject) -> None:
        """Stop the watcher for a specific project."""
        with cls._instance_lock:
            watcher = cls._instances.pop(project, None)
            if watcher:
                if watcher._running:
                    watcher.stop()
                else:
                    logger.warning(f"Watcher for project {project.project_name} is not running.")
            else:
                logger.warning(f"No watcher found for project {project.project_name}")

    @classmethod
    def stop_path(cls, path: Path | str) -> None:
        """Stop the watcher for a specific project path."""
        with cls._instance_lock:
            path = Path(path).expanduser().resolve()
            for project in (
                watch._project for watch in list(cls._instances.values()) if watch._project
            ):
                project_path = project.project_root
                if path == project_path or project_path in path.parents:
                    watcher = cls._instances.pop(project)
                    watcher.stop()
                else:
                    logger.warning(f"No watcher found for project at {path}")

    @classmethod
    def active_watchers(cls) -> list[DbtProjectWatcher]:
        """Return a list of currently active project paths being watched."""
        with cls._instance_lock:
            return list(cls._instances.values())
