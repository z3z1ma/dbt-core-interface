# pyright: reportImportCycles=false
"""State for managing dbt projects in a singleton container."""

from __future__ import annotations

import logging
import threading
import typing as t
from collections.abc import Generator
from pathlib import Path

if t.TYPE_CHECKING:
    from dbt_core_interface.project import DbtConfiguration, DbtProject

__all__ = ["DbtProjectContainer"]

logger = logging.getLogger(__name__)


@t.final
class DbtProjectContainer:
    """Singleton container for managing multiple DbtProject instances."""

    _instance: DbtProjectContainer | None = None
    _instance_lock: threading.Lock = threading.Lock()

    _projects: dict[Path, DbtProject] = {}
    _default_project: Path | None = None
    _lock = threading.RLock()

    def __new__(cls) -> DbtProjectContainer:
        """Ensure only one instance of DbtProjectContainer exists."""
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance

    def get_project(self, name: Path | str) -> DbtProject | None:
        """Return the project registered at the given Path, or None if not found."""
        with self._lock:
            return self._projects.get(Path(name).expanduser().resolve())

    def find_project_in_tree(self, path: Path | str) -> DbtProject | None:
        """Return the project whose root is at or above the given path."""
        p = Path(path).expanduser().resolve()
        with self._lock:
            for project in self._projects.values():
                root = project.project_root.resolve()
                if p == root or root in p.parents:
                    return project
        logger.debug("No project found in tree for path '%s'.", p)

    def get_default_project(self) -> DbtProject | None:
        """Return the default project (first added), or None if no projects exist."""
        with self._lock:
            if self._default_project is None:
                return None
            return self._projects.get(self._default_project)

    def set_default_project(self, path: Path | str) -> None:
        """Set the default project by name."""
        self._default_project = Path(path).expanduser().resolve()
        logger.debug("Default project set to '%s'.", self._default_project)

    def add_project(self, project: DbtProject) -> None:
        """Add a project to the container."""
        with self._lock:
            if project.project_root in self._projects:
                raise ValueError(f"Project '{project.project_root}' is already registered.")
            self._projects[project.project_root] = project
            if self._default_project is None:
                self._default_project = project.project_root
        logger.debug("Registered project '%s' at '%s'.", project.project_name, project.project_root)

    def create_project(
        self,
        target: str | None = None,
        profiles_dir: str | None = None,
        project_dir: str | None = None,
        threads: int = 1,
        vars: dict[str, t.Any] | None = None,
    ) -> DbtProject:
        """Instantiate and register a new DbtProject."""
        from dbt_core_interface.project import DbtProject

        project = DbtProject(
            target=target,
            profiles_dir=profiles_dir,
            project_dir=project_dir,
            threads=threads,
            vars=vars or {},
            autoregister=False,
        )
        self.add_project(project)
        return project

    def create_project_from_config(self, config: DbtConfiguration) -> DbtProject:
        """Instantiate a project from configuration and register it."""
        from dbt_core_interface.project import DbtProject

        project = DbtProject.from_config(config)
        self.add_project(project)
        return project

    def drop_project(self, path: Path | str) -> DbtProject | None:
        """Unregister and clean up the project with the given name."""
        with self._lock:
            project = self._projects.pop(p := Path(path).expanduser().resolve(), None)
            if project is None:
                return
            project.adapter.connections.cleanup_all()
            if p == self._default_project:
                self._default_project = next(iter(self._projects), None)
            return project

    def drop_all(self) -> None:
        """Unregister and clean up all projects."""
        with self._lock:
            logger.debug("Dropping all registered projects.")
            for name in list(self._projects):
                _ = self.drop_project(name)

    def reparse_all(self) -> None:
        """Re-parse all registered projects safely."""
        with self._lock:
            logger.debug("Re-parsing all registered projects.")
            for project in self._projects.values():
                project.parse_project()

    def registered_projects(self) -> list[Path]:
        """Return a list of registered project paths."""
        with self._lock:
            return list(self._projects.keys())

    def __len__(self) -> int:
        """Get number of registered projects."""
        return len(self._projects)

    def __getitem__(self, path: Path | str) -> DbtProject:
        """Get the project registered at the given path."""
        project = self.get_project(path)
        if project is None:
            raise KeyError(f"No project registered under '{path}'.")
        return project

    def __contains__(self, path: Path | str) -> bool:
        """Check if a project is registered at the given path."""
        return path in self._projects

    def __iter__(self) -> Generator[DbtProject, None, None]:
        """Iterate over all registered projects."""
        yield from self._projects.values()

    def __repr__(self) -> str:  # pyright: ignore[reportImplicitOverride]
        """Return a string representation of the container."""
        s = "DbtProjectContainer({})"
        if len(self._projects) == 0:
            return s.format("<empty>")
        return s.format(
            "\n  ",
            "\n  ".join(
                f"DbtProject(name={proj.project_name}, root={proj.project_root}),"
                for proj in self._projects.values()
            )
            + "\n",
        )


CONTAINER = DbtProjectContainer()
