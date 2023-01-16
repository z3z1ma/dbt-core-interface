"""Provides an interface to manage multiple dbt projects in memory at the same time."""
import os
from collections import OrderedDict
from typing import Dict, List, Optional

from dbt_core_interface.project import DbtConfiguration, DbtProject


class DbtProjectContainer:
    """Manages multiple DbtProjects.

    A DbtProject corresponds to a single project. This interface is used
    dbt projects in a single process. It enables basic multitenant servers.
    """

    def __init__(self) -> None:
        """Initialize the container."""
        self._projects: Dict[str, DbtProject] = OrderedDict()
        self._default_project: Optional[str] = None

    def get_project(self, project_name: str) -> Optional[DbtProject]:
        """Primary interface to get a project and execute code."""
        return self._projects.get(project_name)

    def get_project_by_root_dir(self, root_dir: str) -> Optional[DbtProject]:
        """Get a project by its root directory."""
        root_dir = os.path.abspath(os.path.normpath(root_dir))
        for project in self._projects.values():
            if os.path.abspath(project.project_root) == root_dir:
                return project
        return None

    def get_default_project(self) -> Optional[DbtProject]:
        """Gets the default project which at any given time is the earliest project inserted into the container."""
        return self._projects.get(self._default_project)

    def add_project(
        self,
        target: Optional[str] = None,
        profiles_dir: Optional[str] = None,
        project_dir: Optional[str] = None,
        threads: Optional[int] = 1,
        vars: Optional[str] = "{}",
        name_override: Optional[str] = "",
    ) -> DbtProject:
        """Add a DbtProject with arguments."""
        project = DbtProject(target, profiles_dir, project_dir, threads, vars)
        project_name = name_override or project.dbt_config.project_name
        if self._default_project is None:
            self._default_project = project_name
        self._projects[project_name] = project
        return project

    def add_parsed_project(self, project: DbtProject) -> DbtProject:
        """Add an already instantiated DbtProject."""
        self._projects.setdefault(project.dbt_config.project_name, project)
        return project

    def add_project_from_args(self, config: DbtConfiguration) -> DbtProject:
        """Add a DbtProject from a DbtConfiguration."""
        project = DbtProject.from_config(config)
        self._projects.setdefault(project.dbt_config.project_name, project)
        return project

    def drop_project(self, project_name: str) -> None:
        """Drop a DbtProject."""
        project = self.get_project(project_name)
        if project is None:
            return
        # Encourage garbage collection
        project.clear_internal_caches()
        project.adapter.connections.cleanup_all()
        self._projects.pop(project_name)
        if self._default_project == project_name:
            if len(self) > 0:
                self._default_project = self._projects.keys()[0]
            else:
                self._default_project = None

    def drop_all_projects(self) -> None:
        """Drop all DbtProject's in the container."""
        self._default_project = None
        for project in self._projects:
            self.drop_project(project)

    def reparse_all_projects(self) -> None:
        """Reparse all projects."""
        for project in self:
            project.safe_parse_project()

    def registered_projects(self) -> List[str]:
        """Convenience to grab all registered project names."""
        return list(self._projects.keys())

    def __len__(self):
        """Allows len(DbtProjectContainer)."""
        return len(self._projects)

    def __getitem__(self, project: str):
        """Allows DbtProjectContainer['jaffle_shop']."""
        maybe_project = self.get_project(project)
        if maybe_project is None:
            raise KeyError(project)
        return maybe_project

    def __delitem__(self, project: str):
        """Allows del DbtProjectContainer['jaffle_shop']."""
        self.drop_project(project)

    def __iter__(self):
        """Allows project for project in DbtProjectContainer."""
        for project in self._projects:
            yield self.get_project(project)

    def __contains__(self, project):
        """Allows 'jaffle_shop' in DbtProjectContainer."""
        return project in self._projects

    def __repr__(self):
        """Canonical string representation of DbtProjectContainer instance."""
        return "\n".join(
            f"Project: {project.project_name}, Dir: {project.project_root}"
            for project in self
        )
