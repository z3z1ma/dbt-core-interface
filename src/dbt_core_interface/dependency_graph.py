#!/usr/bin/env python
# pyright: reportAny=false
"""Model dependency graph visualization for dbt-core-interface.

This module provides functionality to generate visual dependency graphs
showing model lineage (upstream/downstream relationships) with export
to SVG/PNG formats.
"""

from __future__ import annotations

import logging
import subprocess
import tempfile
import typing as t
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from dbt.contracts.graph.nodes import ManifestNode

if t.TYPE_CHECKING:
    from dbt.contracts.graph.manifest import Manifest

logger = logging.getLogger(__name__)


class GraphDirection(Enum):
    """Graph layout direction."""

    TOP_DOWN = "TB"
    BOTTOM_UP = "BT"
    LEFT_RIGHT = "LR"
    RIGHT_LEFT = "RL"


class GraphFormat(Enum):
    """Output format for graph rendering."""

    SVG = "svg"
    PNG = "png"
    PDF = "pdf"
    DOT = "dot"


@dataclass(frozen=True)
class GraphNode:
    """A node in the dependency graph."""

    unique_id: str
    name: str
    resource_type: str
    package: str
    path: str
    original_file_path: str

    @property
    def label(self) -> str:
        """Get the display label for the node."""
        return self.name

    @property
    def color(self) -> str:
        """Get the color for the node based on resource type."""
        colors = {
            "model": "#4299e1",  # blue
            "source": "#48bb78",  # green
            "test": "#ed8936",  # orange
            "snapshot": "#9f7aea",  # purple
            "analysis": "#38b2ac",  # teal
        }
        return colors.get(self.resource_type, "#a0aec0")  # gray default

    @classmethod
    def from_manifest_node(cls, node: ManifestNode) -> GraphNode:
        """Create a GraphNode from a ManifestNode."""
        return cls(
            unique_id=node.unique_id,
            name=node.name,
            resource_type=node.resource_type,
            package=node.package_name,
            path=node.path or "",
            original_file_path=node.original_file_path,
        )


@dataclass(frozen=True)
class GraphEdge:
    """An edge in the dependency graph."""

    source: str  # unique_id of source node
    target: str  # unique_id of target node

    def to_dot(self) -> str:
        """Convert edge to DOT format."""
        # Escape special characters in unique_id
        escaped_source = self.source.replace('"', '\\"')
        escaped_target = self.target.replace('"', '\\"')
        return f'  "{escaped_source}" -> "{escaped_target}";'


@dataclass
class DependencyGraph:
    """A dependency graph for dbt models showing lineage.

    Example:
        from dbt_core_interface import DbtProject

        project = DbtProject()
        graph = project.build_dependency_graph()

        # Generate full graph
        graph.render_svg("output.svg")

        # Generate for specific model with depth
        graph = project.build_dependency_graph(
            model_name="my_model",
            depth=2,
            direction="downstream"
        )
        graph.render_png("output.png")
    """

    nodes: dict[str, GraphNode] = field(default_factory=dict)
    edges: list[GraphEdge] = field(default_factory=list)
    direction: GraphDirection = GraphDirection.LEFT_RIGHT

    def add_node(self, node: GraphNode) -> None:
        """Add a node to the graph."""
        if node.unique_id not in self.nodes:
            self.nodes[node.unique_id] = node

    def add_edge(self, edge: GraphEdge) -> None:
        """Add an edge to the graph."""
        self.edges.append(edge)

    def get_upstream_nodes(self, node_id: str) -> list[GraphNode]:
        """Get all upstream (dependency) nodes for a given node."""
        upstream: set[str] = set()
        to_visit = [node_id]

        while to_visit:
            current = to_visit.pop()
            for edge in self.edges:
                if edge.target == current and edge.source not in upstream:
                    upstream.add(edge.source)
                    to_visit.append(edge.source)

        return [self.nodes[nid] for nid in upstream if nid in self.nodes]

    def get_downstream_nodes(self, node_id: str) -> list[GraphNode]:
        """Get all downstream (dependent) nodes for a given node."""
        downstream: set[str] = set()
        to_visit = [node_id]

        while to_visit:
            current = to_visit.pop()
            for edge in self.edges:
                if edge.source == current and edge.target not in downstream:
                    downstream.add(edge.target)
                    to_visit.append(edge.target)

        return [self.nodes[nid] for nid in downstream if nid in self.nodes]

    def filter_by_depth(
        self,
        center_node_id: str,
        depth: int,
        direction: t.Literal["upstream", "downstream", "both"] = "both",
    ) -> DependencyGraph:
        """Filter graph to show nodes within specified depth of center node.

        Args:
            center_node_id: The unique_id of the center node
            depth: Maximum depth from center node (0 = center node only)
            direction: Which direction to traverse
        """
        included: set[str] = {center_node_id}
        current_depth = 0

        def traverse(node_ids: set[str], current_depth: int, direction: str) -> None:
            if current_depth >= depth:
                return
            next_level: set[str] = set()

            if direction in ("upstream", "both"):
                for nid in node_ids:
                    for edge in self.edges:
                        if edge.target == nid and edge.source not in included:
                            next_level.add(edge.source)
                            included.add(edge.source)

            if direction in ("downstream", "both"):
                for nid in node_ids:
                    for edge in self.edges:
                        if edge.source == nid and edge.target not in included:
                            next_level.add(edge.target)
                            included.add(edge.target)

            if next_level:
                traverse(next_level, current_depth + 1, direction)

        traverse({center_node_id}, 0, direction)

        # Filter nodes and edges
        filtered = DependencyGraph(direction=self.direction)
        for nid in included:
            if nid in self.nodes:
                filtered.nodes[nid] = self.nodes[nid]

        for edge in self.edges:
            if edge.source in included and edge.target in included:
                filtered.edges.append(edge)

        return filtered

    def to_dot(self) -> str:
        """Convert graph to DOT format for graphviz.

        Returns:
            DOT format string representation of the graph
        """
        lines = [
            "digraph dbt_dependency_graph {",
            f"  rankdir={self.direction.value};",
            f'  node [shape=box, style=rounded, fontname="Arial", fontsize=10];',
            f'  edge [fontname="Arial", fontsize=9, color="#718096"];',
            "",
        ]

        # Add nodes
        for node in self.nodes.values():
            escaped_id = node.unique_id.replace('"', '\\"')
            lines.append(
                f'  "{escaped_id}" [label="{node.label}", '
                f'fillcolor="{node.color}", style="filled,rounded"];'
            )

        lines.append("")

        # Add edges
        for edge in self.edges:
            lines.append(edge.to_dot())

        lines.append("}")
        return "\n".join(lines)

    def _render_with_graphviz(
        self,
        output_path: Path | str,
        format: GraphFormat,
    ) -> Path:
        """Render graph using graphviz dot command.

        Args:
            output_path: Path where to save the rendered graph
            format: Output format

        Returns:
            Path to the rendered file

        Raises:
            RuntimeError: If graphviz is not installed
        """
        output_path = Path(output_path)
        if not output_path.suffix:
            output_path = output_path.parent / f"{output_path.name}.{format.value}"

        # Check if dot is available
        try:
            subprocess.run(
                ["dot", "-V"],
                check=True,
                capture_output=True,
            )
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            raise RuntimeError(
                "graphviz is not installed. Please install it:\n"
                "  - macOS: brew install graphviz\n"
                "  - Ubuntu/Debian: sudo apt-get install graphviz\n"
                "  - Windows: Download from https://graphviz.org/download/"
            ) from e

        # Write DOT to temporary file
        dot_content = self.to_dot()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".dot", delete=False) as tmp:
            tmp.write(dot_content)
            tmp_path = Path(tmp.name)

        try:
            # Run graphviz
            subprocess.run(
                ["dot", f"-T{format.value}", "-o", str(output_path), str(tmp_path)],
                check=True,
                capture_output=True,
            )
            logger.info(f"Rendered graph to {output_path}")
        finally:
            tmp_path.unlink(missing_ok=True)

        return output_path

    def render_svg(self, output_path: Path | str) -> Path:
        """Render graph to SVG format.

        Args:
            output_path: Path where to save the SVG file

        Returns:
            Path to the rendered SVG file
        """
        return self._render_with_graphviz(output_path, GraphFormat.SVG)

    def render_png(self, output_path: Path | str) -> Path:
        """Render graph to PNG format.

        Args:
            output_path: Path where to save the PNG file

        Returns:
            Path to the rendered PNG file
        """
        return self._render_with_graphviz(output_path, GraphFormat.PNG)

    def render_pdf(self, output_path: Path | str) -> Path:
        """Render graph to PDF format.

        Args:
            output_path: Path where to save the PDF file

        Returns:
            Path to the rendered PDF file
        """
        return self._render_with_graphviz(output_path, GraphFormat.PDF)

    def save_dot(self, output_path: Path | str) -> Path:
        """Save graph DOT source to file.

        Args:
            output_path: Path where to save the DOT file

        Returns:
            Path to the saved DOT file
        """
        output_path = Path(output_path)
        if not output_path.suffix or output_path.suffix != ".dot":
            output_path = output_path.parent / f"{output_path.name}.dot"

        output_path.write_text(self.to_dot(), encoding="utf-8")
        logger.info(f"Saved DOT source to {output_path}")
        return output_path

    @classmethod
    def from_manifest(
        cls,
        manifest: Manifest,
        model_name: str | None = None,
        resource_types: list[str] | None = None,
        direction: GraphDirection = GraphDirection.LEFT_RIGHT,
    ) -> DependencyGraph:
        """Build a dependency graph from a dbt manifest.

        Args:
            manifest: The dbt manifest to build graph from
            model_name: Optional model name to filter graph to
            resource_types: Optional list of resource types to include
                          (e.g., ["model", "source"])
            direction: Graph layout direction

        Returns:
            DependencyGraph instance populated from manifest
        """
        graph = cls(direction=direction)
        resource_types = resource_types or ["model", "source", "snapshot"]

        # Build nodes
        for node in manifest.nodes.values():
            if node.resource_type not in resource_types:
                continue

            if model_name and node.name != model_name:
                # Still include dependencies if filtering by model
                pass

            graph_node = GraphNode.from_manifest_node(node)
            graph.add_node(graph_node)

        # Also add sources
        for source in manifest.sources.values():
            if "source" not in resource_types:
                continue

            graph_node = GraphNode(
                unique_id=source.unique_id,
                name=source.name,
                resource_type="source",
                package=source.package_name,
                path=source.source_name or "",
                original_file_path=source.original_file_path,
            )
            graph.add_node(graph_node)

        # Build edges from dependencies
        for node in manifest.nodes.values():
            if node.resource_type not in resource_types:
                continue

            if hasattr(node, "depends_on") and node.depends_on:
                for dep in node.depends_on.nodes:
                    # Only create edge if dependency is in our nodes
                    if dep in graph.nodes:
                        edge = GraphEdge(source=dep, target=node.unique_id)
                        graph.add_edge(edge)

        # Filter to subgraph if model_name specified
        if model_name:
            # Find the model's unique_id
            model_id = None
            for nid, n in graph.nodes.items():
                if n.name == model_name and n.resource_type in resource_types:
                    model_id = nid
                    break

            if model_id:
                graph = graph.filter_by_depth(model_id, depth=10, direction="both")

        return graph

    def get_node_statistics(self) -> dict[str, t.Any]:
        """Get statistics about the graph.

        Returns:
            Dictionary with graph statistics
        """
        resource_counts: dict[str, int] = {}
        for node in self.nodes.values():
            resource_counts[node.resource_type] = resource_counts.get(node.resource_type, 0) + 1

        # Calculate connected components
        visited: set[str] = set()
        components = 0

        for node_id in self.nodes:
            if node_id not in visited:
                components += 1
                # BFS to mark all nodes in this component
                queue = [node_id]
                while queue:
                    current = queue.pop(0)
                    if current not in visited:
                        visited.add(current)
                        # Add neighbors
                        for edge in self.edges:
                            if edge.source == current and edge.target not in visited:
                                queue.append(edge.target)
                            if edge.target == current and edge.source not in visited:
                                queue.append(edge.source)

        return {
            "total_nodes": len(self.nodes),
            "total_edges": len(self.edges),
            "resource_type_counts": resource_counts,
            "connected_components": components,
        }
