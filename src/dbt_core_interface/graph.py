#!/usr/bin/env python
# pyright: reportAny=false,reportUnknownMemberType=false,reportUnknownVariableType=false
"""Model dependency graph visualization for dbt-core-interface."""

from __future__ import annotations

import logging
import typing as t
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from dbt.contracts.graph.manifest import Manifest

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GraphNode:
    """Represents a node in the dependency graph."""

    unique_id: str
    name: str
    resource_type: str
    package_name: str
    file_path: str
    depends_on: list[str] = field(default_factory=list)
    dependents: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DependencyGraph:
    """Represents the complete dependency graph for a dbt project."""

    nodes: dict[str, GraphNode] = field(default_factory=dict)
    edges: list[tuple[str, str]] = field(default_factory=list)

    def get_upstream(self, node_id: str, depth: int = 1) -> set[str]:
        """Get all upstream nodes for a given node up to specified depth."""
        visited: set[str] = set()
        frontier = {node_id}
        for _ in range(depth):
            if not frontier:
                break
            next_frontier: set[str] = set()
            for nid in frontier:
                if nid in self.nodes:
                    for dep in self.nodes[nid].depends_on:
                        if dep not in visited:
                            visited.add(dep)
                            next_frontier.add(dep)
            frontier = next_frontier
        return visited

    def get_downstream(self, node_id: str, depth: int = 1) -> set[str]:
        """Get all downstream nodes for a given node up to specified depth."""
        visited: set[str] = set()
        frontier = {node_id}
        for _ in range(depth):
            if not frontier:
                break
            next_frontier: set[str] = set()
            for nid in frontier:
                if nid in self.nodes:
                    for dep in self.nodes[nid].dependents:
                        if dep not in visited:
                            visited.add(dep)
                            next_frontier.add(dep)
            frontier = next_frontier
        return visited

    def get_lineage(
        self, node_id: str, upstream_depth: int = 3, downstream_depth: int = 3
    ) -> dict[str, t.Any]:
        """Get the complete lineage for a node."""
        upstream = self.get_upstream(node_id, upstream_depth)
        downstream = self.get_downstream(node_id, downstream_depth)
        return {
            "node": self.nodes.get(node_id),
            "upstream": [self.nodes[nid] for nid in upstream if nid in self.nodes],
            "downstream": [self.nodes[nid] for nid in downstream if nid in self.nodes],
        }


def build_dependency_graph(
    manifest: Manifest,
    resource_types: list[str] | None = None,
    packages: list[str] | None = None,
) -> DependencyGraph:
    """Build a dependency graph from a dbt manifest.

    Args:
        manifest: The dbt manifest to build the graph from.
        resource_types: Optional list of resource types to include (e.g., ['model', 'source']).
        packages: Optional list of package names to filter by.

    Returns:
        A DependencyGraph containing all nodes and edges.

    """
    if resource_types is None:
        resource_types = ["model", "source", "snapshot", "test"]
    if packages is None:
        packages = []

    nodes: dict[str, GraphNode] = {}
    edges: list[tuple[str, str]] = []

    # Track dependents
    dependents_map: defaultdict[str, list[str]] = defaultdict(list)

    # Process all nodes in the manifest
    for node_id, node in manifest.nodes.items():
        if node.resource_type not in resource_types:
            continue
        if packages and node.package_name not in packages:
            continue

        depends_on: list[str] = []
        if hasattr(node, "depends_on") and node.depends_on:  # pyright: ignore[reportAttributeAccessIssue]
            depends_on = node.depends_on.nodes or []

        graph_node = GraphNode(
            unique_id=node_id,
            name=node.name,
            resource_type=node.resource_type,
            package_name=node.package_name,
            file_path=node.original_file_path or "",
            depends_on=depends_on,
        )
        nodes[node_id] = graph_node

        # Track edges and dependents
        for dep in depends_on:
            edges.append((dep, node_id))
            dependents_map[dep].append(node_id)

    # Also process sources
    for source_id, source in manifest.sources.items():
        if "source" not in resource_types:
            continue
        if packages and source.package_name not in packages:
            continue

        graph_node = GraphNode(
            unique_id=source_id,
            name=source.name,
            resource_type="source",
            package_name=source.package_name,
            file_path=source.original_file_path or "",
            depends_on=[],
        )
        nodes[source_id] = graph_node

    # Update dependents for each node
    updated_nodes: dict[str, GraphNode] = {}
    for node_id, node in nodes.items():
        updated_nodes[node_id] = GraphNode(
            unique_id=node.unique_id,
            name=node.name,
            resource_type=node.resource_type,
            package_name=node.package_name,
            file_path=node.file_path,
            depends_on=node.depends_on,
            dependents=dependents_map.get(node_id, []),
        )

    return DependencyGraph(nodes=updated_nodes, edges=edges)


def generate_dot_graph(
    graph: DependencyGraph,
    focus_node: str | None = None,
    upstream_depth: int = 2,
    downstream_depth: int = 2,
    format: str = "dot",
) -> str:
    """Generate a DOT format graph representation.

    Args:
        graph: The dependency graph.
        focus_node: Optional node to focus the graph on. If None, includes all nodes.
        upstream_depth: How many levels of upstream dependencies to include.
        downstream_depth: How many levels of downstream dependencies to include.
        format: Output format ('dot' for raw DOT, 'svg' for SVG wrapper).

    Returns:
        A string containing the graph in DOT format.

    """
    # Determine which nodes to include
    nodes_to_include: set[str] = set()

    if focus_node:
        lineage = graph.get_lineage(focus_node, upstream_depth, downstream_depth)
        nodes_to_include.add(focus_node)
        nodes_to_include.update(n.unique_id for n in lineage["upstream"])
        nodes_to_include.update(n.unique_id for n in lineage["downstream"])
    else:
        nodes_to_include = set(graph.nodes.keys())

    # Color coding for node types
    type_colors = {
        "model": "#4CAF50",
        "source": "#FF9800",
        "snapshot": "#2196F3",
        "test": "#9C27B0",
        "seed": "#F44336",
    }

    # Build DOT graph
    lines = ["digraph dbt_dependency_graph {"]
    lines.append("  rankdir=LR;")
    lines.append('  node [fontname="Arial", fontsize=10];')
    lines.append('  edge [fontname="Arial", fontsize=9];')

    # Add nodes
    for node_id in sorted(nodes_to_include):
        node = graph.nodes.get(node_id)
        if not node:
            continue

        color = type_colors.get(node.resource_type, "#9E9E9E")
        label = f"{node.package_name}.{node.name}"
        tooltip = f"{node.file_path}"

        # Highlight focus node
        if node_id == focus_node:
            lines.append(
                f'  "{node_id}" [label="{label}", fillcolor="{color}", style="filled", penwidth=2.0];'
            )
        else:
            lines.append(
                f'  "{node_id}" [label="{label}", fillcolor="{color}", style="filled,filled", tooltip="{tooltip}"];'
            )

    # Add edges
    for src, dst in graph.edges:
        if src in nodes_to_include and dst in nodes_to_include:
            lines.append(f'  "{src}" -> "{dst}";')

    lines.append("}")
    return "\n".join(lines)


def generate_svg_from_dot(dot_graph: str) -> str:
    """Convert DOT graph to SVG using graphviz.

    Args:
        dot_graph: The DOT format graph string.

    Returns:
        SVG string representation of the graph.

    Raises:
        ImportError: If graphviz is not installed.
        RuntimeError: If graphviz fails to render the graph.

    """
    try:
        import graphviz  # pyright: ignore[reportMissingImports]
    except ImportError as e:
        raise ImportError(
            "graphviz package is required for SVG generation. Install it with: pip install graphviz"
        ) from e

    try:
        src = graphviz.Source(dot_graph)
        svg = src.pipe(format="svg").decode("utf-8")
        # Extract just the SVG content from the full XML output
        if "<?xml" in svg:
            start = svg.find("<svg")
            end = svg.find("</svg>") + 6
            if start != -1 and end > start:
                svg = svg[start:end]
        return svg
    except Exception as e:
        raise RuntimeError(f"Failed to render SVG: {e}") from e


def export_graph(
    graph: DependencyGraph,
    output_path: Path | str,
    focus_node: str | None = None,
    upstream_depth: int = 2,
    downstream_depth: int = 2,
) -> None:
    """Export a dependency graph to a file.

    Args:
        graph: The dependency graph.
        output_path: Path to output file. Extension determines format (.svg, .png, .dot).
        focus_node: Optional node to focus the graph on.
        upstream_depth: How many levels of upstream dependencies to include.
        downstream_depth: How many levels of downstream dependencies to include.

    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    dot_graph = generate_dot_graph(
        graph,
        focus_node=focus_node,
        upstream_depth=upstream_depth,
        downstream_depth=downstream_depth,
    )

    ext = output_path.suffix.lower()
    if ext == ".dot":
        output_path.write_text(dot_graph, encoding="utf-8")
    elif ext == ".svg":
        svg = generate_svg_from_dot(dot_graph)
        output_path.write_text(svg, encoding="utf-8")
    elif ext == ".png":
        try:
            import graphviz  # pyright: ignore[reportMissingImports]
        except ImportError as e:
            raise ImportError(
                "graphviz package is required for PNG generation. "
                "Install it with: pip install graphviz"
            ) from e

        src = graphviz.Source(dot_graph)
        src.format = "png"
        src.render(filename=str(output_path.with_suffix("")), cleanup=True)
    else:
        raise ValueError(f"Unsupported output format: {ext}. Use .svg, .png, or .dot")

    logger.info(f"Exported graph to {output_path}")


def get_node_info(manifest: Manifest, node_id: str) -> dict[str, t.Any] | None:
    """Get detailed information about a specific node.

    Args:
        manifest: The dbt manifest.
        node_id: The unique ID of the node.

    Returns:
        A dictionary with node information, or None if not found.

    """
    node = manifest.nodes.get(node_id) or manifest.sources.get(node_id)
    if not node:
        return None

    depends_on: list[str] = []
    if hasattr(node, "depends_on") and node.depends_on:  # pyright: ignore[reportAttributeAccessIssue]
        depends_on = node.depends_on.nodes or []

    # Find dependents (nodes that depend on this one)
    dependents: list[str] = []
    for other_id, other_node in manifest.nodes.items():
        if hasattr(other_node, "depends_on") and other_node.depends_on:  # pyright: ignore[reportAttributeAccessIssue]
            if node_id in (other_node.depends_on.nodes or []):  # pyright: ignore[reportAttributeAccessIssue]
                dependents.append(other_id)

    return {
        "unique_id": node_id,
        "name": node.name,
        "resource_type": node.resource_type,
        "package_name": node.package_name,
        "file_path": node.original_file_path,
        "depends_on": depends_on,
        "dependents": dependents,
        "description": getattr(node, "description", ""),
    }


def list_models(manifest: Manifest, resource_type: str | None = None) -> list[dict[str, t.Any]]:
    """List all models or resources in the manifest.

    Args:
        manifest: The dbt manifest.
        resource_type: Optional resource type filter (e.g., 'model', 'source').

    Returns:
        A list of dictionaries with model information.

    """
    results = []
    for node_id, node in manifest.nodes.items():
        if resource_type and node.resource_type != resource_type:
            continue
        results.append(
            {
                "unique_id": node_id,
                "name": node.name,
                "resource_type": node.resource_type,
                "package_name": node.package_name,
                "file_path": node.original_file_path,
            }
        )
    return results
