#!/usr/bin/env python
# pyright: reportAny=false,reportUnreachable=false
"""Tests for dependency graph visualization."""

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dbt_core_interface.dependency_graph import (
    DependencyGraph,
    GraphDirection,
    GraphEdge,
    GraphFormat,
    GraphNode,
)


class TestGraphNode:
    """Tests for GraphNode class."""

    def test_from_manifest_node(self) -> None:
        """Test creating GraphNode from ManifestNode."""
        mock_node = MagicMock()
        mock_node.unique_id = "model.test.my_model"
        mock_node.name = "my_model"
        mock_node.resource_type = "model"
        mock_node.package_name = "test"
        mock_node.path = "models/my_model.sql"
        mock_node.original_file_path = "models/my_model.sql"

        graph_node = GraphNode.from_manifest_node(mock_node)

        assert graph_node.unique_id == "model.test.my_model"
        assert graph_node.name == "my_model"
        assert graph_node.resource_type == "model"
        assert graph_node.package == "test"
        assert graph_node.label == "my_model"

    def test_node_colors(self) -> None:
        """Test node colors by resource type."""
        model = GraphNode(
            unique_id="model.test.m",
            name="m",
            resource_type="model",
            package="test",
            path="",
            original_file_path="",
        )
        source = GraphNode(
            unique_id="source.test.s",
            name="s",
            resource_type="source",
            package="test",
            path="",
            original_file_path="",
        )
        test = GraphNode(
            unique_id="test.test.t",
            name="t",
            resource_type="test",
            package="test",
            path="",
            original_file_path="",
        )

        assert model.color == "#4299e1"  # blue
        assert source.color == "#48bb78"  # green
        assert test.color == "#ed8936"  # orange


class TestGraphEdge:
    """Tests for GraphEdge class."""

    def test_to_dot(self) -> None:
        """Test converting edge to DOT format."""
        edge = GraphEdge(source="model.test.a", target="model.test.b")
        dot = edge.to_dot()
        assert "model.test.a" in dot
        assert "model.test.b" in dot
        assert "->" in dot

    def test_edge_with_quotes_in_id(self) -> None:
        """Test edge escaping for IDs with quotes."""
        edge = GraphEdge(source='model.test"a"', target='model.test"b"')
        dot = edge.to_dot()
        assert '\\"' in dot


class TestDependencyGraph:
    """Tests for DependencyGraph class."""

    def test_add_node_and_edge(self) -> None:
        """Test adding nodes and edges to graph."""
        graph = DependencyGraph()

        node_a = GraphNode(
            unique_id="model.test.a",
            name="a",
            resource_type="model",
            package="test",
            path="",
            original_file_path="",
        )
        node_b = GraphNode(
            unique_id="model.test.b",
            name="b",
            resource_type="model",
            package="test",
            path="",
            original_file_path="",
        )

        graph.add_node(node_a)
        graph.add_node(node_b)
        graph.add_edge(GraphEdge(source="model.test.a", target="model.test.b"))

        assert len(graph.nodes) == 2
        assert len(graph.edges) == 1

    def test_to_dot(self) -> None:
        """Test converting graph to DOT format."""
        graph = DependencyGraph(direction=GraphDirection.TOP_DOWN)

        node_a = GraphNode(
            unique_id="model.test.a",
            name="a",
            resource_type="model",
            package="test",
            path="",
            original_file_path="",
        )
        node_b = GraphNode(
            unique_id="model.test.b",
            name="b",
            resource_type="source",
            package="test",
            path="",
            original_file_path="",
        )

        graph.add_node(node_a)
        graph.add_node(node_b)
        graph.add_edge(GraphEdge(source="model.test.a", target="model.test.b"))

        dot = graph.to_dot()

        assert "digraph dbt_dependency_graph" in dot
        assert "rankdir=TB" in dot
        assert "model.test.a" in dot
        assert "model.test.b" in dot
        assert "#4299e1" in dot  # model color
        assert "#48bb78" in dot  # source color

    def test_get_upstream_nodes(self) -> None:
        """Test getting upstream nodes."""
        graph = DependencyGraph()

        nodes = {
            "model.test.a": GraphNode(
                unique_id="model.test.a",
                name="a",
                resource_type="model",
                package="test",
                path="",
                original_file_path="",
            ),
            "model.test.b": GraphNode(
                unique_id="model.test.b",
                name="b",
                resource_type="model",
                package="test",
                path="",
                original_file_path="",
            ),
            "model.test.c": GraphNode(
                unique_id="model.test.c",
                name="c",
                resource_type="model",
                package="test",
                path="",
                original_file_path="",
            ),
        }

        for node in nodes.values():
            graph.add_node(node)

        # a -> b -> c
        graph.add_edge(GraphEdge(source="model.test.a", target="model.test.b"))
        graph.add_edge(GraphEdge(source="model.test.b", target="model.test.c"))

        # Upstream of c should be b and a
        upstream = graph.get_upstream_nodes("model.test.c")
        upstream_ids = {n.unique_id for n in upstream}
        assert upstream_ids == {"model.test.a", "model.test.b"}

    def test_get_downstream_nodes(self) -> None:
        """Test getting downstream nodes."""
        graph = DependencyGraph()

        nodes = {
            "model.test.a": GraphNode(
                unique_id="model.test.a",
                name="a",
                resource_type="model",
                package="test",
                path="",
                original_file_path="",
            ),
            "model.test.b": GraphNode(
                unique_id="model.test.b",
                name="b",
                resource_type="model",
                package="test",
                path="",
                original_file_path="",
            ),
            "model.test.c": GraphNode(
                unique_id="model.test.c",
                name="c",
                resource_type="model",
                package="test",
                path="",
                original_file_path="",
            ),
        }

        for node in nodes.values():
            graph.add_node(node)

        # a -> b -> c
        graph.add_edge(GraphEdge(source="model.test.a", target="model.test.b"))
        graph.add_edge(GraphEdge(source="model.test.b", target="model.test.c"))

        # Downstream of a should be b and c
        downstream = graph.get_downstream_nodes("model.test.a")
        downstream_ids = {n.unique_id for n in downstream}
        assert downstream_ids == {"model.test.b", "model.test.c"}

    def test_filter_by_depth(self) -> None:
        """Test filtering graph by depth."""
        graph = DependencyGraph()

        # Create chain: a -> b -> c -> d
        nodes = {}
        for letter in ["a", "b", "c", "d"]:
            node_id = f"model.test.{letter}"
            nodes[node_id] = GraphNode(
                unique_id=node_id,
                name=letter,
                resource_type="model",
                package="test",
                path="",
                original_file_path="",
            )
            graph.add_node(nodes[node_id])

        graph.add_edge(GraphEdge(source="model.test.a", target="model.test.b"))
        graph.add_edge(GraphEdge(source="model.test.b", target="model.test.c"))
        graph.add_edge(GraphEdge(source="model.test.c", target="model.test.d"))

        # Filter to depth 1 from b
        filtered = graph.filter_by_depth("model.test.b", depth=1, direction="both")

        # Should include b, a (upstream), c (downstream)
        assert "model.test.b" in filtered.nodes
        assert "model.test.a" in filtered.nodes
        assert "model.test.c" in filtered.nodes
        assert "model.test.d" not in filtered.nodes  # Too far

    def test_filter_by_depth_downstream_only(self) -> None:
        """Test filtering graph by depth in downstream direction only."""
        graph = DependencyGraph()

        # Create chain: a -> b -> c -> d
        nodes = {}
        for letter in ["a", "b", "c", "d"]:
            node_id = f"model.test.{letter}"
            nodes[node_id] = GraphNode(
                unique_id=node_id,
                name=letter,
                resource_type="model",
                package="test",
                path="",
                original_file_path="",
            )
            graph.add_node(nodes[node_id])

        graph.add_edge(GraphEdge(source="model.test.a", target="model.test.b"))
        graph.add_edge(GraphEdge(source="model.test.b", target="model.test.c"))
        graph.add_edge(GraphEdge(source="model.test.c", target="model.test.d"))

        # Filter to depth 1 from b, downstream only
        filtered = graph.filter_by_depth("model.test.b", depth=1, direction="downstream")

        # Should include b and c only
        assert "model.test.b" in filtered.nodes
        assert "model.test.c" in filtered.nodes
        assert "model.test.a" not in filtered.nodes  # Upstream
        assert "model.test.d" not in filtered.nodes  # Too far

    def test_get_node_statistics(self) -> None:
        """Test getting graph statistics."""
        graph = DependencyGraph()

        for letter in ["a", "b", "c"]:
            node = GraphNode(
                unique_id=f"model.test.{letter}",
                name=letter,
                resource_type="model",
                package="test",
                path="",
                original_file_path="",
            )
            graph.add_node(node)

        source = GraphNode(
            unique_id="source.test.s",
            name="s",
            resource_type="source",
            package="test",
            path="",
            original_file_path="",
        )
        graph.add_node(source)

        graph.add_edge(GraphEdge(source="model.test.a", target="model.test.b"))
        graph.add_edge(GraphEdge(source="source.test.s", target="model.test.a"))

        stats = graph.get_node_statistics()

        assert stats["total_nodes"] == 4
        assert stats["total_edges"] == 2
        assert stats["resource_type_counts"]["model"] == 3
        assert stats["resource_type_counts"]["source"] == 1

    def test_save_dot(self) -> None:
        """Test saving DOT file."""
        graph = DependencyGraph()

        node = GraphNode(
            unique_id="model.test.a",
            name="a",
            resource_type="model",
            package="test",
            path="",
            original_file_path="",
        )
        graph.add_node(node)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.dot"
            result = graph.save_dot(output_path)

            assert result == output_path
            assert result.exists()
            content = result.read_text()
            assert "digraph dbt_dependency_graph" in content

    def test_save_dot_adds_extension(self) -> None:
        """Test that save_dot adds .dot extension if missing."""
        graph = DependencyGraph()

        node = GraphNode(
            unique_id="model.test.a",
            name="a",
            resource_type="model",
            package="test",
            path="",
            original_file_path="",
        )
        graph.add_node(node)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test"
            result = graph.save_dot(output_path)

            assert result.suffix == ".dot"
            assert result.exists()

    @patch("subprocess.run")
    def test_render_svg_with_graphviz(self, mock_run: MagicMock) -> None:
        """Test rendering SVG with graphviz."""
        # Mock successful dot -V check
        mock_run.side_effect = [
            MagicMock(returncode=0),  # dot -V
            MagicMock(returncode=0),  # dot -Tsvg
        ]

        graph = DependencyGraph()

        node = GraphNode(
            unique_id="model.test.a",
            name="a",
            resource_type="model",
            package="test",
            path="",
            original_file_path="",
        )
        graph.add_node(node)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.svg"
            result = graph.render_svg(output_path)

            assert result == output_path
            assert mock_run.call_count >= 1

    @patch("subprocess.run")
    def test_render_fails_without_graphviz(self, mock_run: MagicMock) -> None:
        """Test that rendering fails gracefully without graphviz."""
        # Mock dot -V failure
        mock_run.side_effect = FileNotFoundError("dot not found")

        graph = DependencyGraph()

        node = GraphNode(
            unique_id="model.test.a",
            name="a",
            resource_type="model",
            package="test",
            path="",
            original_file_path="",
        )
        graph.add_node(node)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.svg"
            with pytest.raises(RuntimeError) as exc_info:
                graph.render_svg(output_path)

            assert "graphviz is not installed" in str(exc_info.value)

    @patch("subprocess.run")
    def test_render_png(self, mock_run: MagicMock) -> None:
        """Test rendering PNG with graphviz."""
        mock_run.side_effect = [
            MagicMock(returncode=0),  # dot -V
            MagicMock(returncode=0),  # dot -Tpng
        ]

        graph = DependencyGraph()

        node = GraphNode(
            unique_id="model.test.a",
            name="a",
            resource_type="model",
            package="test",
            path="",
            original_file_path="",
        )
        graph.add_node(node)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.png"
            result = graph.render_png(output_path)

            assert result == output_path

    @patch("subprocess.run")
    def test_render_pdf(self, mock_run: MagicMock) -> None:
        """Test rendering PDF with graphviz."""
        mock_run.side_effect = [
            MagicMock(returncode=0),  # dot -V
            MagicMock(returncode=0),  # dot -Tpdf
        ]

        graph = DependencyGraph()

        node = GraphNode(
            unique_id="model.test.a",
            name="a",
            resource_type="model",
            package="test",
            path="",
            original_file_path="",
        )
        graph.add_node(node)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.pdf"
            result = graph.render_pdf(output_path)

            assert result == output_path

    def test_from_manifest_minimal(self) -> None:
        """Test building graph from minimal mock manifest."""
        mock_manifest = MagicMock()
        mock_manifest.nodes = {}
        mock_manifest.sources = {}

        # Add a simple model
        mock_node = MagicMock()
        mock_node.unique_id = "model.test.my_model"
        mock_node.name = "my_model"
        mock_node.resource_type = "model"
        mock_node.package_name = "test"
        mock_node.path = "models/my_model.sql"
        mock_node.original_file_path = "models/my_model.sql"
        mock_node.depends_on = MagicMock()
        mock_node.depends_on.nodes = []

        mock_manifest.nodes["model.test.my_model"] = mock_node

        graph = DependencyGraph.from_manifest(mock_manifest)

        assert len(graph.nodes) == 1
        assert "model.test.my_model" in graph.nodes
        assert len(graph.edges) == 0

    def test_from_manifest_with_dependencies(self) -> None:
        """Test building graph from manifest with dependencies."""
        mock_manifest = MagicMock()
        mock_manifest.nodes = {}
        mock_manifest.sources = {}

        # Create source
        mock_source = MagicMock()
        mock_source.unique_id = "source.test.raw"
        mock_source.name = "raw"
        mock_source.package_name = "test"
        mock_source.source_name = "raw"
        mock_source.original_file_path = "models/schema.yml"
        mock_manifest.sources["source.test.raw"] = mock_source

        # Create staging model (depends on source)
        mock_stg = MagicMock()
        mock_stg.unique_id = "model.test.stg_users"
        mock_stg.name = "stg_users"
        mock_stg.resource_type = "model"
        mock_stg.package_name = "test"
        mock_stg.path = "models/staging/stg_users.sql"
        mock_stg.original_file_path = "models/staging/stg_users.sql"
        mock_stg.depends_on = MagicMock()
        mock_stg.depends_on.nodes = ["source.test.raw"]
        mock_manifest.nodes["model.test.stg_users"] = mock_stg

        # Create final model (depends on staging)
        mock_final = MagicMock()
        mock_final.unique_id = "model.test.users"
        mock_final.name = "users"
        mock_final.resource_type = "model"
        mock_final.package_name = "test"
        mock_final.path = "models/users.sql"
        mock_final.original_file_path = "models/users.sql"
        mock_final.depends_on = MagicMock()
        mock_final.depends_on.nodes = ["model.test.stg_users"]
        mock_manifest.nodes["model.test.users"] = mock_final

        graph = DependencyGraph.from_manifest(mock_manifest)

        assert len(graph.nodes) == 3
        assert len(graph.edges) == 2
        assert "source.test.raw" in graph.nodes
        assert "model.test.stg_users" in graph.nodes
        assert "model.test.users" in graph.nodes

    def test_from_manifest_filters_by_model_name(self) -> None:
        """Test filtering graph by specific model name."""
        mock_manifest = MagicMock()
        mock_manifest.nodes = {}
        mock_manifest.sources = {}

        # Create models
        for i, name in enumerate(["a", "b", "c"]):
            mock_node = MagicMock()
            mock_node.unique_id = f"model.test.{name}"
            mock_node.name = name
            mock_node.resource_type = "model"
            mock_node.package_name = "test"
            mock_node.path = f"models/{name}.sql"
            mock_node.original_file_path = f"models/{name}.sql"
            mock_node.depends_on = MagicMock()
            # a -> b -> c
            if i > 0:
                prev = f"model.test.{chr(ord('a') + i - 1)}"
                mock_node.depends_on.nodes = [prev]
            else:
                mock_node.depends_on.nodes = []
            mock_manifest.nodes[f"model.test.{name}"] = mock_node

        graph = DependencyGraph.from_manifest(mock_manifest, model_name="b")

        # Should have filtered to b and its dependencies
        assert len(graph.nodes) <= 3  # b, a (upstream), c (downstream)
        assert "model.test.b" in graph.nodes


class TestGraphDirection:
    """Tests for GraphDirection enum."""

    def test_direction_values(self) -> None:
        """Test direction enum values."""
        assert GraphDirection.TOP_DOWN.value == "TB"
        assert GraphDirection.BOTTOM_UP.value == "BT"
        assert GraphDirection.LEFT_RIGHT.value == "LR"
        assert GraphDirection.RIGHT_LEFT.value == "RL"


class TestGraphFormat:
    """Tests for GraphFormat enum."""

    def test_format_values(self) -> None:
        """Test format enum values."""
        assert GraphFormat.SVG.value == "svg"
        assert GraphFormat.PNG.value == "png"
        assert GraphFormat.PDF.value == "pdf"
        assert GraphFormat.DOT.value == "dot"
