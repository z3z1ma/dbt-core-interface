"""write_manifest must JSON-serialize datetime/date in the manifest payload."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from dbt_core_interface.project import DbtProject, _json_default


def test_json_default_datetime_isoformat() -> None:
    assert _json_default(datetime(2024, 10, 6, 7, 35, 52)) == "2024-10-06T07:35:52"


def test_json_default_date_isoformat() -> None:
    assert _json_default(date(2024, 5, 1)) == "2024-05-01"


def test_json_default_unsupported_type_raises() -> None:
    with pytest.raises(TypeError, match="Object of type object is not JSON serializable"):
        _json_default(object())


def test_dumps_manifest_like_payload() -> None:
    payload = {
        "generated_at": datetime(2024, 10, 6, 7, 35, 52),
        "begin": date(2024, 5, 1),
    }
    encoded = json.dumps(payload, default=_json_default)
    decoded = json.loads(encoded)
    assert decoded["generated_at"] == "2024-10-06T07:35:52"
    assert decoded["begin"] == "2024-05-01"


def _project_with_writable(writable: MagicMock, tmp_path: Path) -> DbtProject:
    harness = MagicMock()
    harness.target_path = tmp_path
    harness.project_root = tmp_path
    harness.manifest.writable_manifest.return_value = writable
    return harness


def test_write_manifest_prefers_artifact_write(tmp_path: Path) -> None:
    writable = MagicMock()
    writable.write = MagicMock()
    project = _project_with_writable(writable, tmp_path)

    DbtProject.write_manifest(project)

    writable.write.assert_called_once_with(str(tmp_path / "manifest.json"))
    writable.to_dict.assert_not_called()


def test_write_manifest_fallback_serializes_datetime(tmp_path: Path) -> None:
    writable = MagicMock()
    writable.write.side_effect = TypeError("Object of type datetime is not JSON serializable")
    writable.to_dict.return_value = {
        "generated_at": datetime(2024, 10, 6, 7, 35, 52),
        "begin": date(2024, 5, 1),
    }
    project = _project_with_writable(writable, tmp_path)

    DbtProject.write_manifest(project)

    payload = json.loads((tmp_path / "manifest.json").read_text())
    assert payload["generated_at"] == "2024-10-06T07:35:52"
    assert payload["begin"] == "2024-05-01"


def test_write_manifest_fallback_unsupported_type_raises(tmp_path: Path) -> None:
    writable = MagicMock()
    writable.write.side_effect = TypeError("not serializable")
    writable.to_dict.return_value = {"bad": object()}
    project = _project_with_writable(writable, tmp_path)

    with pytest.raises(TypeError, match="is not JSON serializable"):
        DbtProject.write_manifest(project)
