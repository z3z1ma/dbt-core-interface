import difflib
import os
import shutil
import urllib.parse
from pathlib import Path

import pytest
from webtest import TestApp

import dbt_core_interface.project

from tests.sqlfluff_templater.fixtures.dbt.templater import (
    DBT_FLUFF_CONFIG,
    profiles_dir,
    project_dir,
    sqlfluff_config_path,
)  # noqa: F401

client = TestApp(dbt_core_interface.project.default_app.default)

SQL_PATH = (
    Path(DBT_FLUFF_CONFIG["templater"]["dbt"]["project_dir"])
    / "models/my_new_project/issue_1608.sql"
)


@pytest.mark.parametrize(
    "param_name, param_value",
    [
        pytest.param("sql_path", SQL_PATH, id="sql_file"),
        pytest.param(None, SQL_PATH.read_text(), id="sql_string"),
    ],
)
def test_lint(param_name, param_value, profiles_dir, project_dir, sqlfluff_config_path, caplog):
    params = {}
    kwargs = {}
    data = ''
    if param_name:
        params[param_name] = param_value
    else:
        data = param_value
    response = client.post(
        f"/lint?{urllib.parse.urlencode(params)}",
        data,
        headers={"X-dbt-Project": "dbt_project"},
        **kwargs,
    )
    assert response.status_code == 200
    response_json = response.json
    assert response_json == {
        "result": [
            {
                "code": "L003",
                "description": "Expected 2 indentations, found less than 2 "
                "[compared to line 03]",
                "line_no": 4,
                "line_pos": 6,
            },
            {
                "code": "L023",
                "description": "Single whitespace expected after 'AS' in 'WITH' " "clause.",
                "line_no": 7,
                "line_pos": 7,
            },
        ]
    }


def test_lint_error_no_sql_provided(profiles_dir, project_dir, sqlfluff_config_path, caplog):
    response = client.post(
        "/lint",
        headers={"X-dbt-Project": "dbt_project"},
        expect_errors=True,
    )
    assert response.status_code == 400
    response_json = response.json
    assert response_json == {
        "error": {
            "data": {},
            "message": "No SQL provided. Either provide a SQL file path or a SQL string to lint.",
        }
    }


def test_lint_parse_failure(profiles_dir, project_dir, sqlfluff_config_path, caplog):
    response = client.post(
        "/lint",
        """select
    {{ dbt_utils.star(ref("i_dont_exists")) }}
from {{ ref("me_either") }}
li""",
        headers={"X-dbt-Project": "dbt_project"},
    )
    assert response.status_code == 200
    response_json = response.json
    print(response_json)
    assert response_json == {"result": []}


@pytest.mark.parametrize(
    "param_name, param_value",
    [
        pytest.param("sql_path", SQL_PATH, id="sql_file"),
        pytest.param(None, SQL_PATH.read_text(), id="sql_string"),
    ],
)
def test_format(param_name, param_value, profiles_dir, project_dir, sqlfluff_config_path, caplog):
    if param_name:
        # Make a copy of the file and format the copy so we don't modify a file in
        # git.
        destination_path = param_value.parent / f"{param_value.stem + '_new'}{param_value.suffix}"
        shutil.copy(str(param_value), str(destination_path))
        param_value = destination_path

    params = {}
    kwargs = {}
    data = ''
    if param_name:
        # Formatting a file
        params[param_name] = param_value
        original_lines = param_value.read_text().splitlines()
    else:
        data = param_value
        original_lines = param_value.splitlines()
    response = client.post(
        f"/format?{urllib.parse.urlencode(params)}",
        data,
        headers={"X-dbt-Project": "dbt_project"},
        **kwargs,
    )
    try:
        assert response.status_code == 200

        # Compare "before and after" SQL and verify the expected changes were made.
        if param_name:
            formatted_lines = destination_path.read_text().splitlines()
        else:
            formatted_lines = response.json["sql"].splitlines()
        differ = difflib.Differ()
        diff = list(differ.compare(original_lines, formatted_lines))
        assert diff == [
            "  {{ config(materialized='view') }}",
            "  ",
            "  with cte_example as (",
            "-      select 1 as col_name",
            "? -\n",
            "+     select 1 as col_name",
            "  ),",
            "  ",
            "- final as",
            "+ final as (",
            "?         ++\n",
            "- (",
            "      select",
            "          col_name,",
            "          {{- echo('col_name') -}} as col_name2",
            "      from",
            "          cte_example",
            "  )",
            "  ",
            "  select * from final",
        ]
    finally:
        if param_name:
            os.unlink(destination_path)


@pytest.mark.parametrize(
    "param_name, param_value, clients, sample",
    [
        ("sql_path", SQL_PATH, 10, 50),
        (None, SQL_PATH.read_text(), 40, 80),
    ],
)
def test_massive_parallel_linting(param_name, param_value, clients, sample):
    import time
    from concurrent.futures import ThreadPoolExecutor

    SIMULATED_CLIENTS = clients
    LOAD_TEST_SIZE = sample

    e = ThreadPoolExecutor(max_workers=SIMULATED_CLIENTS)

    params = {}
    kwargs = {}
    data = ''
    if param_name:
        params[param_name] = param_value
    else:
        data = param_value
    print(params)

    t1 = time.perf_counter()
    resps = e.map(
        lambda i: client.post(
            f"/lint?{urllib.parse.urlencode(params)}",
            data,
            headers={"X-dbt-Project": "dbt_project"},
            **kwargs,
        ),
        range(LOAD_TEST_SIZE),
    )
    t2 = time.perf_counter()
    print(
        (t2 - t1) / LOAD_TEST_SIZE,
        f"seconds per `/lint` across {LOAD_TEST_SIZE} calls from {SIMULATED_CLIENTS} simulated clients",
    )
    e.shutdown(wait=True)
    for resp in resps:
        assert resp.status_code == 200
