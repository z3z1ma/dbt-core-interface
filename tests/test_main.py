"""Test cases for the project module."""


def test_import_succeeds() -> None:
    """A simple test to ensure that the project module can be imported.

    This implicitly guarantees both that none of the surface area of dbt-core
    imports have changed and that our import side-effects are succeeding.
    It exits with a status code of zero.
    """
    from dbt_core_interface import project

    _ = project


def test_list():
    """Test the list method of the project module.

    This validates project parsing and the ability to list nodes.
    """
    from dbt_core_interface.project import DbtProject

    project = DbtProject(
        project_dir="demo_sqlite",
        profiles_dir="demo_sqlite",
        target="dev",
    )
    nodes = project.list("*")
    assert len(nodes) == 28


def test_server():
    """Some quick and dirty functional tests for the server.

    This is not a comprehensive test suite, but it does test the server's ability to
    handle multiple clients and multiple projects simultaneously as well as WSGI compliance.
    It also touches lots of the codebase, so it's a good sanity check.
    """
    import json
    import random
    import time
    from concurrent.futures import ThreadPoolExecutor

    from webtest import TestApp

    from dbt_core_interface.project import (
        DbtInterfaceServerPlugin,
        JSONPlugin,
        __dbt_major_version__,
        __dbt_minor_version__,
        app,
        install,
        server_serializer,
    )

    install(DbtInterfaceServerPlugin())
    install(JSONPlugin(json_dumps=lambda body: json.dumps(body, default=server_serializer)))
    client = TestApp(app.default)

    SIMULATED_CLIENTS = 50  # noqa: N806
    DUCKDB_PROJECTS = (  # noqa: N806
        [
            "j_shop_1_duckdb",
            "j_shop_2_duckdb",
            "h_niceserver_1_duckdb",
            "h_niceserver_2_duckdb",
        ]
        if (__dbt_major_version__, __dbt_minor_version__) < (1, 4)
        else []  # DuckDB adapter is not supported in dbt 1.4+ yet
    )
    SQLITE_PROJECTS = [  # noqa: N806
        "j_shop_1_sqlite",
        "j_shop_2_sqlite",
        "j_shop_3_sqlite",
        "j_shop_4_sqlite",
        "h_niceserver_1_sqlite",
    ]
    PROJECTS = DUCKDB_PROJECTS + SQLITE_PROJECTS  # noqa: N806

    for proj in SQLITE_PROJECTS:
        register_response = client.post(
            "/register",
            params=json.dumps(
                {
                    "project_dir": "demo_sqlite",
                    "profiles_dir": "demo_sqlite",
                    "target": "dev",
                }
            ),
            headers={"X-dbt-Project": proj},
            content_type="application/json",
            status="*",
        )
        print(register_response)
    for proj in DUCKDB_PROJECTS:
        register_response = client.post(
            "/register",
            params=json.dumps(
                {
                    "project_dir": "./demo_duckdb",
                    "profiles_dir": "./demo_duckdb",
                    "target": "dev",
                }
            ),
            headers={"X-dbt-Project": proj},
            content_type="application/json",
            status="*",
        )
        print(register_response)

    e = ThreadPoolExecutor(max_workers=SIMULATED_CLIENTS)

    STATEMENT = r"""
    {{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}}

    with orders as (

        select * from {{ ref('stg_orders') }}

    ),

    payments as (

        select * from {{ ref('stg_payments') }}

    ),

    order_payments as (

        select
            order_id,

            {{% for payment_method in payment_methods -%}}
            sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount,
            {{% endfor -%}}

            sum(amount) as total_amount

        from payments

        group by order_id

    ),

    final as (

        select
            orders.order_id,
            orders.customer_id,
            orders.order_date,
            orders.status,

            {{% for payment_method in payment_methods -%}}

            order_payments.{{ payment_method }}_amount,

            {{% endfor -%}}

            order_payments.total_amount as amount

        from orders


        left join order_payments
            on orders.order_id = order_payments.order_id

    )

    select * from final
    """  # noqa: N806
    LOAD_TEST_SIZE = 1000  # noqa: N806

    print("\n", "=" * 20, "\n")
    print("TEST /compile")
    t1 = time.perf_counter()
    futs = e.map(
        lambda i: client.post(
            "/compile",
            params=f"--> select {{{{ 1 + {i} }}}} \n{STATEMENT}",
            headers={"X-dbt-Project": random.choice(PROJECTS)},
            content_type="text/plain",
        ),
        range(LOAD_TEST_SIZE),
    )
    print("All Successful:", all(futs))
    t2 = time.perf_counter()
    print(
        (t2 - t1) / LOAD_TEST_SIZE,
        (
            f"seconds per `/compile` across {LOAD_TEST_SIZE} calls from"
            f" {SIMULATED_CLIENTS} simulated clients randomly distributed between"
            f" {len(PROJECTS)} different projects with a sql statement of ~{len(STATEMENT)} chars"
        ),
    )

    print("\n", "=" * 20, "\n")
    print("TEST /run")
    t1 = time.perf_counter()
    futs = e.map(
        lambda i: client.post(
            "/run",
            params=f"-->> select {{{{ 1 + {i} }}}} \n{STATEMENT}",
            headers={"X-dbt-Project": random.choice(PROJECTS)},
            content_type="text/plain",
        ),
        range(LOAD_TEST_SIZE),
    )
    print("All Successful:", all(futs))
    t2 = time.perf_counter()
    print(
        (t2 - t1) / LOAD_TEST_SIZE,
        (
            f"seconds per `/run` across {LOAD_TEST_SIZE} calls from {SIMULATED_CLIENTS} simulated"
            f" clients randomly distributed between {len(PROJECTS)} different projects with a sql"
            f" statement of ~{len(STATEMENT)} chars"
        ),
    )

    e.shutdown(wait=True)
