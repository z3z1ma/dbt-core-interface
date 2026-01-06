"""Basic usage examples for the Generic Test Library.

This module demonstrates how to use the Generic Test Library for common
data quality testing scenarios.
"""

from dbt_core_interface import DbtProject
from dbt_core_interface.generic_tests import (
    GenericTestConfig,
    GenericTestDefinition,
    GenericTestLibrary,
    GenericTestType,
    TestSeverity,
)


def example_basic_usage():
    """Basic example: Initialize library and list available tests."""
    # Load your dbt project
    project = DbtProject(project_dir="/path/to/dbt_project")

    # Initialize the test library
    library = GenericTestLibrary(project)

    # List all available tests
    print("Available generic tests:")
    for test in library.list_tests():
        print(f"  - {test.name}: {test.description}")

    # Get a specific test
    unique_test = library.get("unique")
    if unique_test:
        print("\nUnique test details:")
        print(f"  Required args: {unique_test.required_args}")
        print(f"  Optional args: {unique_test.optional_args}")


def example_generate_schema_yml():
    """Example: Auto-generate schema.yml with suggested tests."""
    project = DbtProject(project_dir="/path/to/dbt_project")
    library = GenericTestLibrary(project)

    # Define your model's columns
    columns = {
        "id": {"description": "Primary key"},
        "user_id": {"description": "Foreign key to users"},
        "email": {"description": "User email address"},
        "status": {"description": "Account status"},
        "created_at": {"description": "Creation timestamp"},
        "updated_at": {"description": "Last update timestamp"},
    }

    # Generate schema YAML
    schema_yml = library.generate_schema_yml("users", columns)
    print(schema_yml)

    # Output would look like:
    # version: 2
    #
    # models:
    #   - name: users
    #     columns:
    #       - name: id
    #         description: Primary key
    #         tests:
    #           - unique:
    #               users.id
    #           - not_null:
    #               users.id
    #       - name: user_id
    #         description: Foreign key to users
    #         tests:
    #           - not_null:
    #               users.user_id
    #       ...


def example_custom_test_config():
    """Example: Create and use custom test configurations."""
    # Create a custom test configuration
    config = GenericTestConfig(
        test_type=GenericTestType.ACCEPTED_VALUES,
        column_name="status",
        severity=TestSeverity.WARN,
        config={"values": ["active", "inactive", "pending"]},
    )

    # Convert to YAML
    yaml_output = config.to_yaml("users")
    print(yaml_output)

    # Output:
    #     - accepted_values:
    #         users.status
    #         severity: warn
    #         values: ['active', 'inactive', 'pending']


def example_column_suggestions():
    """Example: Get test suggestions for specific columns."""
    project = DbtProject(project_dir="/path/to/dbt_project")
    library = GenericTestLibrary(project)

    # Get suggestions for different column types
    column_names = ["id", "user_id", "email", "status", "created_at"]

    for column_name in column_names:
        suggestions = library.suggest_tests_for_column(column_name)
        print(f"\nSuggestions for '{column_name}':")
        for suggestion in suggestions:
            print(f"  - {suggestion.test_type.value}")


def example_register_custom_test():
    """Example: Register a custom generic test."""
    project = DbtProject(project_dir="/path/to/dbt_project")
    library = GenericTestLibrary(project)

    # Define a custom test
    email_format_test = GenericTestDefinition(
        name="email_format",
        description="Validates that email addresses match the expected format",
        test_type=GenericTestType.CUSTOM_SQL,
        sql_template="""
            {% set column_name = kwargs.get('column_name') %}

            SELECT {{ column_name }}
            FROM {{ this }}
            WHERE {{ column_name }} IS NOT NULL
              AND {{ column_name }} !~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
        """,
        required_args=["column_name"],
        examples=[
            {
                "description": "Validate email format",
                "config": {"column_name": "email"},
            }
        ],
    )

    # Register the test
    library.register(email_format_test)

    # Now you can use it like any other test
    config = email_format_test.render_config(column_name="email")
    print(config.to_yaml("users"))


def example_batch_generate_schemas():
    """Example: Generate schema.yml files for all models in a project."""
    project = DbtProject(project_dir="/path/to/dbt_project")
    library = GenericTestLibrary(project)

    # Get all models in the project
    # (This assumes you have a way to list models)
    model_names = ["users", "orders", "products"]  # Example

    for model_name in model_names:
        # Get the model node
        model = project.ref(model_name)
        if not model:
            continue

        # Extract column information
        columns = {name: {} for name in model.columns.keys()}

        # Generate schema YAML
        schema_yml = library.generate_schema_yml(model_name, columns)

        # Write to file (in a real scenario)
        output_path = f"models/{model_name}_schema.yml"
        with open(output_path, "w") as f:
            f.write(schema_yml)

        print(f"Generated {output_path}")


def example_integration_with_test_suggester():
    """Example: Use with TestSuggester for enhanced suggestions."""
    from dbt_core_interface.test_suggester import TestSuggester

    project = DbtProject(project_dir="/path/to/dbt_project")
    library = GenericTestLibrary(project)
    suggester = TestSuggester()

    # Get model
    model = project.ref("users")
    if not model:
        return

    # Get AI-powered suggestions
    ai_suggestions = suggester.suggest_tests_for_model(model)

    # Get pattern-based suggestions
    pattern_suggestions = []
    for col_name in model.columns.keys():
        pattern_suggestions.extend(library.suggest_tests_for_column(col_name))

    # Combine both for comprehensive coverage
    print("AI-powered suggestions:")
    for suggestion in ai_suggestions:
        print(f"  - {suggestion.test_type.value} on {suggestion.column_name}")

    print("\nPattern-based suggestions:")
    for suggestion in pattern_suggestions:
        print(f"  - {suggestion.test_type.value} on {suggestion.column_name}")


if __name__ == "__main__":
    # Run examples
    example_basic_usage()
