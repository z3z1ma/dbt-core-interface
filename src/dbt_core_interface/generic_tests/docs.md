# Generic Test Library Documentation

A comprehensive library of reusable, customizable dbt generic tests for common data quality scenarios.

## Overview

The Generic Test Library provides pre-built, production-ready tests that can be easily configured via YAML schema files. These tests cover the most common data quality scenarios including null checks, uniqueness, relationships, and more.

## Available Tests

### Core Tests

#### `unique`
Ensures that a column has unique values (no duplicates).

**Parameters:**
- `column_name` (required): The column to test

**Example:**
```yaml
models:
  - name: users
    columns:
      - name: id
        tests:
          - unique
```

#### `not_null`
Ensures that a column has no null values.

**Parameters:**
- `column_name` (required): The column to test
- `severity` (optional): "error" or "warn" (default: "error")

**Example:**
```yaml
models:
  - name: users
    columns:
      - name: email
        tests:
          - not_null:
              severity: warn
```

#### `relationships`
Ensures that a column's values exist as a primary key in another table (referential integrity).

**Parameters:**
- `column_name` (required): The column to test
- `to` (required): The referenced table/model
- `field` (optional): Column name in the from table (default: same as column_name)
- `to_field` (optional): Column name in the to table (default: same as field)

**Example:**
```yaml
models:
  - name: orders
    columns:
      - name: customer_id
        tests:
          - relationships:
              to: ref('users')
              field: customer_id
              to_field: id
```

#### `accepted_values`
Ensures that a column only contains values from a specified list.

**Parameters:**
- `column_name` (required): The column to test
- `values` (required): List of accepted values
- `quote` (optional): Whether to quote values (default: true)

**Example:**
```yaml
models:
  - name: users
    columns:
      - name: status
        tests:
          - accepted_values:
              values: ['active', 'inactive', 'pending']
```

### Advanced Tests

#### `recency`
Checks that data has been updated within a specified time window.

**Parameters:**
- `column_name` (required): Timestamp column to check
- `interval` (optional): Time window (default: "24 hour")

**Example:**
```yaml
models:
  - name: orders
    tests:
      - recency:
          column_name: updated_at
          interval: "48 hour"
```

#### `cardinality_equals`
Ensures that the cardinality (distinct count) of a column matches another table.

**Parameters:**
- `column_name` (required): The column to test
- `to` (required): The referenced table/model
- `to_field` (optional): Column name in the to table (default: same as column_name)

**Example:**
```yaml
models:
  - name: fct_orders
    tests:
      - cardinality_equals:
          column_name: customer_id
          to: ref('stg_customers')
```

## Programmatic Usage

### Basic Usage

```python
from dbt_core_interface import DbtProject
from dbt_core_interface.generic_tests import GenericTestLibrary

# Load your project
project = DbtProject(project_dir="/path/to/dbt_project")

# Initialize the test library
library = GenericTestLibrary(project)

# List all available tests
for test in library.list_tests():
    print(f"{test.name}: {test.description}")
```

### Auto-Generating Tests

```python
# Get column information for a model
model = project.ref("my_model")
columns = {name: {} for name in model.columns.keys()}

# Generate schema.yml with suggested tests
schema_yml = library.generate_schema_yml("my_model", columns)
print(schema_yml)
```

### Custom Test Suggestions

```python
# Get suggestions for a specific column
suggestions = library.suggest_tests_for_column("user_id")

for suggestion in suggestions:
    print(f"Test: {suggestion.test_type.value}")
    print(f"YAML:\n{suggestion.to_yaml('my_model')}")
```

### Registering Custom Tests

```python
from dbt_core_interface.generic_tests import GenericTestDefinition, GenericTestType

# Define a custom test
custom_test = GenericTestDefinition(
    name="email_format",
    description="Validates email address format",
    test_type=GenericTestType.CUSTOM_SQL,
    sql_template="""
        SELECT email
        FROM {{ this }}
        WHERE email NOT REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
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
library.register(custom_test)
```

## Integration with DbtProject

The Generic Test Library integrates seamlessly with the DbtProject class:

```python
from dbt_core_interface import DbtProject

project = DbtProject(project_dir="/path/to/dbt_project")

# Access the built-in test library
library = project.generic_tests

# Generate tests for all models
for model_name in project.models:
    model = project.ref(model_name)
    columns = {name: {} for name in model.columns.keys()}
    schema = library.generate_schema_yml(model_name, columns)
    # Write to file...
```

## Best Practices

### 1. Test Selection
- **Primary keys**: Always test `unique` and `not_null`
- **Foreign keys**: Test `not_null` and `relationships`
- **Status columns**: Test `accepted_values`
- **Timestamps**: Consider `recency` tests for freshness

### 2. Severity Levels
Use `warn` for non-critical issues and `error` for critical data quality problems:

```yaml
columns:
  - name: middle_name
    tests:
      - not_null:
          severity: warn  # OK if missing
  - name: user_id
    tests:
      - not_null:
          severity: error  # Critical
```

### 3. Grouping Tests
Organize tests logically in your schema.yml:

```yaml
models:
  - name: users
    description: User dimension table
    columns:
      # Identification
      - name: id
        description: Primary key
        tests:
          - unique
          - not_null

      # Contact info
      - name: email
        tests:
          - unique
          - not_null

      # Classification
      - name: status
        tests:
          - accepted_values:
              values: ['active', 'inactive']
```

### 4. Performance Considerations
- Relationships tests can be expensive on large tables
- Consider using `where` clauses to limit scope:
```yaml
tests:
  - relationships:
      to: ref('users')
      where: "created_at > '2024-01-01'"
```

## Troubleshooting

### Test Failures
When a test fails:
1. Check the test output SQL
2. Run the query manually to inspect failing rows
3. Verify test parameters are correct
4. Check for data quality issues vs. test configuration issues

### Performance Issues
- Add appropriate indexes on columns used in tests
- Use `where` clauses to limit data volume
- Consider sampling for exploratory tests

### Common Pitfalls
- **Case sensitivity**: Test values are case-sensitive
- **Null handling**: Remember that `NOT IN` with NULLs returns no rows
- **Cross-database**: Ensure SQL is compatible with your warehouse

## API Reference

### GenericTestLibrary

#### Methods

- `register(definition: GenericTestDefinition)` - Register a custom test
- `get(test_name: str) -> GenericTestDefinition | None` - Get a test by name
- `list_tests() -> list[GenericTestDefinition]` - List all tests
- `suggest_tests_for_column(column_name: str, column_info: dict) -> list[GenericTestConfig]` - Get test suggestions
- `generate_schema_yml(model_name: str, columns: dict) -> str` - Generate schema YAML

### GenericTestConfig

#### Attributes
- `test_type: GenericTestType` - The type of test
- `column_name: str | None` - Column to test
- `severity: TestSeverity` - "error" or "warn"
- `config: dict` - Additional test parameters
- `enabled: bool` - Whether the test is enabled

#### Methods
- `to_yaml(model_name: str, indent: int) -> str` - Convert to YAML format

### GenericTestDefinition

#### Attributes
- `name: str` - Test name
- `description: str` - Test description
- `test_type: GenericTestType` - Type of test
- `sql_template: str` - SQL Jinja template
- `required_args: list[str]` - Required parameters
- `optional_args: dict` - Optional parameters with descriptions
- `examples: list[dict]` - Usage examples
