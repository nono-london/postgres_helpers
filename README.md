# postgres_helpers

PostgreSQL Connection Helper in Async, Sync and Pool modes.

## Installation

```bash
pip install postgres_helpers
```

Or install from source:
```bash
pip install -e .
```

For development:
```bash
pip install -e ".[dev]"
```

## Configuration

Set environment variables or create a `.env` file:

```env
POSTGRES_DB_HOST=localhost
POSTGRES_DB_PORT=5432
POSTGRES_DB_USER=myuser
POSTGRES_DB_PASS=mypassword
POSTGRES_DB_NAME=mydatabase
```

## Quick Start

### Async with Pool (Recommended for async applications)

```python
from postgres_helpers.postgres_async_pool import PostgresConnectorAsyncPool

async with PostgresConnectorAsyncPool() as db:
    users = await db.fetch_all_as_dicts("SELECT * FROM users")
    
    # Single value
    count = await db.fetch_value("SELECT COUNT(*) FROM users")
    
    # DataFrame
    df = await db.fetch_all_as_df("SELECT * FROM orders")
```

### Sync with Pool (Recommended for sync applications)

```python
from postgres_helpers.postgres_sync_pool import PostgresConnectorPool

with PostgresConnectorPool() as db:
    users = db.fetch_all_as_dicts("SELECT * FROM users")
    count = db.fetch_value("SELECT COUNT(*) FROM users")
```

### Single Connection (Async)

```python
from postgres_helpers.postgres_async import PostgresConnectorAsync

async with PostgresConnectorAsync() as db:
    users = await db.fetch_all_as_dicts("SELECT * FROM users")
```

### Single Connection (Sync)

```python
from postgres_helpers.postgres_sync import PostgresConnector

with PostgresConnector() as db:
    users = db.fetch_all_as_dicts("SELECT * FROM users")
```

## Transactions

All connectors support transactions via context managers:

```python
# Async
async with db.transaction() as conn:
    await conn.execute("INSERT INTO orders (user_id) VALUES ($1)", user_id)
    await conn.execute("UPDATE inventory SET stock = stock - 1 WHERE id = $1", item_id)
    # Auto-commits on success, auto-rollbacks on exception

# Sync
with db.transaction() as cursor:
    cursor.execute("INSERT INTO orders (user_id) VALUES (%s)", (user_id,))
    cursor.execute("UPDATE inventory SET stock = stock - 1 WHERE id = %s", (item_id,))
```

## Insert Helpers

```python
# Simple insert
result = await db.insert_into_with_dict(
    "users",
    {"email": "test@example.com", "name": "Test User"},
    on_duplicate_ignore=True
)

if result.was_duplicate:
    print("User already exists")
elif result.was_inserted:
    print("User created!")

# Insert with RETURNING
result = await db.insert_with_dict_returning(
    "users",
    {"email": "test@example.com", "name": "Test User"}
)
print(f"Created user ID: {result.returning_row['id']}")

# Upsert (INSERT ... ON CONFLICT UPDATE)
result = await db.insert_into_with_dict_update(
    "user_settings",
    {"user_id": 123, "theme": "dark"},
    constraint_key="user_settings_user_id_key"
)
```

## Error Handling

```python
from postgres_helpers.exceptions import (
    PostgresHelperError,
    UniqueViolationError,
    ForeignKeyViolationError,
    QueryExecutionError
)

try:
    await db.insert_into_with_dict(
        "users",
        {"email": "exists@example.com"},
        on_duplicate_ignore=False
    )
except UniqueViolationError as e:
    print(f"Duplicate key: {e.constraint_name}")
except PostgresHelperError as e:
    print(f"Database error: {e}")
```

## Available Methods

All connectors provide these methods:

| Method | Returns | Description |
|--------|---------|-------------|
| `execute_one_query()` | `QueryResult` | Execute INSERT/UPDATE/DELETE |
| `execute_many_query()` | `ExecuteManyResult` | Batch execute |
| `fetch_all_as_dicts()` | `List[Dict]` | SELECT ? list of dicts |
| `fetch_all_as_df()` | `DataFrame` | SELECT ? pandas DataFrame |
| `fetch_one_as_dict()` | `Dict \| None` | Single row |
| `fetch_value()` | `Any \| None` | Single value |
| `insert_into_with_dict()` | `InsertResult` | Insert from dict |
| `insert_with_dict_returning()` | `InsertResult` | Insert with RETURNING |
| `insert_into_with_dict_update()` | `UpsertResult` | Upsert |
| `insert_into_with_dict_update_returning()` | `UpsertResult` | Upsert with insert/update detection |
| `get_postgresql_version()` | `str` | Get server version |
| `table_exists()` | `bool` | Check table existence |
| `transaction()` | context manager | Transaction support |

## Placeholder Syntax

**Important:** Async and sync connectors use different placeholder syntax:

| Connector | Placeholder | Example |
|-----------|-------------|---------|
| Async (asyncpg) | `$1, $2, $3` | `SELECT * FROM users WHERE id = $1` |
| Sync (psycopg2) | `%s, %s, %s` | `SELECT * FROM users WHERE id = %s` |

## Logging

```python
from postgres_helpers.app_config import logging_config
import logging

# Default: logs to {project_root}/logs/postgres_helpers.log
logging_config()

# Custom file and level
logging_config(log_file_name='my_app.log', log_level=logging.INFO)
```

## Running Tests

```bash
pip install -e ".[test]"
pytest
```

## Useful Git Commands

Remove files from git repository (not the file system):
```bash
git rm --cached <path of file to be removed from git repo>
git commit -m "Deleted file from repository only"
git push
```

Cancel staged changes:
```bash
git restore --staged .
```

Create tags/versions on GitHub (triggers pip upgrade):
```bash
git tag <version_number>
git push origin <version_number>

# Example for this version:
git tag 0.1.0
git push origin 0.1.0
```

## Useful venv Commands

Create a new venv:
```bash
python -m venv venv
```

Install requirements:
```bash
pip install -e .
# or with dev dependencies
pip install -e ".[dev]"
```

Upgrade pip:
```bash
# Linux
python -m pip install --upgrade pip

# Windows
python.exe -m pip install --upgrade pip
```

## License

MIT License