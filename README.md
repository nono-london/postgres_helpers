# postgres_helpers

PostgreSQL helpers for synchronous and asynchronous Python applications, with direct connections or connection pools. Use parameterised queries, dictionary inserts, bulk inserts and explicit transactions through a consistent set of methods.

## Contents

- [Choose a connector](#choose-a-connector)
- [Installation](#installation)
- [Configuration](#configuration)
- [Quick starts](#quick-starts)
- [Queries and writes](#queries-and-writes)
- [Transactions](#transactions)
- [Connection management](#connection-management)
- [API reference](#api-reference)
- [Errors and logging](#errors-and-logging)
- [Development](#development)
- [Licence](#licence)

## Choose a connector

All four classes provide the same query and insert helpers. Choose how connections are managed and whether calls need `await`.

| Class | Driver | Connection model | Typical use |
| --- | --- | --- | --- |
| `PostgresConnectorAsyncPool` | asyncpg | Pool | Concurrent tasks within an async application |
| `PostgresConnectorPool` | psycopg2 | Pool | Repeated synchronous operations |
| `PostgresConnectorAsync` | asyncpg | Single connection | Sequential async operations on one connection |
| `PostgresConnector` | psycopg2 | Single connection | Scripts and sequential synchronous operations |

## Installation

Python 3.9 or later is required.

```bash
pip install git+https://github.com/nono-london/postgres_helpers.git
```

For installation from a local checkout, see [Development](#development).

## Configuration

Provide all five environment variables, or pass explicit connection arguments to the constructor.

```dotenv
POSTGRES_DB_HOST=localhost
POSTGRES_DB_PORT=5432
POSTGRES_DB_USER=myuser
POSTGRES_DB_PASS=mypassword
POSTGRES_DB_NAME=mydatabase
```

If these values are in your application's `.env` file, call `load_dotenv()` before constructing a connector, as shown in the quick starts. The library's automatic `.env` lookup is relative to the library location; explicitly loading your application's file avoids depending on that location.

| Constructor argument | Applies to | Default or source |
| --- | --- | --- |
| `db_host` | All four | `POSTGRES_DB_HOST` |
| `db_port` | All four | `POSTGRES_DB_PORT` |
| `db_user` | All four | `POSTGRES_DB_USER` |
| `db_password` | All four | `POSTGRES_DB_PASS` |
| `db_name` | All four | `POSTGRES_DB_NAME` |
| `application_name` | All four | `None`; optional name shown in `pg_stat_activity` |
| `connect_timeout` | Sync direct and pool | `6` seconds; connection establishment |
| `command_timeout` | Async direct and pool | `None`; optional query timeout in seconds |
| `pool_size_min` | Sync pool / async pool | `2` / `3` connections |
| `pool_size_max` | Both pools | `5` connections |

For example, `PostgresConnectorPool(pool_size_min=1, pool_size_max=5, application_name="reporting")` uses environment credentials with explicit pool settings.

## Quick starts

These four examples query a constant, so they need no application tables. Each context manager opens the connection or pool and closes it on exit. The [API reference](#api-reference) enumerates the full functionality; the following sections demonstrate reads, writes, batches and transactions.

### Async pool

```python
import asyncio

from dotenv import load_dotenv
from postgres_helpers.postgres_async_pool import PostgresConnectorAsyncPool


async def read_database_value():
    load_dotenv()
    async with PostgresConnectorAsyncPool(application_name="quick_start") as db:
        rows = await db.fetch_all_as_dicts("SELECT $1::integer AS value", (42,))
        print(rows)  # [{'value': 42}]


if __name__ == "__main__":
    asyncio.run(read_database_value())
```

### Sync pool

```python
from dotenv import load_dotenv
from postgres_helpers.postgres_sync_pool import PostgresConnectorPool

load_dotenv()
with PostgresConnectorPool(application_name="quick_start") as db:
    rows = db.fetch_all_as_dicts("SELECT %s::integer AS value", (42,))
    print(rows)  # [{'value': 42}]
```

### Direct async connection

```python
import asyncio

from dotenv import load_dotenv
from postgres_helpers.postgres_async import PostgresConnectorAsync


async def read_database_value():
    load_dotenv()
    async with PostgresConnectorAsync(application_name="quick_start") as db:
        rows = await db.fetch_all_as_dicts("SELECT $1::integer AS value", (42,))
        print(rows)  # [{'value': 42}]


if __name__ == "__main__":
    asyncio.run(read_database_value())
```

### Direct sync connection

```python
from dotenv import load_dotenv
from postgres_helpers.postgres_sync import PostgresConnector

load_dotenv()
with PostgresConnector(application_name="quick_start") as db:
    rows = db.fetch_all_as_dicts("SELECT %s::integer AS value", (42,))
    print(rows)  # [{'value': 42}]
```

## Queries and writes

The examples below run inside an open connector context, where `db` is your chosen connector. Async examples work with either async class; sync examples work with either sync class.

### SQL parameters

Pass values separately as a tuple. A single value needs a trailing comma: `(42,)`.

| Operation | Sync connectors | Async connectors |
| --- | --- | --- |
| Ordinary queries and `execute_many_query()` | `%s`, `%s`, … | `$1`, `$2`, … |
| `insert_many_by_batch()` | One `VALUES %s` marker | One `VALUES %s` marker |
| Dictionary insert and upsert helpers | Placeholders generated internally | Placeholders generated internally |

### Read rows, values and DataFrames

```python
# Async: multiple rows, one row, one value and a pandas DataFrame.
rows = await db.fetch_all_as_dicts("SELECT $1::integer AS value", (42,))
row = await db.fetch_one_as_dict("SELECT $1::integer AS value", (42,))
value = await db.fetch_value("SELECT $1::integer", (42,))
dataframe = await db.fetch_all_as_df("SELECT $1::integer AS value", (42,))
```

<details>
<summary>Sync equivalents</summary>

```python
rows = db.fetch_all_as_dicts("SELECT %s::integer AS value", (42,))
row = db.fetch_one_as_dict("SELECT %s::integer AS value", (42,))
value = db.fetch_value("SELECT %s::integer", (42,))
dataframe = db.fetch_all_as_df("SELECT %s::integer AS value", (42,))
```

</details>

When no rows match, `fetch_all_as_dicts()` returns `[]`, and `fetch_all_as_df()` returns an empty DataFrame retaining the query’s column names and aliases. `fetch_one_as_dict()` and `fetch_value()` return `None`. A query referencing a non-existent table raises `QueryExecutionError`.

### Example table

The write examples use this table. Create it in a development database before running them.

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL
);
```

### Execute one statement or repeated statements

```python
result = await db.execute_one_query(
    "UPDATE users SET name = $1 WHERE id = $2", ("Ada", 1)
)
print(result.rows_affected, result.status_message)

batch = await db.execute_many_query(
    "UPDATE users SET name = $1 WHERE id = $2",
    [("Ada", 1), ("Grace", 2)],
)
print(batch.total_statements)
```

<details>
<summary>Sync equivalents</summary>

```python
result = db.execute_one_query(
    "UPDATE users SET name = %s WHERE id = %s", ("Ada", 1)
)
print(result.rows_affected, result.status_message)

batch = db.execute_many_query(
    "UPDATE users SET name = %s WHERE id = %s",
    [("Ada", 1), ("Grace", 2)],
)
print(batch.total_statements)
```

</details>

`execute_one_query()` returns execution status and an affected-row count. Its `QueryResult.last_inserted_id` and `QueryResult.returning_row` fields remain `None`, even if the SQL includes `RETURNING`. To retrieve an inserted row and its ID, use `insert_with_dict_returning()`: it returns an `InsertResult` with `returning_row` and, when the returned row has a column named `id`, `last_inserted_id`.

`execute_many_query()` repeats a statement with separate parameter tuples; its affected-row count can be unavailable (`-1`).

### Insert dictionaries and handle duplicates

```python
result = await db.insert_into_with_dict(
    "users",
    {"id": 1, "email": "ada@example.com", "name": "Ada"},
    on_duplicate_ignore=True,
)
if result.was_duplicate:
    print("Duplicate skipped")

created = await db.insert_with_dict_returning(
    "users",
    {"id": 2, "email": "grace@example.com", "name": "Grace"},
    on_duplicate_ignore=False,
)
print(created.returning_row, created.last_inserted_id)
```

Both helpers ignore unique conflicts by default. Set `on_duplicate_ignore=False` to raise an error instead. An ignored duplicate has no returned row; `last_inserted_id` is populated from a returned column named `id`.

For sync connectors, use the same calls without `await`.

### Bulk inserts

```python
rows = [(3, "alan@example.com", "Alan"), (4, "katherine@example.com", "Katherine")]
result = await db.insert_many_by_batch(
    "INSERT INTO users (id, email, name) VALUES %s ON CONFLICT DO NOTHING",
    rows,
    page_size=1_000,
)
print(result.total_statements, result.rows_affected)
```

Use the same SQL and arguments without `await` for sync connectors. All four classes require exactly one `VALUES %s` marker for this method, including the async classes.

The helper groups rows into multi-row INSERT statements. `page_size` defaults to `10_000`; async connectors may use smaller pages to stay within the driver parameter limit. All pages run in one transaction. `total_statements` records the number of input rows, while `rows_affected` counts inserted rows across all pages, excluding ignored conflicts. Empty input returns zero for both counts.

### Upserts and returned rows

```python
result = await db.insert_into_with_dict_update(
    "users",
    {"id": 1, "email": "ada@example.com", "name": "Ada Lovelace"},
    constraint_key="users_pkey",
    on_duplicate_update=True,
)
print(result.rows_affected)

saved = await db.insert_into_with_dict_update_returning(
    "users",
    {"id": 1, "email": "ada@example.com", "name": "Ada Lovelace"},
    constraint_key="users_pkey",
)
print(saved.returning_row, saved.was_inserted, saved.was_updated)
```

For sync connectors, use the same calls without `await`. `constraint_key` names an existing unique or primary-key constraint and defaults to `<table_name>_pkey`. The supplied dictionary columns are updated on conflict. The ordinary upsert also accepts `on_duplicate_update=False` to skip conflicting rows.

The returning variant uses PostgreSQL’s `xmax` system column to infer whether an upsert inserted or updated a row. If a sync connector cannot convert that metadata to an integer, it still returns the row, leaves both flags false and logs a warning. The ordinary upsert does not distinguish inserts from updates reliably.

## Transactions

Use the object yielded by `transaction()` for every statement in that transaction. Sync connectors yield a psycopg2 cursor; async connectors yield an asyncpg connection. The transaction commits on normal exit and rolls back when an exception escapes the block.

```python
# Either async connector.
async with db.transaction() as connection:
    await connection.execute("UPDATE users SET name = $1 WHERE id = $2", "Ada", 1)
    await connection.execute("UPDATE users SET name = $1 WHERE id = $2", "Grace", 2)
```

```python
# Either sync connector.
with db.transaction() as cursor:
    cursor.execute("UPDATE users SET name = %s WHERE id = %s", ("Ada", 1))
    cursor.execute("UPDATE users SET name = %s WHERE id = %s", ("Grace", 2))
```

These are driver calls: asyncpg takes separate positional values, while psycopg2 takes a parameter tuple. Wrapper calls such as `db.execute_one_query()` manage connections independently and should not be used to compose this transaction.

## Connection management

Context managers are the simplest way to guarantee cleanup. When managing connections manually, close them in a `finally` block.

<details>
<summary>Direct connections: manual lifecycle and status</summary>

```python
# Async direct connector, inside an async function.
db = PostgresConnectorAsync()
try:
    await db.open_connection()
    connected = db.is_connected()
    info = await db.get_connection_info()
    version = await db.get_postgresql_version()
    exists = await db.table_exists("users", schema="public")
finally:
    await db.close_connection()
```

```python
# Sync direct connector.
db = PostgresConnector()
try:
    db.open_connection()
    connected = db.is_connected()
    info = db.get_connection_info()
    version = db.get_postgresql_version()
    exists = db.table_exists("users", schema="public")
finally:
    db.close_connection()
```

Query helpers also open a direct connection when needed. Direct query, insert and utility methods accept `close_connection=True` to close it after the operation; pool methods do not accept this argument. An empty input to `insert_many_by_batch()` returns immediately without opening or closing a connection. `is_connected()` is synchronous in both classes.

</details>

<details>
<summary>Pools: status, manual cleanup and raw connections</summary>

```python
# Inside an open async pool context.
active = db.is_pool_active()
info = await db.get_pool_status()
async with db.acquire_connection() as connection:
    value = await connection.fetchval("SELECT $1::integer", 42)

# For a manually managed async pool: await db.close_pool() in finally.
```

```python
# Inside an open sync pool context.
active = db.is_pool_active()
info = db.get_pool_status()
with db.acquire_connection() as connection:
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT %s::integer", (42,))
            value = cursor.fetchone()[0]

# For a manually managed sync pool: db.close_pool() in finally.
```

`acquire_connection()` returns the borrowed connection to its pool on exit. Acquisition provides no transaction scope. Use the driver's transaction context to group raw writes; asyncpg statements outside an explicit transaction commit independently. `is_pool_active()` is synchronous in both classes.

Async pool status includes the live pool size and idle connection count. Sync pool status reports the configured maximum as `pool_size`; it does not populate `pool_free` or `server_version`.

Both pool classes also provide `get_postgresql_version()` and `table_exists("users", schema="public")`; await these calls for the async pool.

</details>

## API reference

### Shared methods

Every method below is available on all four connectors. Await async methods, except when entering `transaction()`, which uses `async with`.

| Method | Returns | Purpose |
| --- | --- | --- |
| `fetch_all_as_dicts()` | `list[dict]` | Read all rows as dictionaries |
| `fetch_all_as_df()` | pandas `DataFrame` | Read all rows as a DataFrame |
| `fetch_one_as_dict()` | `dict` or `None` | Read the first row |
| `fetch_value()` | Scalar or `None` | Read the first column of the first row |
| `execute_one_query()` | `QueryResult` | Execute a statement; return count and status |
| `execute_many_query()` | `ExecuteManyResult` | Repeat a statement for parameter tuples |
| `insert_many_by_batch()` | `ExecuteManyResult` | Insert rows in pages using `VALUES %s` |
| `insert_into_with_dict()` | `InsertResult` | Insert a dictionary, optionally ignoring duplicates |
| `insert_with_dict_returning()` | `InsertResult` | Insert a dictionary and return the inserted row |
| `insert_into_with_dict_update()` | `UpsertResult` | Insert or update on a constraint conflict |
| `insert_into_with_dict_update_returning()` | `UpsertResult` | Upsert with returned row and insert/update flags |
| `transaction()` | Context manager | Group driver calls into one transaction |
| `get_postgresql_version()` | `str` | Read the server version |
| `table_exists()` | `bool` | Check a table name in a schema; default `public` |

### Lifecycle and status methods

| Method | Direct sync | Direct async | Sync pool | Async pool |
| --- | --- | --- | --- | --- |
| `open_connection()` | Call | Await | — | — |
| `close_connection()` | Call | Await | — | — |
| `is_connected()` | Call | Call | — | — |
| `get_connection_info()` | Call | Await | — | — |
| `close_pool()` | — | — | Call | Await |
| `is_pool_active()` | — | — | Call | Call |
| `get_pool_status()` | — | — | Call | Await |
| `acquire_connection()` | — | — | `with` | `async with` |

The `get_connection_info()` and `get_pool_status()` methods return `ConnectionInfo`. Connector context managers use `with` for sync classes and `async with` for async classes.

### Result objects

Import result types from `postgres_helpers.results` when needed for type annotations.

| Result type | Main fields |
| --- | --- |
| `QueryResult` | `success`, `rows_affected`, `status_message` |
| `ExecuteManyResult` | `success`, `total_statements`, `rows_affected` |
| `InsertResult` | `success`, `rows_affected`, `was_inserted`, `was_duplicate`, `returning_row`, `last_inserted_id` |
| `UpsertResult` | `success`, `rows_affected`, `was_inserted`, `was_updated`, `returning_row` |
| `ConnectionInfo` | `host`, `port`, `database`, `user`, `server_version`, `is_connected`, `pool_size`, `pool_free` |

Returned rows and insert/update flags depend on the selected method, as described in [Queries and writes](#queries-and-writes). An unavailable affected-row count is `-1`.

## Errors and logging

### Handle database errors

Wrapper exceptions are available from `postgres_helpers.exceptions`. Catch a specific error before its parent class.

```python
from postgres_helpers.exceptions import PostgresHelperError, UniqueViolationError

try:
    await db.insert_into_with_dict(
        "users",
        {"id": 1, "email": "ada@example.com", "name": "Ada"},
        on_duplicate_ignore=False,
    )
except UniqueViolationError as error:
    print(f"Duplicate constraint: {error.constraint_name}")
except PostgresHelperError as error:
    print(f"Database error: {error}")
```

For sync connectors, remove `await`. Other exception types include `ConnectionError`, `PoolError`, `QueryExecutionError`, `ForeignKeyViolationError`, `CheckViolationError`, `QueryTimeoutError` and `TransactionError`. Calls made directly on a borrowed driver connection use the driver's exception behaviour; transaction blocks can wrap failures in `TransactionError`.

### Configure logging

```python
import logging

from postgres_helpers.app_config import logging_config

logging_config(log_file_name="my_app.log", log_level=logging.INFO)
```

The helper writes timestamped logs under the library's root `logs/` directory. Its default filename is `postgres_helpers.log` and default level is `DEBUG`. Applications can instead configure Python's standard logging themselves.

## Development

<details>
<summary>Install from source and run tests</summary>

```bash
git clone https://github.com/nono-london/postgres_helpers.git
cd postgres_helpers
python -m venv venv
source venv/bin/activate
pip install -e ".[dev]"
pytest
```

The tests need a running PostgreSQL server and the five connection variables from [Configuration](#configuration). Use a dedicated development database and a role permitted to create and drop databases: some tests create and drop `test_db` as well as test tables.

For test dependencies without the development tools, use `pip install -e ".[test]"`.

</details>

The GitHub Actions workflow runs the tests in an isolated PostgreSQL CI environment. Its configuration is in [postgres_helpers/.github/workflows/ci-postgresql.yml](.github/workflows/ci-postgresql.yml).

<details>
<summary>Run the CI tests locally with Docker</summary>

To build the CI Docker image and run the same tests locally:

```bash
bash .github/workflows/ci-postgresql/local-build.sh
```

This requires Docker with Buildx. Each run uses a dedicated builder and removes
its container, image and build cache on exit, including after a test failure.
Reports remain in `.github/ci-local/ci-postgresql/<run-id>/`, which Git ignores.
If a run is forcibly terminated or cleanup fails, recover with:

```bash
bash .github/workflows/ci-postgresql/local-cleanup.sh <run-id>
```

Cleanup is safe to repeat and retains the reports. These local tools do not run
as part of the GitHub-hosted workflow.

</details>

## Licence

MIT License.
