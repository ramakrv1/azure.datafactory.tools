# ingest

Portable, metadata-driven PySpark ingestion framework. Transforms are written once against the PySpark
DataFrame API; engine differences (Databricks first, Fabric/Snowflake later) live in thin adapters.
The metadata, override, selection and DAG patterns are ported from the PowerShell module in this repository.

## Layout
```
src/ingest/core/      engine-agnostic: model, config, selection, dag, transforms, quality, pipeline
src/ingest/ports.py   engine ports (Protocols) and the `Engine` bundle
src/ingest/adapters/  engine adapters (`memory` is the reference; Databricks etc. later)
metadata/entities/    entity definitions (YAML)
metadata/env/<env>.yaml  per-environment overlay
tests/unit/           no Spark needed
tests/spark/          local plain-Spark tests (`@pytest.mark.spark`)
```

## Entity metadata
```yaml
name: sales.orders          # unique; layer: bronze|silver|gold; group is optional
layer: bronze
source: {type: jdbc, options: {url: "${SQL_URL}", dbtable: dbo.Orders, password: "${secret:kv/pwd}"}}
load: {mode: incremental, watermark_column: modified_at}   # full | incremental | merge (needs keys)
target: {table: bronze.orders}                             # catalog injected by the overlay
transforms: [{op: rename, args: {columns: {OrderID: order_id}}}]
quality: [{name: id_not_null, expr: "order_id IS NOT NULL", action: quarantine}]  # fail|quarantine|warn
depends_on: []
```
Source types: `files` (format, path), `jdbc` (options.url plus dbtable or query), `table` (name of another entity).
Dependencies (`source.table`, join `with`, `depends_on`) drive execution order. Unknown keys are rejected.

## Environment overlay (`env/<env>.yaml`)
```yaml
variables: {CATALOG: dev_lake}
overrides:
  - {entity: "sales.*", path: target.catalog, value: "${CATALOG}", action: add}   # update (default) | add | remove
options: {fail_when_entity_not_found: true, fail_when_path_not_found: true}
```
Mapping to `Update-PropertiesFromFile`:
- `entity` is a case-insensitive wildcard (like `-like`); `path` is dotted (`$.` and `$.properties.` are stripped).
- `+path` / `-path` are shorthand for add / remove. Overrides apply in file order.
- `update` and `remove` need the path to exist; `add` creates intermediate keys and overwrites.
- The two `fail_when_*` options downgrade errors to warnings.
- Afterwards `${NAME}` tokens are expanded from `variables`, then the process environment. An unresolved token
  is an error (stricter than PowerShell). `${secret:scope/key}` is left for runtime resolution; `$${X}` escapes.

## Transform ops
Pure `fn(df, ctx, **args)` steps, validated without Spark (`validate_steps`: unknown op, missing or unexpected args).
- `select(columns)`, `drop(columns)`, `rename(columns: {old: new})`, `cast(columns: {col: type})`
- `filter(expr)`, `with_columns(columns: {name: sql_expr})`, `trim_strings()` (all string columns)
- `drop_duplicates(keys, order_by=None)`: with `order_by` (`["modified_at desc"]`) keeps the first row per key
- `hash_key(columns, name)`: sha256 of the null-safe, `||`-joined columns
- `join(with, on, how='inner', select=None)`: right side is the target of entity `with`
- `aggregate(group_by, aggs)`: group_by items are columns or `{name: sql_expr}`; aggs are `{name: sql_expr}`
- `custom(function: "pkg.module:func", args)`: calls `func(df, **args)`

## Quality actions
A row violates a rule when `NOT coalesce(expr, false)` (NULL fails). All rule counts come from one aggregation.
- `fail`: the run fails, listing every violated rule with its count
- `quarantine`: rows move to `<target>_quarantine` with `_dq_failed_rules` (array of rule names)
- `warn`: counted only

## Pipeline
`run_entity` per entity: validate transforms, read state, read source (secrets resolved only now), filter
`watermark_column > stored watermark`, count rows and `max(watermark_column)` in one pass, transform, add audit columns
(`_ingested_at`, `_run_id`, `_source_entity`), apply quality, write (full=overwrite, incremental=append, merge=upsert on
keys), save state. `watermark_column` names a column of the source, before transforms.
- Empty incremental/merge loads return `no_new_data` and write nothing.
- State: `watermark` (Spark string cast of the max), `last_run_id`, `last_status`, `rows_written`, `updated_at`.
  The watermark advances only after a successful write. Failures record `last_status: failed` and keep the watermark.
- Secrets never enter state or results; secret values are redacted from error messages.
- `run_project` runs in dependency order; dependents of a failed entity are `skipped`, and with `fail_fast` (default)
  everything after the first failure is too.

## Ports and adapters
Core talks to the engine only through `ports.py`: `SourceReader`, `TableWriter` (overwrite/append/merge),
`StateStore`, `SecretProvider`, bundled as `Engine`. Core may import `pyspark.sql` (DataFrame API only, Spark Connect
compatible) but never `delta`, `dbutils`, `notebookutils`, `snowflake`, `databricks` or `ingest.adapters`
(enforced by `tests/unit/test_portability.py`). `adapters/memory.py` implements all ports in memory and is the reference
for adapter contract tests.

## Selection
`<layer>.<name>[@<groupPattern>]`, case-insensitive wildcards, e.g. `bronze.sales.*`, `*.orders@sales`.
As in `ApplyExclusionOptions`, excludes apply only when no includes are given. Rules files: `+include`,
`-exclude`, a bare line is an include, `#` starts a comment.

## Tests
```
pip install -e ".[dev]"
pytest -q
```
