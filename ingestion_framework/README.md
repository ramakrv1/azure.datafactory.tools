# ingest

Portable, metadata-driven PySpark ingestion framework. Transforms are written once against the PySpark
DataFrame API; engine differences (Databricks first, Fabric/Snowflake later) live in thin adapters.
The metadata, override, selection and DAG patterns are ported from the PowerShell module in this repository.

## Layout
```
src/ingest/core/      engine-agnostic: model, config, selection, dag, transforms, quality, pipeline
src/ingest/ports.py   engine ports (Protocols) and the `Engine` bundle
src/ingest/adapters/  engine adapters: memory (reference), spark_common, delta_common, fabric
src/ingest/runner.py  engine-neutral `run_metadata` (load, select, run, JSON summary)
src/ingest/cli.py     `ingest validate|plan|docs`
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
- `join(with, on, how='inner', select=None)`: right side is the target of entity `with`. In YAML quote the key (`"on"`): bare `on` parses as boolean true
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

## Adapters
- `memory`: reference implementation of every port, in memory.
- `spark_common.SparkReader`: `files` (`format`, `options`, `path`) and `jdbc` sources, `read_table`. A `table` source never
  reaches `read`; the pipeline resolves it through `read_table`.
- `delta_common`: `DeltaWriter` (managed tables by name; creates the schema and the table when absent) and `DeltaStateStore`
  (Delta table `entity, state JSON, updated_at`, one row per entity). Shared by Fabric and Databricks.
- `fabric`: `fabric_engine(spark, notebookutils, state_table)`, `FabricSecretProvider` and `run_from_metadata`.
  `notebookutils` is injected, never imported. Set `TableRef.catalog` to the lakehouse name (`lakehouse.schema.table`).
  In `${secret:<vault>/<name>}` the scope is the Key Vault name (a full `https://` URL also works).

Merge semantics (both memory and Delta, pinned by the contract suite):
- duplicate keys in the source raise `ValueError("merge source has duplicate keys ...")` before anything is written;
- keys match null-safely: a NULL key is a value, so a repeated load updates that row instead of adding copies;
- new source columns are added to the table (schema evolution); a merge into a missing table creates it.

## Contract suite
`tests/contract/test_adapter_contract.py` runs the same tests against memory and local Delta: overwrite/append/merge with
schema evolution, merge key rules, read-after-write, state round-trip and a two-run incremental pipeline. A new adapter
adds one harness to `ENGINES` and must pass unchanged. All tests share one Delta-enabled local SparkSession.

## CLI
```
ingest validate --metadata DIR --env ENV          # OK: N entities, or the error and exit code 2
ingest plan --metadata DIR --env ENV [--include P ...] [--exclude P ...]   # parallel waves of the selection
ingest docs --metadata DIR --env ENV              # Mermaid dependency graph
```
There is no `run` command: runs happen inside the engine (on Fabric, a runner notebook calls `run_from_metadata`).

## CI
`.github/workflows/ingestion-framework-ci.yml` tests Python 3.12 with Spark 4.0 / Delta 4.0 and Python 3.13 with Spark
4.1 / Delta 4.2 (Fabric Runtime 2.0), with a 90% coverage floor.
