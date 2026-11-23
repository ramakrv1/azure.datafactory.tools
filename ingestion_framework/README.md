# ingest

Portable, metadata-driven PySpark ingestion framework. Transforms are written once against the PySpark
DataFrame API; engine differences (Databricks first, Fabric/Snowflake later) live in thin adapters.
The metadata, override, selection and DAG patterns are ported from the PowerShell module in this repository.

## Layout
```
src/ingest/core/      engine-agnostic: model, config (overlay), selection, dag (stdlib + pyyaml only)
src/ingest/adapters/  engine adapters (later phases)
metadata/entities/    entity definitions (YAML)
metadata/env/<env>.yaml  per-environment overlay
tests/unit/           no Spark needed
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

## Selection
`<layer>.<name>[@<groupPattern>]`, case-insensitive wildcards, e.g. `bronze.sales.*`, `*.orders@sales`.
As in `ApplyExclusionOptions`, excludes apply only when no includes are given. Rules files: `+include`,
`-exclude`, a bare line is an include, `#` starts a comment.

## Tests
```
pip install -e ".[dev]"
pytest -q
```
