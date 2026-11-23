# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# PARAMETERS CELL ********************

env = "fabric-dev"
metadata_dir = "/lakehouse/default/Files/ingest/metadata"
run_id = ""  # empty: a UTC timestamp
includes = ""  # comma-separated entity patterns
excludes = ""
state_table = "ingest_lh.ops.ingest_state"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from ingest.adapters.fabric import run_notebook

summary = run_notebook(
    spark, notebookutils, env=env, metadata_dir=metadata_dir, run_id=run_id,
    includes=includes, excludes=excludes, state_table=state_table,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

# Not inside try/except: notebookutils.notebook.exit has no effect there.
notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
