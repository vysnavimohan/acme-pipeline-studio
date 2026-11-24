"""Emits Lakeflow Connect configuration for the Postgres ingestion stage.

For the PoC we generate a `connection.yml` + `ingestion_pipeline.yml` pair
that follow the same shape as the lakeflow-tapworks output. The actual
pipeline is then deployed via DAB.

In a future iteration we'd call the Databricks Connectors API directly.
For now the DAB resource block is the contract.
"""

from __future__ import annotations

import yaml
from typing import Any

from agent.state import PipelineSpec


def render_connection_yaml(spec: PipelineSpec, instance_name: str, secret_scope: str) -> str:
    """Defines a Lakeflow Connect 'connection' resource (Postgres).

    `instance_name` is the Lakebase instance name (e.g. `databricks-pipeline-studio-billing`).
    """
    config: dict[str, Any] = {
        "resources": {
            "connections": {
                f"{spec.source.system}_postgres": {
                    "name": f"{spec.source.system}-postgres-conn",
                    "connection_type": "POSTGRESQL",
                    "options": {
                        "host": f"${{databricks.database_instances.{spec.source.system}.read_write_dns}}",
                        "port": "5432",
                        "database": "databricks_postgres",
                        "user": "{{secrets/" + secret_scope + "/" + spec.source.system + "_pg_user}}",
                        "password": "{{secrets/" + secret_scope + "/" + spec.source.system + "_pg_pwd}}",
                    },
                    "comment": f"Auto-generated connection to {spec.source.system.upper()} for pipeline {spec.target.table}",
                }
            }
        }
    }
    return yaml.safe_dump(config, sort_keys=False)


def render_ingestion_yaml(spec: PipelineSpec, dev_catalog: str) -> str:
    """Defines the Lakeflow Connect ingestion pipeline resource.

    Each source table becomes one bronze table in the dev catalog.
    """
    tables = []
    for t in spec.source.tables:
        # Allow `schema.table` or bare `table`
        if "." in t:
            src_schema, src_table = t.split(".", 1)
        else:
            src_schema, src_table = "public", t
        tables.append({
            "source_schema": src_schema,
            "source_table": src_table,
            "destination_catalog": dev_catalog,
            "destination_schema": "bronze",
            "destination_table": f"{spec.source.system}__{src_table}",
            "scd_type": "SCD_TYPE_1" if spec.target.write_mode != "append" else "APPEND_ONLY",
            "primary_keys": spec.source.primary_key or [src_table + "_id"],
        })

    streaming = spec.sla.mode == "streaming"
    # In streaming mode Lakeflow Connect runs every 10 min (its minimum
    # cadence today); in batch mode we use the spec's freshness_minutes
    # rounded into a cron expression — defaults to nightly 1am if cron is
    # set on the spec.
    ingest_cadence = (
        {"continuous": False, "schedule_cron": "*/10 * * * *",
         "comment": "Streaming mode: Lakeflow Connect runs every 10 min (its current min cadence)."}
        if streaming else
        {"continuous": False,
         "schedule_cron": spec.sla.schedule_cron or "0 1 * * *",
         "comment": f"Batch mode: every {spec.sla.freshness_minutes} min target freshness."}
    )

    config = {
        "resources": {
            "pipelines": {
                f"{spec.source.system}_ingestion": {
                    "name": f"ingest-{spec.source.system}-{spec.target.table}",
                    "channel": "PREVIEW",
                    "edition": "ADVANCED",
                    "ingestion_cadence": ingest_cadence,
                    "ingestion_definition": {
                        "connection_name": f"{spec.source.system}-postgres-conn",
                        "objects": [{"table": t} for t in tables],
                    },
                }
            }
        }
    }
    return yaml.safe_dump(config, sort_keys=False)
