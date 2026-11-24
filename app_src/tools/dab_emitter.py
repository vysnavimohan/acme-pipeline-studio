"""Emits the file set for a generated pipeline bundle.

The bundle follows the canonical SDP medallion folder layout:

  pipelines/<table>/
    databricks.yml
    spec.yaml
    README.md
    resources/
      pipeline.yml          (SDP pipeline DAB resource)
      job.yml               (scheduled Job that triggers the pipeline)
      connection.yml        (Lakeflow Connect Connection, production pattern)
      ingestion.yml         (Lakeflow Connect IngestionPipeline + Gateway)
    transformations/
      staging/bronze_views.py  (@dlt.view pass-throughs over ingested bronze)
      silver/<table>.py        (the @dlt.table for silver)
    tests/
      row_checks.sql

The agent deploys this transformations/ tree as a multi-file SDP project
(pipeline root_path + glob transformations/**), matching what the PR shows. The
`resources/job.yml` reflects the cron schedule the prod deploy actually wires
up via the SDK. The Lakeflow Connect YAMLs document the production-grade
bronze ingestion path (LIVE: the agent deploys this via the shared external Postgres source).
"""

from __future__ import annotations

import os
import textwrap
from pathlib import PurePosixPath
from typing import Any

import yaml

from agent.state import PipelineSpec


def _silver_table_name(spec: PipelineSpec) -> str:
    return f"{spec.target.catalog}.{spec.target.target_schema}.{spec.target.table}"


def _bronze_table_name(spec: PipelineSpec, source_table: str) -> str:
    return f"{spec.target.catalog}.bronze.{spec.source.system}__{source_table}"


def render_sdp_pipeline_py(spec: PipelineSpec,
                             table_comment: str | None = None,
                             column_comments: dict[str, dict[str, str]] | None = None,
                             inline_comments: list[str] | None = None,
                             silver_sql_select: str | None = None) -> str:
    """Emit the silver SDP source. Delegates to real_deployer.render_sdp_source
    so PR and runtime are guaranteed identical."""
    from tools import real_deployer
    return real_deployer.render_sdp_source(
        spec,
        request_id="(stamped by PR header)",
        table_comment=table_comment,
        column_comments=column_comments,
        inline_comments=inline_comments,
        silver_sql_select=silver_sql_select,
    )


def _pipeline_compute_yaml() -> dict:
    """Plain-dict compute block for the emitted reference pipeline.yml — mirrors
    tools.real_deployer._pipeline_compute but YAML-serialisable. Emits a classic
    pipeline cluster when OPS_PIPELINE_NODE_TYPE_ID is set (non-serverless deploy),
    otherwise serverless, so the PR the reviewer sees matches what we deploy."""
    node = os.environ.get("OPS_PIPELINE_NODE_TYPE_ID", "").strip()
    if not node:
        return {"serverless": True}
    try:
        workers = int(os.environ.get("OPS_PIPELINE_NUM_WORKERS", "1") or 1)
    except ValueError:
        workers = 1
    return {"clusters": [{"label": "default", "node_type_id": node, "num_workers": workers}]}


def render_pipeline_yml(spec: PipelineSpec) -> str:
    """DAB resource for the SDP pipeline. Uses glob-libraries so adding new
    files under transformations/ doesn't require a YAML edit."""
    streaming = spec.sla.mode == "streaming"
    pipeline_def = {
        "name": f"silver-{spec.target.table}",
        "channel": "PREVIEW",
        "edition": "ADVANCED",
        "catalog": spec.target.catalog,
        "target": spec.target.target_schema,
        **_pipeline_compute_yaml(),
        "photon": True,
        # Glob libraries: all .py files under transformations/ get picked up
        "libraries": [{"glob": {"include": "../transformations/**"}}],
        "root_path": "..",
        "continuous": streaming,
        "configuration": {
            "pipeline.bronze_fq": (
                f"{spec.target.catalog}.dev_bronze"   # overwritten by deployer at runtime
            ),
            "pipeline.source_system": spec.source.system,
            "pipeline.target_table": spec.target.table,
        },
    }
    config = {"resources": {"pipelines": {f"silver_{spec.target.table}": pipeline_def}}}
    return yaml.safe_dump(config, sort_keys=False)


def render_job_yml(spec: PipelineSpec) -> str:
    """DAB resource for the scheduled Job that triggers the pipeline on cron.

    Batch pipelines get a cron schedule. Streaming pipelines get a Job with
    no schedule (the continuous pipeline drives itself; the Job is a
    'trigger now' handle for ad-hoc full refresh).
    """
    streaming = spec.sla.mode == "streaming"
    job_def: dict[str, Any] = {
        "name": f"job-silver-{spec.target.table}",
        "tasks": [{
            "task_key": "trigger_silver",
            "pipeline_task": {
                "pipeline_id": "${resources.pipelines." + f"silver_{spec.target.table}" + ".id}",
                "full_refresh": False,
            },
            "timeout_seconds": 0,
        }],
    }
    if not streaming:
        job_def["schedule"] = {
            "quartz_cron_expression": _quartz_from_unix_cron(
                spec.sla.schedule_cron or "0 1 * * *"
            ),
            "timezone_id": "Australia/Sydney",
            "pause_status": "UNPAUSED",
        }
    config = {"resources": {"jobs": {f"job_silver_{spec.target.table}": job_def}}}
    return yaml.safe_dump(config, sort_keys=False)


def _quartz_from_unix_cron(unix_cron: str) -> str:
    """Convert standard 5-field unix cron to Quartz 6-field (prepended with
    a seconds field, defaulting to 0). If already 6 or 7 fields, returns as-is.
    """
    parts = unix_cron.strip().split()
    if len(parts) == 5:
        # unix: minute hour dom month dow → quartz: sec min hour dom month dow ?
        # Quartz requires either day-of-month or day-of-week to be `?`
        minute, hour, dom, month, dow = parts
        # If both dom and dow are wildcards, use `?` for dow
        if dom == "*" and dow == "*":
            dow = "?"
        elif dow != "*" and dom == "*":
            dom = "?"
        elif dom != "*" and dow != "*":
            dow = "?"
        return f"0 {minute} {hour} {dom} {month} {dow}"
    return unix_cron


def render_connection_yml(spec: PipelineSpec, secret_scope: str = "databricks-pipeline-studio") -> str:
    """Lakeflow Connect Postgres Connection (production pattern). The agent
    does NOT currently deploy this against Lakebase (Lakebase blocks
    CREATE PUBLICATION); the YAML is in the PR as the canonical pattern for
    deploying against an external Postgres source."""
    config = {
        "resources": {
            "connections": {
                f"{spec.source.system}_postgres_src": {
                    "name": f"{spec.source.system}-postgres-src",
                    "connection_type": "POSTGRESQL",
                    "comment": (
                        f"Lakeflow Connect Postgres source connection for {spec.source.system.upper()}. "
                        f"Production-grade CDC ingestion lands the source tables in bronze. "
                        f"NOTE: against Databricks Lakebase, CREATE PUBLICATION is currently blocked — "
                        f"the agent falls back to a SQL-warehouse bronze loader for PoC. "
                        f"For an external Postgres source, this Connection works as-is."
                    ),
                    "options": {
                        "host": "<postgres-host>",
                        "port": "5432",
                        "database": "<database-name>",
                        "user": "{{secrets/" + secret_scope + "/" + spec.source.system + "_pg_user}}",
                        "password": "{{secrets/" + secret_scope + "/" + spec.source.system + "_pg_pwd}}",
                    },
                }
            }
        }
    }
    return yaml.safe_dump(config, sort_keys=False)


def render_ingestion_yml(spec: PipelineSpec, dev_catalog: str) -> str:
    """Lakeflow Connect IngestionGateway + IngestionPipeline (production
    pattern). Same NOTE as render_connection_yml applies."""
    streaming = spec.sla.mode == "streaming"

    # Gateway pipeline (captures CDC from the source Postgres)
    gateway = {
        "name": f"gw-{spec.source.system}",
        "gateway_definition": {
            "connection_name": "databricks-pipeline-studio-pg",
            "gateway_storage_catalog": dev_catalog,
            "gateway_storage_schema": f"_lfc_gateway_{spec.source.system}",
            "gateway_storage_name": f"gw_{spec.source.system}",
        },
        "channel": "PREVIEW",
        "edition": "ADVANCED",
    }

    # Ingestion pipeline (consumes from gateway, lands in bronze schema)
    tables = []
    for t in spec.source.tables:
        if "." in t:
            src_schema, src_table = t.split(".", 1)
        else:
            src_schema, src_table = "public", t
        tables.append({
            "table": {
                "source_schema": src_schema,
                "source_table": src_table,
                "destination_catalog": dev_catalog,
                "destination_schema": "bronze",
                "destination_table": f"{spec.source.system}__{src_table}",
                "scd_type": "SCD_TYPE_1" if spec.target.write_mode != "append" else "APPEND_ONLY",
                "primary_keys": spec.source.primary_key or [src_table + "_id"],
            }
        })

    ingestion = {
        "name": f"ingest-{spec.source.system}-{spec.target.table}",
        "channel": "PREVIEW",
        "edition": "ADVANCED",
        "continuous": streaming,
        "ingestion_definition": {
            "ingestion_gateway_id": "${resources.pipelines." + f"gateway_{spec.source.system}" + ".id}",
            "objects": tables,
        },
    }

    config = {
        "resources": {
            "pipelines": {
                f"gateway_{spec.source.system}": gateway,
                f"ingestion_{spec.source.system}_{spec.target.table}": ingestion,
            }
        }
    }
    return yaml.safe_dump(config, sort_keys=False)


def render_tests_yml(spec: PipelineSpec, silver_fqn: str | None = None) -> str:
    """SQL tests that run post-deploy to validate the silver table.

    `silver_fqn` is the table the tests should actually query. Without it they
    fall back to `spec.target`, which carries the LOGICAL schema (`silver`) rather
    than the environment-prefixed one that is really deployed — so the generated
    tests named a table that does not exist in dev and could never pass. A PR
    reviewer flagged exactly this, twice.
    """
    silver = silver_fqn or _silver_table_name(spec)
    sql_tests: list[str] = [
        f"-- Tests for {silver}. Generated from the spec's validation rules.",
    ]
    for c in spec.validation.not_null:
        sql_tests.append(f"SELECT COUNT(*) AS bad FROM {silver} WHERE {c} IS NULL;")
    for c in spec.validation.unique:
        sql_tests.append(f"SELECT {c}, COUNT(*) AS n FROM {silver} GROUP BY {c} HAVING COUNT(*) > 1;")
    # Business rules are enforced in the pipeline (expect_or_drop / .where), so
    # assert the post-condition here: no surviving row may violate the rule.
    from tools.real_deployer import compile_business_rule
    for r in ((spec.transform.business_rules or []) if spec.transform else []):
        pred = compile_business_rule(r)
        if pred:
            sql_tests.append(f"-- rule: {r}")
            sql_tests.append(f"SELECT COUNT(*) AS violations FROM {silver} WHERE NOT ({pred});")
    sql_tests.append(f"SELECT COUNT(*) AS row_count FROM {silver};")
    return "\n".join(sql_tests)


def render_bundle_root(spec: PipelineSpec, dev_catalog: str, prod_catalog: str, github_repo: str) -> str:
    """Top-level databricks.yml for the generated pipeline sub-bundle."""
    config = {
        "bundle": {
            "name": f"pipeline-{spec.target.table}",
        },
        "include": ["resources/*.yml"],
        "variables": {
            "dev_catalog": {"default": dev_catalog},
            "prod_catalog": {"default": prod_catalog},
        },
        "targets": {
            "dev": {
                "default": True,
                "mode": "development",
                "workspace": {"host": "${env.DATABRICKS_HOST}"},
            },
            "prod": {
                "mode": "production",
                "workspace": {"host": "${env.DATABRICKS_HOST}"},
            },
        },
    }
    return yaml.safe_dump(config, sort_keys=False)


def render_readme(spec: PipelineSpec) -> str:
    streaming = spec.sla.mode == "streaming"
    sched_blurb = ("continuous (streaming pipeline runs in continuous mode)"
                   if streaming
                   else f"on cron `{spec.sla.schedule_cron or '0 1 * * *'}` Australia/Sydney")
    return textwrap.dedent(f"""
        # Pipeline: `{spec.target.table}`

        > {spec.intent}

        **Source system:** `{spec.source.system.upper()}`
        **Source tables:** {", ".join(f"`{t}`" for t in spec.source.tables)}
        **Target:** `{_silver_table_name(spec)}` (write mode: `{spec.target.write_mode}`)
        **SLA:** {spec.sla.freshness_minutes} min freshness · mode: `{spec.sla.mode}`
        **Schedule:** {sched_blurb}
        **Required approvers:** {", ".join(spec.required_approvers) or "(none configured)"}

        ## What gets deployed

        When this PR is approved and merged, the Databricks Pipeline Studio agent:

        1. Ingests bronze via Lakeflow Connect (Direct CDC) — the source
           tables are added to the CONSOLIDATED ingestion pipeline
           `ops-<env>-source-ingest` (reused across requests; no duplicate
           ingestion). Falls back to a SQL-warehouse one-shot only when the
           external Postgres source isn't configured.
        2. Deploys the multi-file project under `transformations/`
           (staging/bronze_views.py + silver/{spec.target.table}.py) as a real
           Spark Declarative Pipeline `ops-<env>-{spec.target.table}` via the
           SDK — pipeline root_path + glob `transformations/**`.
        3. Creates (or resets) a scheduled Job `job-silver-{spec.target.table}`
           that triggers the pipeline {sched_blurb}.
        4. Triggers an immediate pipeline update, polls until COMPLETED,
           verifies row counts.

        The pipeline + job are visible in **Workspace → Pipelines** and
        **Workspace → Jobs & Pipelines** with full lineage, schedule, event
        log, and run history.

        ## Validation

        - Not null: {spec.validation.not_null or "(none)"}
        - Unique: {spec.validation.unique or "(none)"}
        - Row count tolerance: ±{spec.validation.row_count_tolerance_pct}%

        ## Files

        ```
        pipelines/{spec.target.table}/
        ├── databricks.yml
        ├── spec.yaml
        ├── README.md  ← this file
        ├── resources/
        │   ├── pipeline.yml        SDP pipeline DAB resource (uses glob libs)
        │   ├── job.yml             Scheduled job that triggers the pipeline
        │   ├── connection.yml      Lakeflow Connect Postgres Connection (production pattern)
        │   └── ingestion.yml       Lakeflow Connect Gateway + IngestionPipeline
        ├── transformations/
        │   ├── staging/bronze_views.py    @dlt.view pass-throughs over ingested bronze
        │   └── silver/
        │       └── {spec.target.table}.py    @dlt.table for the silver layer
        └── tests/
            └── row_checks.sql      Validation SQL run post-deploy
        ```

        ## How this was generated

        Authored by **Databricks Pipeline Studio**'s agentic LangGraph workflow.
        The spec is at `spec.yaml`. Edit and re-run the agent to regenerate;
        do not hand-edit the SDP code unless you also update `spec.yaml`.
    """).lstrip()


_COMMENT_PREFIX = {
    ".py":   "# ",
    ".sql":  "-- ",
    ".yml":  "# ",
    ".yaml": "# ",
    ".md":   "<!-- ",
}


def _stamp(path: str, content: str, request_id: str, ts: str) -> str:
    """Prepend a generation-stamp header in the file's comment syntax so every
    generation produces a visible diff."""
    ext = "." + path.rsplit(".", 1)[-1] if "." in path else ""
    prefix = _COMMENT_PREFIX.get(ext)
    if prefix is None:
        return content
    if ext == ".md":
        header = f"<!-- generated by Databricks Pipeline Studio · request_id: {request_id} · at {ts} -->\n\n"
    else:
        header = (
            f"{prefix}generated by Databricks Pipeline Studio\n"
            f"{prefix}request_id: {request_id}\n"
            f"{prefix}generated_at: {ts}\n\n"
        )
    return header + content


def emit_all(spec: PipelineSpec, dev_catalog: str, prod_catalog: str, github_repo: str,
             secret_scope: str = "databricks-pipeline-studio",
             request_id: str | None = None,
             table_comment: str | None = None,
             column_comments: dict[str, dict[str, str]] | None = None,
             inline_comments: list[str] | None = None,
             silver_schema: str | None = None,
             silver_sql_select: str | None = None) -> dict[str, str]:
    """Emit the full file set for a generated pipeline bundle.

    The optional metadata parameters (table_comment, column_comments,
    inline_comments) come from the pipeline_generator LLM and drive UC
    metadata + code documentation quality.
    """
    import datetime as _dt

    base = PurePosixPath("pipelines") / spec.target.table
    rid = request_id or "unknown"
    ts = _dt.datetime.utcnow().isoformat() + "Z"

    raw_files: dict[str, str] = {
        str(base / "databricks.yml"):                          render_bundle_root(spec, dev_catalog, prod_catalog, github_repo),
        str(base / "spec.yaml"):                               yaml.safe_dump(spec.model_dump(mode="json"), sort_keys=False),
        str(base / "README.md"):                               render_readme(spec),
        str(base / "resources" / "pipeline.yml"):              render_pipeline_yml(spec),
        str(base / "resources" / "job.yml"):                   render_job_yml(spec),
        str(base / "resources" / "connection.yml"):            render_connection_yml(spec, secret_scope),
        str(base / "resources" / "ingestion.yml"):             render_ingestion_yml(spec, dev_catalog),
        str(base / "tests" / "row_checks.sql"): render_tests_yml(
            spec,
            # The REAL deployed table, not spec.target's logical `silver` schema —
            # otherwise the tests query a table that does not exist in dev.
            f"{dev_catalog}.{silver_schema}.{spec.target.table}" if silver_schema else None),
    }
    # Multi-file SDP project (transformations/staging + silver) — EXACTLY what
    # the agent deploys, so the PR reviewer sees the real artifact.
    from tools import real_deployer
    project = real_deployer.render_sdp_project(
        spec, rid, table_comment=table_comment,
        column_comments=column_comments, inline_comments=inline_comments,
        silver_sql_select=silver_sql_select)
    for rel, content in project.items():
        if rel == "README.md":
            continue  # keep the richer top-level README
        raw_files[str(base / rel)] = content
    return {path: _stamp(path, content, rid, ts) for path, content in raw_files.items()}
