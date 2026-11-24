"""Typed state schema for the LangGraph multi-agent pipeline builder.

The state object flows through every node. Each node mutates a small subset.
The Streamlit app polls events to drive the request tracker.
"""

from __future__ import annotations

import datetime as dt
import enum
from typing import Annotated, Any, Literal, Optional

import operator
from pydantic import BaseModel, Field


# =====================================================================
# Spec — what the user fills in the 5-box form
# =====================================================================

class FileColumn(BaseModel):
    """A column discovered by profiling a landing-zone file. Bronze keeps every
    column as STRING (faithful 1:1 landing); `type` is a Silver-stage hint.

    `name` is the (possibly agent-renamed, snake_case) column name used for the
    Bronze/Silver columns; `source_name` is the ORIGINAL header token in the
    file — needed to drop the header line, which a rename would otherwise miss."""
    name: str
    type: str = "string"
    is_json: bool = False
    source_name: Optional[str] = None
    # P1 Silver flatten: for a JSON column, the agent-selected, request-relevant
    # fields to promote into typed Silver columns. Each entry:
    # {"path": "OFFER_ID", "alias": "offer_id", "type": "string", "comment": "..."}.
    # Empty → the JSON column is kept as a raw string in Silver (no flatten).
    json_fields: list[dict] = Field(default_factory=list)


class FileSource(BaseModel):
    """Auto Loader (cloudFiles) ingestion from a landing-zone Volume dataset.
    Present when SourceSpec.kind == 'file'."""
    dataset: str                       # landing-zone dataset dir, e.g. "cust_comm_hist"
    landing_path: str                  # cloudFiles glob, e.g. /Volumes/.../cust_comm_hist/*.dat
    file_format: str = "text"          # cloudFiles.format ("text" for multi-char delim, else "csv")
    delimiter: str = ""                # e.g. "@$!~" (empty when single-col / native csv)
    has_header: bool = True
    columns: list[FileColumn] = Field(default_factory=list)
    # The cleansed Silver table this dataset lands into. Used in the MULTI-dataset
    # path (SourceSpec.files); defaults to the dataset name when empty.
    silver_table: Optional[str] = None


class SourceSpec(BaseModel):
    # kind="cdc" → Lakeflow Connect from Postgres (BILLING/NETWORK, the secondary
    # mode). kind="file" → Auto Loader from a landing-zone Volume (the primary
    # Databricks path). `system` is the logical source system name.
    kind: Literal["cdc", "file", "gold"] = "cdc"   # gold: build a fact/dim from Silver
    system: str                          # cdc: "billing"/"network"; file: logical system e.g. "udp"
    tables: list[str]                    # cdc: schema.table list; file: [dataset(s)]; gold: source silver/gold tables
    incremental_column: Optional[str] = None
    primary_key: list[str] = Field(default_factory=list)
    file: Optional[FileSource] = None    # populated when kind == "file" (single-dataset)
    # MULTI-dataset file requests: one FileSource per landing dataset. When
    # non-empty, ONE request ingests every dataset → its own Bronze + Silver,
    # and the Gold facts/dims join/aggregate across the resulting silvers.
    files: list[FileSource] = Field(default_factory=list)

    model_config = {"extra": "ignore"}


class TargetSpec(BaseModel):
    catalog: str                # e.g. databricks_pipeline_studio_dev (the agent uses dev_catalog from variables)
    target_schema: str = "silver"
    table: str
    write_mode: Literal["overwrite", "append", "merge"] = "merge"


class TransformSpec(BaseModel):
    description: str            # plain English what to do
    join_with: Optional[dict[str, Any]] = None
    type_alignment: Literal["strict_to_source", "permissive"] = "strict_to_source"
    business_rules: list[str] = Field(default_factory=list)


class ValidationSpec(BaseModel):
    not_null: list[str] = Field(default_factory=list)
    unique: list[str] = Field(default_factory=list)
    row_count_tolerance_pct: float = 5.0


class SLASpec(BaseModel):
    # "batch" runs on a schedule. "streaming" runs SDP in continuous mode;
    # Lakeflow Connect ingestion runs every 10 min (its current min cadence).
    mode: Literal["batch", "streaming"] = "batch"
    freshness_minutes: int = 60
    schedule_cron: Optional[str] = None     # if None, on-demand only


class GovernanceSpec(BaseModel):
    """P3: mandatory governance metadata the standards validator enforces and the
    deploy stamps onto the UC table (owner / domain / classification)."""
    data_owner: str = ""
    business_owner: str = ""
    domain: str = ""
    classification: str = ""          # public | internal | confidential | restricted


class GoldFactSpec(BaseModel):
    """An optional Gold fact/dimension built from the request's Silver (the
    'curate' step). The agent generates + validates the SQL at build time.
    `sources` defaults to the request's own Silver target when empty."""
    name: str                          # gold table name, e.g. ppad_rf_fact_cam_metr
    intent: str = ""                   # what the fact/dim is, in business terms
    grain: str = ""                    # grain / measures hint (e.g. "per msisdn,cam_id")
    sources: list[str] = Field(default_factory=list)   # silver/gold tables; default = this request's silver


class AppKPI(BaseModel):
    """One headline metric on the generated app's KPI strip. `column` and `agg`
    reference a Gold table column; `format` controls display (money/pct/int)."""
    label: str
    column: str
    agg: Literal["sum", "avg", "count", "max", "min", "first"] = "sum"
    format: Literal["money", "pct", "int", "float", "text"] = "int"
    help: str = ""


class AppChart(BaseModel):
    """One chart/table panel on the generated app. `x`/`y` reference Gold columns.
    `kind`: bar | line | table (the shapes the deterministic emitter renders)."""
    title: str
    kind: Literal["bar", "line", "table"] = "bar"
    x: str = ""
    y: str = ""
    source_table: str = ""     # gold table name; default = the app's primary gold table
    limit: int = 50


class AppSpec(BaseModel):
    """OPTIONAL Databricks App to build ON TOP OF the request's Gold layer — the
    'consume' step after Curate. `enabled=False` (the default) leaves every
    existing pipeline-only flow byte-identical. What to build is captured in the
    intake spec; HOW it must be built (framework, serving, auth, tenancy) is
    governed by config app-standards + the app_standards_validator agent.

    The app_generator agent turns this + the Gold contract into a deterministic
    app MANIFEST; app_emitter stamps a proven Streamlit template from it (config
    not code — mirrors how pipelines are generated)."""
    enabled: bool = False
    name: str = ""                         # app name, e.g. "funding-optimisation-console"
    title: str = ""                        # header title shown in the app
    subtitle: str = ""
    audience: str = ""                     # who uses it + the decision it drives
    # Which Gold tables the app reads. Default = the request's gold_facts.
    gold_tables: list[str] = Field(default_factory=list)
    # Single-tenant scoping: the column the app filters on (e.g. facility/program).
    # Empty → no tenant scoping (a single global view).
    tenant_column: str = ""
    kpis: list[AppKPI] = Field(default_factory=list)
    charts: list[AppChart] = Field(default_factory=list)
    include_genie: bool = True             # embed an NL panel scoped to the same Gold
    include_writeback: bool = False        # decision-capture to an audit table
    # Branding (falls back to config OPS_BRAND_* / Databricks defaults at emit).
    brand_name: str = ""

    model_config = {"extra": "ignore"}


class PipelineSpec(BaseModel):
    """The pipeline spec — ingestion → bronze → silver → (optional) gold →
    (optional) app. One request can build the whole medallion AND a governed
    Databricks App on top. Serve/BI beyond that stays Databricks-owned."""
    intent: str
    source: SourceSpec
    target: TargetSpec
    transform: TransformSpec
    validation: ValidationSpec
    sla: SLASpec = Field(default_factory=SLASpec)
    governance: GovernanceSpec = Field(default_factory=GovernanceSpec)
    # Optional Gold layer built from this request's Silver (the 'curate' step).
    gold_facts: list[GoldFactSpec] = Field(default_factory=list)
    # Optional Databricks App built on the Gold layer (the 'consume' step).
    app: AppSpec = Field(default_factory=AppSpec)
    required_approvers: list[str] = Field(default_factory=list)
    requested_by: Optional[str] = None
    # True when the spec was confirmed through the interactive intake chat —
    # the Spec Critic then does a single-pass sanity check instead of the
    # full grilling loop (the human already got grilled).
    intake_confirmed: bool = False
    # Acme Corp data-product journey block (see agent/acme_journey.py). engine="genie_code"
    # routes the request to agent/genie_engine.py instead of the LangGraph agent job.
    data_product: dict[str, Any] = Field(default_factory=dict)

    # Pydantic v2: tolerate older spec_json blobs in Delta that still carry
    # a `serve` key; just ignore extras instead of erroring.
    model_config = {"extra": "ignore"}

    def is_multi_dataset(self) -> bool:
        """True when this is a MULTI-dataset file request (one request → many
        landing datasets → many Bronze/Silver tables → shared Gold)."""
        return self.source.kind == "file" and bool(self.source.files)

    def dataset_specs(self) -> list["PipelineSpec"]:
        """Per-dataset sub-specs for the file medallion. Each sub-spec looks like
        a normal SINGLE-dataset request (source.file set, one target Silver), so
        every existing single-dataset deploy function works unchanged in a loop.
        Single-dataset / CDC requests return ``[self]`` (byte-identical path)."""
        if not self.is_multi_dataset():
            return [self]
        out: list[PipelineSpec] = []
        for f in self.source.files:
            sub = self.model_copy(deep=True)
            sub.source.file = f.model_copy(deep=True)
            sub.source.files = []
            sub.source.tables = [f.dataset]
            sub.target = self.target.model_copy(deep=True)
            sub.target.table = f.silver_table or f.dataset
            sub.gold_facts = []          # Gold is built ONCE at the request level
            out.append(sub)
        return out

    def all_silver_tables(self) -> list[str]:
        """Every Silver table this request produces (one per dataset for multi)."""
        if self.is_multi_dataset():
            return [f.silver_table or f.dataset for f in self.source.files]
        return [self.target.table]


# =====================================================================
# Per-step state
# =====================================================================

class StepName(str, enum.Enum):
    QUEUED = "queued"
    INITIATED = "initiated"
    SPEC_CRITIC = "spec_critic"
    SOURCE_PROFILER = "source_profiler"
    PIPELINE_GENERATOR = "pipeline_generator"
    STANDARDS_VALIDATOR = "standards_validator"
    TEST_GENERATOR = "test_generator"
    DEPLOY_INGESTION = "deploy_ingestion"
    DEPLOY_DEV = "deploy_dev"
    BRANCH_AND_PR = "branch_and_pr"
    PR_REVIEWER = "pr_reviewer"
    AWAIT_HUMAN = "await_human"
    VERIFY_PR_APPROVED = "verify_pr_approved"
    DEPLOY_STAGING = "deploy_staging"
    DEPLOY_APP = "deploy_app"         # deploy the generated Databricks App (post-approval)
    DEPLOY_PROD = "deploy_prod"       # prod release (promotion) only
    GOLD_BUILDER = "gold_builder"     # P2: standalone Gold request (mode=gold)
    DEPLOY_GOLD = "deploy_gold"       # in-flow Gold (curate) step after Silver
    APP_GENERATOR = "app_generator"          # author the app manifest from Gold + AppSpec
    APP_STANDARDS_VALIDATOR = "app_standards_validator"  # enforce app standards
    DONE = "done"


# Steps a request can be re-run from (POST /rerun-from-step). Defined in this
# langgraph-free module so the App route can import it WITHOUT importing
# agent.graph (which imports langgraph — intentionally absent from the App
# container; only the Job has it).
RERUNNABLE_STEPS: list["StepName"] = [
    StepName.SPEC_CRITIC, StepName.SOURCE_PROFILER, StepName.PIPELINE_GENERATOR,
    StepName.STANDARDS_VALIDATOR, StepName.TEST_GENERATOR, StepName.DEPLOY_INGESTION,
    StepName.DEPLOY_DEV, StepName.DEPLOY_GOLD, StepName.APP_GENERATOR,
    StepName.APP_STANDARDS_VALIDATOR, StepName.BRANCH_AND_PR, StepName.PR_REVIEWER,
    StepName.AWAIT_HUMAN, StepName.VERIFY_PR_APPROVED, StepName.DEPLOY_STAGING,
]


class StepStatus(str, enum.Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"


class StepEvent(BaseModel):
    timestamp: dt.datetime = Field(default_factory=dt.datetime.utcnow)
    step: StepName
    status: StepStatus
    message: str = ""
    details: dict[str, Any] = Field(default_factory=dict)


# =====================================================================
# Overall agent state
# =====================================================================

class AgentState(BaseModel):
    """The full state passed through every LangGraph node."""

    request_id: str
    spec: PipelineSpec

    # critic loop
    critic_iterations: int = 0
    critic_questions: list[str] = Field(default_factory=list)
    spec_locked: bool = False
    sent_to_queue: bool = False    # Spec Critic gave up; back to queue

    # reviewer ↔ coder loop
    review_iterations: int = 0
    review_feedback: list[str] = Field(default_factory=list)
    review_verdict: Optional[str] = None  # "approve" | "request_changes"

    # generation outputs
    profile: dict[str, Any] = Field(default_factory=dict)
    artifacts: dict[str, str] = Field(default_factory=dict)
    """Path-keyed map of generated files (relative paths -> file contents)."""

    standards_violations: list[str] = Field(default_factory=list)
    tests_generated: list[str] = Field(default_factory=list)

    # App generation (the 'consume' step). app_manifest is the LLM-authored,
    # deterministic description the emitter renders; app_violations are what the
    # app_standards_validator found. Empty when spec.app.enabled is False.
    app_manifest: dict[str, Any] = Field(default_factory=dict)
    app_violations: list[str] = Field(default_factory=list)
    app_deploy: dict[str, Any] = Field(default_factory=dict)

    # rich metadata from the pipeline_generator LLM (drives SDP @dlt.table
    # comment + schema= DDL with COMMENT clauses + inline transformation
    # comments — flows into UC and shows in Catalog Explorer / Genie)
    generator_table_comment: Optional[str] = None
    generator_column_comments: dict[str, dict[str, Any]] = Field(default_factory=dict)
    generator_inline_comments: list[str] = Field(default_factory=list)

    # PR
    branch_name: Optional[str] = None
    pr_url: Optional[str] = None
    pr_number: Optional[int] = None
    ai_review_comment_id: Optional[int] = None
    diff_summary: dict[str, Any] = Field(default_factory=dict)
    # Deploy outputs (source → bronze → silver in {dev,prod} catalogs)
    dev_deploy: dict[str, Any] = Field(default_factory=dict)
    prod_deploy: dict[str, Any] = Field(default_factory=dict)
    # Lakeflow Connect ingestion result (gateway + ingestion pipeline ids);
    # empty when the warehouse bronze strategy was used.
    ingestion: dict[str, Any] = Field(default_factory=dict)

    # human review
    human_decision: Literal["pending", "approved", "rejected"] = "pending"
    human_comments: list[str] = Field(default_factory=list)

    # Per-step operator comments. Keyed by StepName.value. Each node reads
    # `state.human_feedback_per_step.get(self.step_name, [])` and includes it
    # in its prompt. Persisted to the request_events table as `human_comment`
    # events so they're rehydrated on /rerun-from-step.
    human_feedback_per_step: dict[str, list[str]] = Field(default_factory=dict)
    # Set when /rerun-from-step starts a partial sub-graph from this node
    # onward. Nodes can use this to know they're in a re-run context.
    rerun_from_step: Optional[str] = None

    # PR approval gate (between await_human and deploy_prod)
    pr_review_state: dict[str, Any] = Field(default_factory=dict)
    pr_approval_error: Optional[str] = None

    # deploy
    deploy_dev_status: Literal["pending", "running", "succeeded", "failed"] = "pending"
    deploy_prod_status: Literal["pending", "running", "succeeded", "failed"] = "pending"

    # error tracking
    fatal_error: Optional[str] = None
    retry_count: int = 0

    # event log — append-only via operator.add
    events: Annotated[list[StepEvent], operator.add] = Field(default_factory=list)

    def emit(self, step: StepName, status: StepStatus, message: str = "", **details: Any) -> dict:
        """Returns a partial-state update dict that LangGraph will merge.

        Use as: `return state.emit(StepName.X, StepStatus.SUCCEEDED, "...")`
        """
        ev = StepEvent(step=step, status=status, message=message, details=details)
        return {"events": [ev]}
