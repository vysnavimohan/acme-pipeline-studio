"""Deterministic Databricks App emitter.

Pipeline Studio's philosophy (ARCHITECTURE.md): *the agent authors configuration,
not bespoke code; a deterministic versioned engine runs it.* We apply the same to
apps. The `app_generator` LLM agent authors an app **manifest** (which Gold columns
become which KPIs / charts, tenancy, Genie, writeback); THIS module deterministically
stamps a proven Streamlit template from that manifest. That keeps generated apps
reviewable in the PR and consistent every run — versus free-form code-gen of app.py,
which is exactly the anti-pattern the repo avoids for pipelines.

The template is a single-scope governed dashboard (KPI strip, drill charts, an
embedded Genie NL panel, and optional decision writeback).

`emit_app(manifest) -> {relative_path: file_contents}` is PURE (no I/O, no LLM, no
warehouse) so it is fully unit-testable. The caller writes the returned files into
the generated bundle / deploys them as a Databricks App.
"""

from __future__ import annotations

import json
from typing import Any


# --- manifest shape (authored by app_generator, validated by app_standards_validator)
# {
#   "name": "margin-and-cash-tracker",
#   "title": "Acme Corp · Margin & Cash Tracker",
#   "subtitle": "…",
#   "catalog": "classic_sandbox_hdya7g_catalog",
#   "gold_schema": "fo_dev_gold",
#   "primary_table": "margin_by_branch",
#   "tenant_column": "",                      # "" = no tenant scoping
#   "kpis": [{"label","column","agg","format","help"}, ...],
#   "charts": [{"title","kind","x","y","source_table","limit"}, ...],
#   "include_genie": true,
#   "genie_space_id": "",                     # filled at deploy if created
#   "include_writeback": true,
#   "warehouse_id": "",
#   "brand": {"name","accent","deep","link"}, # optional; falls back to Databricks
# }


_FMT_HELPERS = '''\
def _money(x):
    try: return f"${float(x):,.0f}"
    except Exception: return str(x)

def _pct(x):
    try: return f"{float(x):.1%}"
    except Exception: return str(x)

def _int(x):
    try: return f"{int(float(x)):,}"
    except Exception: return str(x)

def _fmt(val, kind):
    return {"money": _money, "pct": _pct, "int": _int}.get(kind, lambda v: str(v))(val)
'''


def _agg_sql(agg: str, column: str) -> str:
    agg = (agg or "sum").lower()
    col = f"`{column}`"
    if agg == "count":
        return "COUNT(*)"
    if agg in ("sum", "avg", "max", "min"):
        return f"{agg.upper()}({col})"
    if agg == "first":
        return f"FIRST({col})"
    return f"SUM({col})"


def _fq(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def _validate_manifest(m: dict[str, Any]) -> list[str]:
    """Structural validation — returns human-readable problems. The
    app_standards_validator adds policy-level checks on top of these."""
    problems: list[str] = []
    for req in ("name", "title", "catalog", "gold_schema", "primary_table"):
        if not str(m.get(req) or "").strip():
            problems.append(f"manifest missing required field: {req}")
    name = str(m.get("name") or "")
    if name and (" " in name or not all(c.isalnum() or c in "-_" for c in name)):
        problems.append(f"app name {name!r} must be alnum/hyphen/underscore only")
    if not m.get("kpis") and not m.get("charts"):
        problems.append("manifest has no kpis and no charts — nothing to render")
    return problems


def _data_py(m: dict[str, Any]) -> str:
    """The data layer: reads Gold via the SQL warehouse using the Databricks SDK
    (StatementExecution). Single-tenant scoping is enforced HERE — every query
    filters to the selected tenant, mirroring the fo-app's segregation model."""
    catalog = m["catalog"]; schema = m["gold_schema"]; primary = m["primary_table"]
    tenant = str(m.get("tenant_column") or "")
    return f'''\
"""Data layer for the generated console. Reads the Gold layer via the SQL
warehouse (Databricks SDK StatementExecution). Governed end to end: the app SP's
UC grants decide what it can read. Single-tenant scoping is enforced in every
query (no cross-tenant read exists in this module)."""
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

CATALOG = os.getenv("APP_CATALOG", {catalog!r})
GOLD_SCHEMA = os.getenv("APP_GOLD_SCHEMA", {schema!r})
PRIMARY_TABLE = os.getenv("APP_PRIMARY_TABLE", {primary!r})
TENANT_COLUMN = os.getenv("APP_TENANT_COLUMN", {tenant!r})
WAREHOUSE_ID = os.getenv("APP_WAREHOUSE_ID", os.getenv("DATABRICKS_WAREHOUSE_ID", ""))

_w = WorkspaceClient()


def _fqn(table):
    return f"{{CATALOG}}.{{GOLD_SCHEMA}}.{{table}}"


def query(sql):
    """Run a SQL statement on the warehouse and return (columns, rows)."""
    r = _w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=sql, wait_timeout="30s")
    # poll if still running
    while r.status and r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        r = _w.statement_execution.get_statement(r.statement_id)
    cols = [c.name for c in (r.manifest.schema.columns if r.manifest and r.manifest.schema else [])]
    rows = (r.result.data_array if r.result and r.result.data_array else []) or []
    return cols, rows


def tenants():
    """Distinct tenant values (for the scope selector). Empty list = no scoping."""
    if not TENANT_COLUMN:
        return []
    _, rows = query(f"SELECT DISTINCT `{{TENANT_COLUMN}}` FROM {{_fqn(PRIMARY_TABLE)}} "
                    f"WHERE `{{TENANT_COLUMN}}` IS NOT NULL ORDER BY 1")
    return [r[0] for r in rows]


def _scope(tenant):
    if TENANT_COLUMN and tenant:
        safe = str(tenant).replace("'", "''")
        return f"WHERE `{{TENANT_COLUMN}}` = '{{safe}}'"
    return ""


def kpi_row(tenant, select_sql):
    cols, rows = query(f"SELECT {{select_sql}} FROM {{_fqn(PRIMARY_TABLE)}} {{_scope(tenant)}}")
    return dict(zip(cols, rows[0])) if rows else {{}}


def chart_data(table, select_sql, order_sql, limit, tenant):
    return query(f"SELECT {{select_sql}} FROM {{_fqn(table)}} {{_scope(tenant)}} "
                 f"{{order_sql}} LIMIT {{int(limit)}}")
'''


def _app_py(m: dict[str, Any]) -> str:
    """The Streamlit app. KPI strip + drill charts + optional Genie panel +
    optional writeback, single-tenant-scoped like the fo-app."""
    kpis = m.get("kpis") or []
    charts = m.get("charts") or []
    tenant = str(m.get("tenant_column") or "")
    include_genie = bool(m.get("include_genie"))
    include_wb = bool(m.get("include_writeback"))
    primary = m["primary_table"]

    # Build the KPI SELECT (aliased so we can read values back by label-key).
    kpi_selects = []
    kpi_meta = []
    for i, k in enumerate(kpis):
        alias = f"kpi_{i}"
        kpi_selects.append(f"{_agg_sql(k.get('agg'), k['column'])} AS {alias}")
        kpi_meta.append({"alias": alias, "label": k["label"],
                         "format": k.get("format", "int"), "help": k.get("help", "")})
    kpi_select_sql = ", ".join(kpi_selects) if kpi_selects else "COUNT(*) AS kpi_0"

    header_title = m.get("title") or m.get("name")
    subtitle = m.get("subtitle") or ""

    parts: list[str] = []
    parts.append(f'''\
"""{header_title}

Generated by Databricks Pipeline Studio (app_emitter) from an LLM-authored manifest.
Reads the request's Gold layer; every model call in the build that produced this app
was governed by Unity AI Gateway. Single-tenant-scoped: one tenant at a time, no
cross-tenant read."""
import os
import streamlit as st
import pandas as pd

import data as D
import theme as T

st.set_page_config(page_title={header_title!r}, layout="wide", page_icon="\U0001f3e6")
T.inject_css()

{_FMT_HELPERS}

T.header({header_title!r}, {subtitle!r})
''')

    # tenant scope selector
    if tenant:
        parts.append(f'''
_tenants = []
try:
    _tenants = D.tenants()
except Exception as e:
    st.error(f"Could not load tenants: {{e}}")
scope = None
if _tenants:
    c1, c2 = st.columns([3, 2])
    with c2:
        scope = st.selectbox("\U0001f512 {tenant} in scope", _tenants)
    st.info(f"**Scope: {{scope}}.** This session shows this {tenant} only — no other "
            f"tenant's data is queried or displayed.", icon="\U0001f512")
''')
    else:
        parts.append('\nscope = None\n')

    # KPI strip
    parts.append(f'''
# ── KPI strip ──────────────────────────────────────────────────────────────
_kpi_meta = {json.dumps(kpi_meta)}
try:
    _vals = D.kpi_row(scope, {kpi_select_sql!r})
    cols = st.columns(max(1, len(_kpi_meta)))
    for col, meta in zip(cols, _kpi_meta):
        col.metric(meta["label"], _fmt(_vals.get(meta["alias"]), meta["format"]),
                   help=meta.get("help") or None)
except Exception as e:
    st.error(f"Could not load KPIs: {{e}}")

st.divider()
''')

    # tabs
    tab_labels = ['"\U0001f4ca Dashboard"']
    if include_genie:
        tab_labels.append('"\U0001f4ac Ask Genie"')
    if include_wb:
        tab_labels.append('"\U0001f4dd Decision log"')
    parts.append(f'\n_tabs = st.tabs([{", ".join(tab_labels)}])\n')

    # dashboard tab: charts
    chart_code = []
    for i, ch in enumerate(charts):
        tbl = ch.get("source_table") or primary
        x = ch.get("x", ""); y = ch.get("y", "")
        kind = ch.get("kind", "bar"); limit = int(ch.get("limit", 50))
        if not x or not y:
            sel = "*"
            order = ""
        else:
            sel = f"`{x}`, {_agg_sql('sum', y)} AS `{y}`"
            order = f"GROUP BY `{x}` ORDER BY `{y}` DESC"
        chart_code.append(f'''
    st.markdown("#### {ch.get('title','') }")
    try:
        _c, _r = D.chart_data({tbl!r}, {sel!r}, {order!r}, {limit}, scope)
        _df = pd.DataFrame(_r, columns=_c)
        if {kind!r} == "table" or _df.empty:
            st.dataframe(_df, use_container_width=True, hide_index=True)
        else:
            import plotly.express as px
            for _col in _df.columns[1:]:
                _df[_col] = pd.to_numeric(_df[_col], errors="coerce")
            _fn = px.line if {kind!r} == "line" else px.bar
            _fig = _fn(_df, x=_df.columns[0], y=_df.columns[1],
                       template=T.PLOTLY_TEMPLATE, color_discrete_sequence=[T.STEEL])
            _fig.update_layout(height=320, margin=dict(l=10, r=10, t=30, b=10))
            st.plotly_chart(_fig, use_container_width=True)
    except Exception as e:
        st.error(f"chart failed: {{e}}")
''')
    parts.append(f'''
with _tabs[0]:
{"".join(chart_code) if chart_code else "    st.caption('No charts configured.')"}
''')

    # genie tab
    if include_genie:
        gi = 1
        parts.append(f'''
with _tabs[{gi}]:
    space_id = os.getenv("APP_GENIE_SPACE_ID", {m.get("genie_space_id","")!r})
    if not space_id:
        st.info("Genie space not yet wired. Set APP_GENIE_SPACE_ID to enable the NL panel.")
    else:
        q = st.text_input("Ask a question about this data", placeholder="e.g. which segment has the most exposure?")
        if q:
            try:
                from databricks.sdk import WorkspaceClient
                w = WorkspaceClient()
                conv = w.genie.start_conversation_and_wait(space_id, q)
                st.write(getattr(conv, "content", None) or "See attachments in Genie.")
            except Exception as e:
                st.error(f"Genie call failed: {{e}}")
''')

    # writeback tab
    if include_wb:
        wi = (2 if include_genie else 1)
        parts.append(f'''
with _tabs[{wi}]:
    st.caption("Capture a decision. Written to the control schema audit table (governed).")
    note = st.text_area("Decision / note")
    if st.button("Record decision", type="primary") and note:
        try:
            audit = f"{{D.CATALOG}}.{{os.getenv('APP_CONTROL_SCHEMA','fo_dev_control')}}.app_decisions"
            safe = note.replace("'", "''"); sc = str(scope or "").replace("'", "''")
            D.query(f"CREATE TABLE IF NOT EXISTS {{audit}} (recorded_at TIMESTAMP, tenant STRING, note STRING) USING DELTA")
            D.query(f"INSERT INTO {{audit}} VALUES (current_timestamp(), '{{sc}}', '{{safe}}')")
            st.success("Recorded.")
        except Exception as e:
            st.error(f"writeback failed: {{e}}")
''')
    return "".join(parts)


def _theme_py(m: dict[str, Any]) -> str:
    """A compact institutional theme (subset of the fo-app theme): CSS header +
    metric cards + a Plotly template. Brand colours come from the manifest, else
    Databricks defaults."""
    brand = m.get("brand") or {}
    deep = brand.get("deep", "#1B3139")      # Databricks Navy
    accent = brand.get("accent", "#FF3621")  # Databricks Lava
    steel = brand.get("link", "#2272B4")     # Databricks Blue
    name = brand.get("name", "Databricks")
    logo = brand.get("logo", "")             # Optional logo path
    logo_alt = brand.get("logo_alt", name)
    return f'''\
"""Generated visual theme (institutional). One place for CSS + Plotly template."""
import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st

NAVY_DK = {deep!r}
ACCENT = {accent!r}
STEEL = {steel!r}
SLATE = "#5A6B7B"
LINE = "#E2E9F1"
BG = "#F5F8FC"
SEQ = [STEEL, ACCENT, "#2FA5A0", "#C8860D", "#5A6B7B", "#2E8540"]
LOGO = {logo!r}
LOGO_ALT = {logo_alt!r}


def inject_css():
    st.markdown(f"""
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
      html, body, [class*="css"], .stApp {{{{ font-family: 'Inter', sans-serif; color: {{NAVY_DK}}; }}}}
      .stApp {{{{ background: {{BG}}; }}}}
      .block-container {{{{ padding-top: 2.2rem; max-width: 1400px; }}}}
      .app-header {{{{ background: linear-gradient(100deg, {{NAVY_DK}} 0%, {{STEEL}} 130%);
        color:#fff; padding:18px 24px; border-radius:12px; margin-bottom:10px; }}}}
      .app-header {{{{ display:flex; align-items:center; gap:16px; }}}}
      .app-header-logo {{{{ height:40px; }}}}
      .app-header-logo img {{{{ height:100%; object-fit:contain; }}}}
      .app-header-text {{{{ flex:1; }}}}
      .app-header h1 {{{{ font-size:1.5rem; font-weight:700; margin:0; color:#fff; }}}}
      .app-header .sub {{{{ font-size:.85rem; opacity:.85; margin-top:2px; }}}}
      div[data-testid="stMetric"] {{{{ background:#fff; border:1px solid {{LINE}};
        border-radius:12px; padding:14px 16px; }}}}
      h3 {{{{ border-left:4px solid {{STEEL}}; padding-left:10px; }}}}
    </style>
    """, unsafe_allow_html=True)


def header(title, subtitle):
    logo_html = ""
    if LOGO:
        logo_html = f'<div class="app-header-logo"><img src="{{LOGO}}" alt="{{LOGO_ALT}}" /></div>'
    st.markdown(f'<div class="app-header">{{logo_html}}<div class="app-header-text"><h1>{{title}}'
                f'<span style="font-size:.6rem;background:rgba(255,255,255,.16);'
                f'padding:2px 8px;border-radius:6px;margin-left:8px;">{name.upper()} · SYNTHETIC</span>'
                f'</h1><div class="sub">{{subtitle}}</div></div></div>', unsafe_allow_html=True)


def _register():
    t = go.layout.Template()
    t.layout = go.Layout(font=dict(family="Inter, sans-serif", color=NAVY_DK, size=13),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", colorway=SEQ,
        margin=dict(l=10, r=10, t=44, b=10))
    pio.templates["appgen"] = t


_register()
PLOTLY_TEMPLATE = "appgen"
'''


def _app_yaml(m: dict[str, Any]) -> str:
    """The Databricks Apps manifest (command + env). Warehouse + Genie + tenant
    injected as env so the same code runs across dev/stage/prod."""
    env_lines = [
        f'  - name: APP_CATALOG\n    value: "{m["catalog"]}"',
        f'  - name: APP_GOLD_SCHEMA\n    value: "{m["gold_schema"]}"',
        f'  - name: APP_PRIMARY_TABLE\n    value: "{m["primary_table"]}"',
    ]
    if m.get("tenant_column"):
        env_lines.append(f'  - name: APP_TENANT_COLUMN\n    value: "{m["tenant_column"]}"')
    if m.get("warehouse_id"):
        env_lines.append(f'  - name: APP_WAREHOUSE_ID\n    value: "{m["warehouse_id"]}"')
    if m.get("genie_space_id"):
        env_lines.append(f'  - name: APP_GENIE_SPACE_ID\n    value: "{m["genie_space_id"]}"')
    env = "\n".join(env_lines)
    return (
        'command: ["streamlit", "run", "app.py", "--server.port", "8000", '
        '"--server.address", "0.0.0.0"]\n\nenv:\n' + env + "\n"
    )


def _requirements() -> str:
    # Pinned, matching the proven fo-app stack.
    return ("streamlit==1.40.2\n"
            "databricks-sdk==0.105.0\n"
            "plotly==5.24.1\n"
            "pandas==2.2.3\n")


def _readme(m: dict[str, Any]) -> str:
    return (
        f"# {m.get('title') or m.get('name')}\n\n"
        f"Generated by **Databricks Pipeline Studio** (app_emitter) from an LLM-authored "
        f"manifest. Reads the Gold layer `{_fq(m['catalog'], m['gold_schema'], m['primary_table'])}`.\n\n"
        f"Every model call in the build that produced this app was routed through "
        f"**Unity AI Gateway** (attribution + inference-table logging + policy + budgets).\n\n"
        f"## Manifest\n```json\n{json.dumps(m, indent=2)}\n```\n"
    )


def emit_app(manifest: dict[str, Any]) -> dict[str, str]:
    """Render the Databricks App bundle from the manifest. PURE — returns a
    {relative_path: contents} map. Raises ValueError on an invalid manifest so
    the caller (app_standards_validator / node) fails loudly, not silently."""
    problems = _validate_manifest(manifest)
    if problems:
        raise ValueError("invalid app manifest: " + "; ".join(problems))

    files: dict[str, str] = {}
    prefix = "app"
    files[f"{prefix}/app.yaml"] = _app_yaml(manifest)
    files[f"{prefix}/app.py"] = _app_py(manifest)
    files[f"{prefix}/data.py"] = _data_py(manifest)
    files[f"{prefix}/theme.py"] = _theme_py(manifest)
    files[f"{prefix}/requirements.txt"] = _requirements()
    files[f"{prefix}/README.md"] = _readme(manifest)
    return files


def validate_manifest(manifest: dict[str, Any]) -> list[str]:
    """Public structural validation (used by app_standards_validator)."""
    return _validate_manifest(manifest)
