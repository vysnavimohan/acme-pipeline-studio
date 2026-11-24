---
name: genie-agent-author
description: >
  Author the two Acme Corp Genie Agents, Margin & Pricing and Working Capital: instructions, sample questions, example SQL
  validated before adding, and benchmarks. Use when the user asks to create, tune or document these Genie Agents, write their
  instructions, add example SQL, or set up benchmarks. Not for general data discovery (use databricks-data-discovery) or metric definitions (use metric-view-author).
metadata:
  version: "1.0.0"
  layer: domain
  domain: reporting
  type: workflow
  scope: workspace
  owner: "BI Lead"
---

# Genie Agent author: Acme Corp standard

Two agents, one per domain, each scoped to its `vmohan_demo_catalog.cfo_workbench.sem_` metric views. Pricing and Finance people ask
questions in their own words; the agent answers with the governed measure, not a re-derived number.

## Rules
1. **Agent scope:** "Margin & Pricing" uses `mv_revenue_margin` and `dim_product`, `dim_customer`, `dim_branch`. "Working Capital" uses `mv_working_capital` and `mv_inventory_ageing`.
2. **Instructions are declarative,** with no SQL in bullets. Example: "Use MEASURE() for every measure in vmohan_demo_catalog.cfo_workbench.mv_revenue_margin."
3. **Use the glossary names.** "Gross Margin Pct", "DSO Days", "Inventory Over 180 Days"; never "margin" or "days outstanding" alone.
4. **Validate every example SQL read-only** against `vmohan_demo_catalog` before it is added. Record the result in `assets/agent_config.json` (`validation_status`).
5. **Benchmarks are hard-coded** and use checked SQL for deterministic questions; the fasteners leakage question has an evaluation note.
6. **Fasteners and top-20 account questions** must cite the same measure the pulse brief uses; a disagreement is a bug in one of them.
7. **Config is code.** The agent definition lives in `assets/agent_config.json` and is applied through the Genie API; no hand edits in production.

## Steps
1. Confirm the 5–10 questions each team asks today (pricing, finance, credit control).
2. Check reuse first: an existing agent with the same sources wins.
3. Draft the config from `assets/agent_config.json`.
4. Validate the example SQL and benchmark SQL read-only in `vmohan_demo_catalog`; mark each as validated.
5. Create or update the agent through the Genie API; run each benchmark through the Conversation API; compare to the expected answer.
6. Each week, review negative feedback; add an example or instruction only where the cause is clear.

## Output contract
```
Agent: Margin & Pricing | Working Capital   Sources: <views>
Instructions: n   Sample questions: n   Example SQL: n (validated n)
Benchmarks: n (pass n / fail n)   Known gaps: ...
```

## References
- `assets/agent_config.json`: both agents, instructions, sample questions, example SQL and benchmarks.
- `../metric-view-author/SKILL.md`: the measures.
- `../business-glossary/SKILL.md`: term names.

## Company-specific notes
- Both agents run on the serverless SQL warehouse; no classic cluster is referenced.
- Benchmarks use synthetic data values for the show-and-tell; re-derive them from the live views before any customer-facing run.
