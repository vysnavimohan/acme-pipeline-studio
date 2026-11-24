"""LangGraph wiring for the multi-agent pipeline builder.

Three builders, all sharing the same node-and-edge topology:
- build_graph()             — full run from spec_critic onward
- build_gate_subgraph()     — used by /retry-prod-deploy (verify_pr_approved + deploy_prod only)
- build_subgraph_from(step) — used by /rerun-from-step (same wiring, but START → step)
"""

from __future__ import annotations

from langgraph.graph import StateGraph, START, END

from agent import nodes, guidelines
# RERUNNABLE_STEPS lives in the langgraph-free state module so the App route can
# import it without pulling in langgraph; re-exported here for callers that
# import it from agent.graph.
from agent.state import AgentState, StepName, RERUNNABLE_STEPS  # noqa: F401


def _reviewer_cfg() -> dict:
    """Wrapper so we can monkey-patch in tests."""
    return guidelines.get_config("pr_reviewer")


def _wire_all_nodes_and_edges(g: StateGraph) -> None:
    """Add every node + every conditional/normal edge to the graph.
    Does NOT add the START edge — callers wire that themselves so they
    can choose the entry node (full graph vs sub-graph)."""
    g.add_node("spec_critic", nodes.spec_critic)
    g.add_node("source_profiler", nodes.source_profiler)
    g.add_node("pipeline_generator", nodes.pipeline_generator)
    g.add_node("standards_validator", nodes.standards_validator)
    g.add_node("test_generator", nodes.test_generator)
    g.add_node("deploy_ingestion", nodes.deploy_ingestion)
    g.add_node("deploy_dev", nodes.deploy_dev)
    g.add_node("deploy_gold", nodes.deploy_gold)
    g.add_node("app_generator", nodes.app_generator)
    g.add_node("app_standards_validator", nodes.app_standards_validator)
    g.add_node("branch_and_pr", nodes.branch_and_pr)
    g.add_node("pr_reviewer", nodes.pr_reviewer)
    g.add_node("await_human", nodes.await_human)
    g.add_node("verify_pr_approved", nodes.verify_pr_approved)
    g.add_node("deploy_staging", nodes.deploy_staging)

    # Spec Critic loop: keep iterating until spec_locked OR sent_to_queue.
    def _from_spec_critic(s: AgentState) -> str:
        if s.sent_to_queue:
            return "end"
        return "source_profiler" if s.spec_locked else "spec_critic"

    g.add_conditional_edges(
        "spec_critic",
        _from_spec_critic,
        {"source_profiler": "source_profiler", "spec_critic": "spec_critic", "end": END},
    )
    # source_profiler hard-fails when a requested table doesn't exist —
    # stop before generating code for tables we can't read.
    g.add_conditional_edges(
        "source_profiler",
        lambda s: "end" if s.fatal_error else "pipeline_generator",
        {"end": END, "pipeline_generator": "pipeline_generator"},
    )
    g.add_edge("pipeline_generator", "standards_validator")
    g.add_edge("standards_validator", "test_generator")
    g.add_edge("test_generator", "deploy_ingestion")

    # If Lakeflow Connect ingestion fails, end early (skipped = proceed,
    # deploy_dev falls back to the warehouse bronze strategy).
    g.add_conditional_edges(
        "deploy_ingestion",
        lambda s: "end" if s.fatal_error else "deploy_dev",
        {"end": END, "deploy_dev": "deploy_dev"},
    )

    # If dev deploy fails, end early — we don't want to open a broken PR.
    # On success → Curate (Gold) step (no-op when no gold requested) → then PR,
    # so the PR + review + promotion cover the whole medallion in one request.
    g.add_conditional_edges(
        "deploy_dev",
        lambda s: "end" if s.fatal_error else "deploy_gold",
        {"end": END, "deploy_gold": "deploy_gold"},
    )
    # After Curate (Gold), the optional Consume step: generate + audit a governed
    # Databricks App on the Gold layer. Both nodes no-op (SKIPPED) when
    # spec.app.enabled is False, so pure-pipeline requests pass straight through
    # to the PR. The app rides the SAME PR + review + promotion as the pipeline.
    g.add_edge("deploy_gold", "app_generator")
    g.add_edge("app_generator", "app_standards_validator")
    g.add_edge("app_standards_validator", "branch_and_pr")
    g.add_conditional_edges(
        "branch_and_pr",
        lambda s: "end" if s.fatal_error else "pr_reviewer",
        {"end": END, "pr_reviewer": "pr_reviewer"},
    )

    # Reviewer ↔ Coder loop: if verdict=request_changes AND iterations remain,
    # loop back to pipeline_generator with the feedback. Otherwise → await_human.
    def _from_reviewer(s: AgentState) -> str:
        cfg = _reviewer_cfg()
        max_iters = max(1, int(cfg.get("max_iterations") or 1))
        if s.review_verdict == "request_changes" and s.review_iterations < max_iters:
            return "pipeline_generator"
        return "await_human"

    g.add_conditional_edges(
        "pr_reviewer",
        _from_reviewer,
        {"pipeline_generator": "pipeline_generator", "await_human": "await_human"},
    )

    # Human-approval pause: a fresh run ENDS at await_human (the request
    # parks in pending_review; approval launches a new gate-mode run).
    # Only when the decision is ALREADY approved (gate re-runs, re-runs
    # after approval) does the flow continue into the approval gate.
    def _from_await(s: AgentState) -> str:
        return "verify_pr_approved" if s.human_decision == "approved" else "end"

    g.add_conditional_edges(
        "await_human",
        _from_await,
        {"verify_pr_approved": "verify_pr_approved", "end": END},
    )

    def _from_gate(s: AgentState) -> str:
        if s.human_decision != "approved":
            return "end"
        if s.pr_approval_error:
            return "end"
        return "deploy_staging"

    g.add_conditional_edges(
        "verify_pr_approved",
        _from_gate,
        {"deploy_staging": "deploy_staging", "end": END},
    )
    g.add_edge("deploy_staging", END)


def build_graph():
    """Full graph: START → spec_critic → ... → deploy_prod → END."""
    g = StateGraph(AgentState)
    _wire_all_nodes_and_edges(g)
    g.add_edge(START, "spec_critic")
    return g.compile()


def build_gate_subgraph():
    """Mini graph used by approve / retry-staging-deploy: only
    verify_pr_approved → deploy_staging.

    Built as a TWO-node graph (not a full-wiring sub-graph) to avoid
    spec_critic / source_profiler being side-effect-reachable via the
    conditional edges in the full topology.
    """
    g = StateGraph(AgentState)
    g.add_node("verify_pr_approved", nodes.verify_pr_approved)
    g.add_node("deploy_staging", nodes.deploy_staging)
    g.add_edge(START, "verify_pr_approved")

    def _from_gate(s: AgentState) -> str:
        if s.human_decision != "approved":
            return "end"
        if s.pr_approval_error:
            return "end"
        return "deploy_staging"

    g.add_conditional_edges(
        "verify_pr_approved",
        _from_gate,
        {"deploy_staging": "deploy_staging", "end": END},
    )
    g.add_edge("deploy_staging", END)
    return g.compile()


def build_promote_subgraph():
    """Mini graph used by Release Management to promote a staged request to
    PROD (mode='promote'). The PR is already merged and the pipeline already
    runs in staging, so this is a single node: deploy_prod → END."""
    g = StateGraph(AgentState)
    g.add_node("deploy_prod", nodes.deploy_prod)
    g.add_edge(START, "deploy_prod")
    g.add_edge("deploy_prod", END)
    return g.compile()


def build_gold_subgraph():
    """mode='gold' — build a Gold fact/dim MATERIALIZED VIEW from Silver
    (single node: gold_builder → END). The agent generates + validates the
    fact SQL and materialises it via the consolidated ops-<env>-gold pipeline."""
    g = StateGraph(AgentState)
    g.add_node("gold_builder", nodes.gold_builder)
    g.add_edge(START, "gold_builder")
    g.add_edge("gold_builder", END)
    return g.compile()


def build_subgraph_from(step: StepName):
    """Re-execute sub-graph for /rerun-from-step.

    Uses the same full wiring as build_graph(), but START → `step`. All
    downstream conditional edges fire identically, so e.g. re-running from
    `pipeline_generator` still loops through the standards_validator →
    test_generator → deploy_dev → ... chain. Re-running from `pr_reviewer`
    can still loop back to pipeline_generator if the reviewer requests
    changes (provided review_iterations < max_iterations).
    """
    if step not in RERUNNABLE_STEPS:
        raise ValueError(f"Cannot re-run from step {step!r}; valid: "
                         f"{[s.value for s in RERUNNABLE_STEPS]}")
    g = StateGraph(AgentState)
    _wire_all_nodes_and_edges(g)
    g.add_edge(START, step.value)
    return g.compile()
