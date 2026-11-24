"""MLflow models-from-code definition of the pipeline-builder agent.

This file IS the model. `scripts/register_model.py` logs it (plus the
agent/tools/common packages as code) and registers it in Unity Catalog as
    <catalog>.<control_schema>.pipeline_builder_agent
with the @champion alias. The per-request Job loads the model FROM UC at
execution time (see agent/run.py) — so:

  * model code (the LangGraph flow + skills) is maintained in git and
    versioned in UC as model versions;
  * agent configuration (per-agent prompts, loop settings, Confluence
    links) is DATA, read at runtime from the `agent_guidelines` Delta
    table — owned by the data engineering team, changeable without a
    model release.

predict() input: one row with columns request_id / mode / start_step.
predict() output: a JSON string per row with the final status.
"""

from __future__ import annotations

import json

import mlflow
from mlflow.models import set_model


class PipelineBuilderAgent(mlflow.pyfunc.PythonModel):

    def predict(self, context, model_input, params=None):
        if hasattr(model_input, "to_dict"):
            rows = model_input.to_dict(orient="records")
        elif isinstance(model_input, dict):
            rows = [model_input]
        else:
            rows = list(model_input)

        # Lazy import: the agent package arrives via code_paths and is on
        # sys.path by the time predict() runs.
        from agent.orchestrate import execute

        out = []
        for row in rows:
            result = execute(
                request_id=str(row["request_id"]),
                mode=str(row.get("mode") or "full"),
                start_step=(str(row["start_step"]) if row.get("start_step") else None),
            )
            out.append(json.dumps(result, default=str))
        return out


set_model(PipelineBuilderAgent())
