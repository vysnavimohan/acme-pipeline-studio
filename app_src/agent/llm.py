"""LLM client. Calls the workspace's Foundation Models serving endpoint
directly via the Databricks SDK's auth — no external API key needed.

Unity AI Gateway governance: when `OPS_LLM_ENDPOINT` is a 3-part Unity Catalog
name (`catalog.schema.service`), it is treated as a UAIG **model service** and
the request is routed through the gateway (`/ai-gateway/openai/v1/chat/completions`)
instead of the raw serving-endpoint path. That makes every agent LLM call in this
accelerator governed: attributed to a requester, payload-logged to the service's
inference table, and subject to UC policies + budgets. A plain endpoint name
(e.g. `databricks-claude-sonnet-4-6`) still uses the serving-endpoint path
unchanged, so the asset stays portable to workspaces without a governed service.

Why hand-rolled vs `databricks_langchain.ChatDatabricks`: that package's
`__init__` chain transitively imports `databricks_openai` which in turn
imports `databricks.vector_search.client.VectorSearchIndex` — and that
symbol is gone in current versions of the vector-search SDK. The App
container crashes at startup with `ImportError`. We don't need vector
search or LangChain's OpenAI client; we just need to POST chat messages
to a serving endpoint and get a response. This shim does exactly that
and exposes the LangChain-style `.invoke([Message, ...]) → response`
interface the rest of the code already uses.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

LOG = logging.getLogger("ops.llm")


# The LLM serving endpoint is configurable so the solution can run against
# whatever Foundation Model / serving endpoint the host workspace has enabled
# (Databricks may use a different in-region endpoint than Claude). Precedence:
#   OPS_LLM_ENDPOINT  (our canonical knob, set by the one-line deploy)
#   DATABRICKS_LLM_ENDPOINT  (back-compat)
#   default: databricks-claude-sonnet-4-6
#
# NOTE (2026-08-03): the previous default `databricks-claude-sonnet-4` was
# RETIRED by Databricks — it now returns HTTP 400 "This endpoint is
# deprecated", which surfaced as an HTTP 500 in the intake agent. When
# picking a replacement, note that some newer endpoints (sonnet-5,
# opus-4-8) REJECT the `temperature` parameter this client sends; sonnet-4-6
# is the newest Sonnet that still accepts it.
DEFAULT_LLM_ENDPOINT = "databricks-claude-sonnet-4-6"


def resolve_endpoint() -> str:
    """Resolve the serving-endpoint name at call time (so a deploy-time env
    change takes effect without code edits). Default is Claude."""
    return (
        os.environ.get("OPS_LLM_ENDPOINT")
        or os.environ.get("DATABRICKS_LLM_ENDPOINT")
        or DEFAULT_LLM_ENDPOINT
    ).strip()


# Back-compat module-level constant (resolved at import time).
MODEL_ID = resolve_endpoint()


# Metadata for a WIDE table is the largest reply we ask for: one comment per
# silver column. The four-feed spike has 78 columns, whose JSON does not fit in
# 2048 tokens — the reply came back cut off mid-string, `json.loads` threw, and
# the caller's `except` turned that into "the LLM returned nothing". Nothing
# errored; the pipeline just shipped `_COLUMN_COMMENTS = {}` and a table comment
# that was a verbatim copy of the spec. See KNOWN_ISSUES 1c.
#
# 16384 fits ~100 columns of prose with headroom. Raising it is necessary but
# NOT sufficient — a ceiling can always be hit by a wider table, so the truncated
# reply must also be DETECTABLE rather than silently empty, which is what
# `truncated` below is for.
DEFAULT_MAX_TOKENS = 16384


@dataclass
class _LLMResponse:
    content: str
    # True when the endpoint stopped because it ran out of output budget rather
    # than because the model finished. A caller parsing JSON MUST treat this as
    # a hard failure: the text is a prefix of valid JSON, so every parse of it
    # fails in the same indistinguishable way as "the model said nothing".
    truncated: bool = False


def _role_for(msg: Any) -> str:
    """Map a LangChain-style message to its serving-endpoints role string."""
    cls = type(msg).__name__
    if cls in ("SystemMessage",):
        return "system"
    if cls in ("HumanMessage", "UserMessage"):
        return "user"
    if cls in ("AIMessage", "AssistantMessage"):
        return "assistant"
    # Fall back to .type attr if present (LangChain core sets this)
    return getattr(msg, "type", "user").lower()


class _DatabricksServingChat:
    """Minimal ChatDatabricks replacement. Exposes .invoke(messages) so the
    existing agent/nodes.py + agent/intake.py code is unchanged."""

    def __init__(self, endpoint: str, temperature: float = 0.0,
                 max_tokens: int = DEFAULT_MAX_TOKENS):
        self.endpoint = endpoint
        self.temperature = temperature
        self.max_tokens = max_tokens
        # Lazy: build SDK client on first call so this module imports cleanly
        # even when DATABRICKS_HOST isn't set (e.g. unit tests with mocks)
        self._client = None

    def _get_client(self):
        if self._client is None:
            from databricks.sdk import WorkspaceClient
            self._client = WorkspaceClient()
        return self._client

    def _is_governed_service(self) -> bool:
        """A 3-part UC name (catalog.schema.name) is a Unity AI Gateway *model
        service*, NOT a serving endpoint. Model services are addressable ONLY on
        the gateway path (`/ai-gateway/...`) — a POST to
        `/serving-endpoints/{3-part}/invocations` 404s (verified on the sandbox).
        Detect the 3-part shape so `invoke()` can route + shape the request for
        the governed path instead. This is what makes every agent LLM call in
        Pipeline Studio flow through Unity AI Gateway (attribution, inference-table
        payload logging, policy + budget enforcement)."""
        name = self.endpoint
        parts = name.split(".")
        return len(parts) == 3 and all(p and " " not in p and "/" not in p for p in parts)

    def invoke(self, messages: list[Any]) -> _LLMResponse:
        governed = self._is_governed_service()
        body = {
            "messages": [
                {"role": _role_for(m),
                 "content": m.content if isinstance(m.content, str) else str(m.content)}
                for m in messages
            ],
            "max_tokens": self.max_tokens,
        }
        # The governed gateway destination (Claude opus/sonnet-5 class) REJECTS the
        # `temperature` parameter with HTTP 400 (verified live). Everything the
        # agents do runs at temperature 0 anyway, so omit it on the governed path.
        # Non-governed serving endpoints keep the explicit temperature.
        if not governed:
            body["temperature"] = self.temperature
        client = self._get_client()
        # Use the SDK's authenticated transport — it handles SP/OAuth + retries
        host = client.config.host.rstrip("/")
        auth_headers = client.config.authenticate()
        import urllib.request
        if governed:
            # Route through Unity AI Gateway. The OpenAI-compatible gateway path
            # accepts the same chat body and returns the same OpenAI-shaped
            # response `_extract()` already parses. `model` is the 3-part service
            # name; the gateway resolves it to the destination model.
            body["model"] = self.endpoint
            url = f"{host}/ai-gateway/openai/v1/chat/completions"
        else:
            url = f"{host}/serving-endpoints/{self.endpoint}/invocations"
        req = urllib.request.Request(
            url,
            data=json.dumps(body).encode("utf-8"),
            method="POST",
        )
        for k, v in auth_headers.items():
            req.add_header(k, v)
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            LOG.warning("serving endpoint call failed: %s", e)
            raise

        return self._extract(payload)

    def _extract(self, payload: dict) -> _LLMResponse:
        """Pull the answer text out of a serving-endpoint response, and record
        whether the endpoint stopped because it ran out of output budget.

        Separated from the HTTP call so the truncation detection is testable
        without a live endpoint — the bug it guards against was invisible
        precisely because nobody could exercise this path offline."""
        # Endpoints typically return OpenAI-compatible:
        # {"choices": [{"message": {"content": "..."}}]}
        # Some return {"choices": [{"text": "..."}]} for completions
        text = ""
        truncated = False
        try:
            choices = payload.get("choices") or []
            if choices:
                first = choices[0]
                # "length" (OpenAI-compatible) / "max_tokens" (some endpoints)
                # means the reply was cut off at the output ceiling. Capture it
                # here, at the only layer that can still tell the difference
                # between a short answer and a severed one.
                if str(first.get("finish_reason") or "").lower() in ("length", "max_tokens"):
                    truncated = True
                    LOG.warning(
                        "serving endpoint %s stopped at the %d-token output ceiling — "
                        "the reply is CUT OFF, not complete", self.endpoint, self.max_tokens)
                msg = first.get("message") or {}
                content = msg.get("content")
                if isinstance(content, list):
                    # Reasoning endpoints (e.g. databricks-qwen35-122b-a10b,
                    # gpt-oss) return a list of content blocks — keep only the
                    # answer ('text'/'output_text'), DROP the 'reasoning' blocks,
                    # so downstream JSON/SQL parsing sees clean output.
                    parts = []
                    for b in content:
                        if isinstance(b, str):
                            parts.append(b)
                        elif isinstance(b, dict) and b.get("type") in (None, "text", "output_text"):
                            parts.append(b.get("text") or b.get("content") or "")
                    text = "".join(parts).strip()
                else:
                    text = content or first.get("text") or ""
        except Exception:
            pass
        if not text:
            # Fallback for non-standard shapes
            text = json.dumps(payload)
        return _LLMResponse(content=text, truncated=truncated)


@lru_cache(maxsize=8)
def _get_llm_cached(endpoint: str) -> _DatabricksServingChat:
    return _DatabricksServingChat(endpoint=endpoint, temperature=0,
                                  max_tokens=DEFAULT_MAX_TOKENS)


def get_llm() -> _DatabricksServingChat:
    """Return the chat client for the currently-configured endpoint
    (resolved fresh each call; cached per endpoint name)."""
    return _get_llm_cached(resolve_endpoint())
