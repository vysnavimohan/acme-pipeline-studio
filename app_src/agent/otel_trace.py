"""@traced_node decorator: wraps a LangGraph node function in an OTel span.

The span gets ops.node, ops.request_id attributes. Small primitive return
values are captured as ops.out.<key> attributes. Exceptions set status=ERROR
and are re-raised.

If OTel isn't available (e.g. local dev without deps), it's a no-op wrapper.
"""

from __future__ import annotations

from functools import wraps
from typing import Any, Callable

try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode
    _tracer = trace.get_tracer("ops.nodes")
    _OTEL_AVAILABLE = True
except Exception:
    _tracer = None
    _OTEL_AVAILABLE = False


def traced_node(name: str) -> Callable:
    def deco(fn: Callable) -> Callable:
        if not _OTEL_AVAILABLE:
            return fn

        @wraps(fn)
        def wrapper(state, *args, **kwargs):
            with _tracer.start_as_current_span(f"node.{name}") as span:
                span.set_attribute("ops.node", name)
                rid = getattr(state, "request_id", None)
                if rid:
                    span.set_attribute("ops.request_id", str(rid))
                spec = getattr(state, "spec", None)
                if spec is not None:
                    target = getattr(spec, "target", None)
                    if target is not None:
                        span.set_attribute("ops.target_table", str(getattr(target, "table", "")))
                try:
                    out = fn(state, *args, **kwargs)
                    if isinstance(out, dict):
                        for k, v in list(out.items())[:8]:
                            if isinstance(v, (str, int, float, bool)) and len(str(v)) < 200:
                                span.set_attribute(f"ops.out.{k}", v)
                    return out
                except Exception as e:
                    span.set_status(Status(StatusCode.ERROR, str(e)[:200]))
                    span.record_exception(e)
                    raise

        return wrapper

    return deco
