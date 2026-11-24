"""Minimal Confluence Cloud REST client (read-only).

Used by the Agent Config integration: an agent's guidelines can live on a
Confluence page owned by the data engineering team. At LLM-call time the
agent pulls the page content (cached, fallback to the stored prompt);
the Agent Config UI shows a rendered preview + deep link.

Auth: basic auth with an Atlassian account email + API token.
  CONFLUENCE_BASE_URL   e.g. https://databricks.atlassian.net/wiki
  CONFLUENCE_EMAIL      Atlassian account email
  CONFLUENCE_API_TOKEN  API token from id.atlassian.com

All functions raise ConfluenceError on failure — callers decide whether
to degrade gracefully.
"""

from __future__ import annotations

import base64
import json
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from html.parser import HTMLParser
from typing import Any, Optional


class ConfluenceError(RuntimeError):
    pass


_DEFAULT_BASE_URL = "https://databricks.atlassian.net/wiki"
_USER_AGENT = "DatabricksPipelineStudio/1.0 (Databricks App; +https://databricks.com)"
_creds_loaded = False
_creds_lock = threading.Lock()


def _load_creds_from_secrets() -> None:
    """If env vars are absent, pull credentials from the workspace secret
    scope (the App SP / Job identity has READ on the scope). One attempt
    per process."""
    global _creds_loaded
    with _creds_lock:
        if _creds_loaded:
            return
        _creds_loaded = True
        if os.environ.get("CONFLUENCE_EMAIL") and os.environ.get("CONFLUENCE_API_TOKEN"):
            return
        scope = os.environ.get("CONFLUENCE_SECRET_SCOPE", "databricks-pipeline-studio")
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()

            def _get(key: str) -> Optional[str]:
                try:
                    resp = w.secrets.get_secret(scope=scope, key=key)
                    return base64.b64decode(resp.value).decode("utf-8")
                except Exception:
                    return None

            email = _get("confluence-email")
            token = _get("confluence-api-token")
            if email and token:
                os.environ.setdefault("CONFLUENCE_EMAIL", email)
                os.environ.setdefault("CONFLUENCE_API_TOKEN", token)
        except Exception:
            pass


def configured() -> bool:
    _load_creds_from_secrets()
    return bool(os.environ.get("CONFLUENCE_EMAIL")
                and os.environ.get("CONFLUENCE_API_TOKEN"))


def _base() -> str:
    return (os.environ.get("CONFLUENCE_BASE_URL") or _DEFAULT_BASE_URL).rstrip("/")


def _request(path: str, params: Optional[dict] = None) -> dict:
    _load_creds_from_secrets()
    email = os.environ.get("CONFLUENCE_EMAIL")
    token = os.environ.get("CONFLUENCE_API_TOKEN")
    if not email or not token:
        raise ConfluenceError("CONFLUENCE_EMAIL / CONFLUENCE_API_TOKEN not set")
    url = _base() + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url)
    cred = base64.b64encode(f"{email}:{token}".encode()).decode()
    req.add_header("Authorization", f"Basic {cred}")
    req.add_header("Accept", "application/json")
    # Identify ourselves rather than sending urllib's default
    # `Python-urllib/3.x`. Not a fix for anything — the 403s we saw from inside
    # the App were the Atlassian IP allowlist, and a local A/B showed both UAs
    # get 200 — but a named UA is what Atlassian's own rate-limit guidance asks
    # for and makes our traffic identifiable in their logs.
    req.add_header("User-Agent", _USER_AGENT)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        # Include Atlassian's own response body: it distinguishes an expired
        # token from a permission problem from a blocked client, and without it
        # every one of those is an indistinguishable bare "403 Forbidden".
        try:
            detail = e.read().decode("utf-8", "replace")[:300]
        except Exception:  # noqa: BLE001
            detail = ""
        raise ConfluenceError(
            f"Confluence API call failed ({path}): {e}"
            + (f" — {detail}" if detail else "")) from e
    except Exception as e:
        raise ConfluenceError(f"Confluence API call failed ({path}): {e}") from e


class _TextExtractor(HTMLParser):
    """Storage/view HTML → readable plain text for LLM prompts."""

    _BLOCK_TAGS = {"p", "div", "li", "tr", "br", "h1", "h2", "h3", "h4", "h5",
                   "h6", "ul", "ol", "table", "blockquote", "pre"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._in_li = False

    def handle_starttag(self, tag, attrs):
        if tag == "li":
            self.parts.append("\n- ")
        elif tag in self._BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data):
        self.parts.append(data)

    def text(self) -> str:
        raw = "".join(self.parts)
        raw = re.sub(r"[ \t]+", " ", raw)
        raw = re.sub(r"\n{3,}", "\n\n", raw)
        return raw.strip()


def html_to_text(html: str) -> str:
    p = _TextExtractor()
    p.feed(html or "")
    return p.text()


def extract_page_id(page_ref: str) -> str:
    """Accept a bare page id or any Confluence page URL and return the id."""
    ref = (page_ref or "").strip()
    if ref.isdigit():
        return ref
    m = re.search(r"/pages/(\d+)", ref)
    if m:
        return m.group(1)
    # viewpage.action?pageId=123
    m = re.search(r"pageId=(\d+)", ref)
    if m:
        return m.group(1)
    raise ConfluenceError(f"could not extract a page id from {page_ref!r}")


def whoami() -> dict[str, Any]:
    """The authenticated Atlassian user. The cheapest call that still proves
    both the credential AND that this host is allowed to reach the site — used
    by the Agent Config UI to tell "not configured" apart from "configured but
    blocked by the Atlassian IP allowlist"."""
    return _request("/rest/api/user/current")


def get_page(page_id: str) -> dict[str, Any]:
    """Fetch a page: title, rendered HTML (view), plain text, web URL, version."""
    data = _request(f"/rest/api/content/{page_id}",
                    {"expand": "body.view,version,space"})
    html = (((data.get("body") or {}).get("view") or {}).get("value")) or ""
    webui = ((data.get("_links") or {}).get("webui")) or ""
    return {
        "page_id": str(data.get("id", page_id)),
        "title": data.get("title", ""),
        "html": html,
        "text": html_to_text(html),
        "url": _base() + webui,
        "version": ((data.get("version") or {}).get("number")),
        "updated_at": ((data.get("version") or {}).get("when")),
        "updated_by": (((data.get("version") or {}).get("by") or {}).get("displayName")),
    }


# --- TTL cache (per-process) -------------------------------------------
# get(agent) runs on every LLM call inside an agent run — don't hammer
# Confluence. 60s is fresh enough ("refresh the page at most").

_CACHE_TTL_SECONDS = int(os.environ.get("CONFLUENCE_CACHE_TTL", "60"))
_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_cache_lock = threading.Lock()


def get_page_cached(page_id: str, force_refresh: bool = False) -> dict[str, Any]:
    now = time.time()
    with _cache_lock:
        hit = _cache.get(page_id)
        if hit and not force_refresh and now - hit[0] < _CACHE_TTL_SECONDS:
            return hit[1]
    page = get_page(page_id)
    with _cache_lock:
        _cache[page_id] = (now, page)
    return page


def clear_cache() -> None:
    with _cache_lock:
        _cache.clear()
