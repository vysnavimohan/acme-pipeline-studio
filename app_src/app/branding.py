"""Deploy-time UI branding, so one deployment can be skinned without a fork.

The repo ships **Databricks** branding and that is the default. A deployment can
override the product name, the accent colours and the logo purely with environment
variables — nothing here names any particular customer, and no override values are
committed.

Why this exists rather than a branch: a themed branch has to be rebased forever,
and the one thing worse than an unthemed demo is a themed branch that silently
becomes the source of truth and ships a customer's colours to everyone. Overriding
at deploy time keeps `main` unambiguously the generic asset.

To skin a deployment, set any of these in the App's environment:

    OPS_BRAND_NAME        product name in the nav, page titles and hero
    OPS_BRAND_SHORT       short form used where space is tight
    OPS_BRAND_LOGO        path under /static (e.g. "/static/acme_logo.svg")
    OPS_BRAND_ACCENT      primary accent / CTA colour   (default Databricks Lava)
    OPS_BRAND_ACCENT_2    the same, darkened for hover
    OPS_BRAND_DEEP        brand / hero colour           (default Databricks Navy)
    OPS_BRAND_LINK        link colour
    OPS_BRAND_TINT        pale chip background
    OPS_BRAND_INK         body text colour
    OPS_BRAND_FONT        CSS font-family stack
    OPS_BRAND_FONT_URL    a stylesheet to load for that font

Colours are validated: a malformed value falls back to the Databricks default
rather than emitting broken CSS, because a half-applied palette looks like a
rendering bug and sends the next person hunting in the wrong place.
"""
from __future__ import annotations

import os
import re

# The shipped defaults ARE the Databricks brand. Keep in step with the `:root`
# block in templates/_base.html, which consumes these.
_DEFAULTS = {
    "name": "Databricks Pipeline Studio",
    "short": "Pipeline Studio",
    "logo": "/static/databricks_logo.svg",
    "logo_alt": "Databricks",
    "accent": "#FF3621",      # Databricks Lava
    "accent_2": "#D92B18",
    "deep": "#1B3139",        # Databricks Navy 800
    "link": "#2272B4",        # Databricks Blue
    "tint": "#F0F5F7",
    "ink": "#1B3139",
    "font": "",               # empty = keep the stylesheet's own stack
    "font_url": "",
}

_HEX = re.compile(r"^#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{6})$")


def _colour(env: str, default: str) -> str:
    val = (os.environ.get(env) or "").strip()
    return val if _HEX.match(val) else default


def _text(env: str, default: str, limit: int = 80) -> str:
    val = (os.environ.get(env) or "").strip()
    return val[:limit] if val else default


def _asset(env: str, default: str) -> str:
    """A logo path must stay under /static — an absolute URL would let a themed
    deployment fetch a third-party asset at page load."""
    val = (os.environ.get(env) or "").strip()
    if val.startswith("/static/") and "://" not in val and ".." not in val:
        return val
    return default


def _rgb_triple(hex_colour: str) -> str:
    """`#1188EE` → `17,136,238`, for use inside `rgba(var(--x), 0.15)`.

    CSS cannot slice a hex string, so a few focus-ring and pulse-glow rules had the
    accent hardcoded as an rgb triple inside `rgba()`. Those stayed Databricks red on a themed
    deployment — invisible to a scan for the hex form, and obvious to anyone who
    focused an input.
    """
    h = hex_colour.lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    try:
        return ",".join(str(int(h[i:i + 2], 16)) for i in (0, 2, 4))
    except ValueError:
        return "255,54,33"


def brand() -> dict[str, str]:
    """The active branding. Read per request: cheap, and it means an env change
    takes effect on restart without a code edit."""
    name = _text("OPS_BRAND_NAME", _DEFAULTS["name"])
    accent = _colour("OPS_BRAND_ACCENT", _DEFAULTS["accent"])
    return {
        "name": name,
        "accent_rgb": _rgb_triple(accent),
        "short": _text("OPS_BRAND_SHORT", _DEFAULTS["short"]),
        "logo": _asset("OPS_BRAND_LOGO", _DEFAULTS["logo"]),
        "logo_alt": _text("OPS_BRAND_LOGO_ALT", name),
        "accent": accent,
        "accent_2": _colour("OPS_BRAND_ACCENT_2", _DEFAULTS["accent_2"]),
        "deep": _colour("OPS_BRAND_DEEP", _DEFAULTS["deep"]),
        "link": _colour("OPS_BRAND_LINK", _DEFAULTS["link"]),
        "tint": _colour("OPS_BRAND_TINT", _DEFAULTS["tint"]),
        "ink": _colour("OPS_BRAND_INK", _DEFAULTS["ink"]),
        "font": _text("OPS_BRAND_FONT", _DEFAULTS["font"], limit=200),
        "font_url": _text("OPS_BRAND_FONT_URL", _DEFAULTS["font_url"], limit=300),
        # True when ANY override is present. Surfaced so a reader of a themed
        # deployment can tell it is skinned rather than assuming the repo changed.
        "themed": any((os.environ.get(k) or "").strip() for k in (
            "OPS_BRAND_NAME", "OPS_BRAND_LOGO", "OPS_BRAND_ACCENT",
            "OPS_BRAND_DEEP", "OPS_BRAND_FONT")),
    }


def is_themed() -> bool:
    return bool(brand()["themed"])
