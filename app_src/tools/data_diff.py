"""Lightweight schema + row-count diff between dev and prod silver tables.

For the PoC we report what would change. A real version would call into
Recce or Datafold via their MCP servers.
"""

from __future__ import annotations

from typing import Any

from databricks import sql as dbsql


def diff_silver(host: str, http_path: str, token: str, dev_table: str, prod_table: str) -> dict[str, Any]:
    result = {"dev_table": dev_table, "prod_table": prod_table}
    with dbsql.connect(
        server_hostname=host.replace("https://", "").rstrip("/"),
        http_path=http_path,
        access_token=token,
    ) as conn:
        with conn.cursor() as cur:
            def safe_count(t: str) -> int:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {t}")
                    return cur.fetchone()[0]
                except Exception:
                    return -1

            def schema(t: str) -> list[tuple[str, str]]:
                try:
                    cur.execute(f"DESCRIBE {t}")
                    return [(r[0], r[1]) for r in cur.fetchall() if r[0] and not r[0].startswith("#")]
                except Exception:
                    return []

            dev_count = safe_count(dev_table)
            prod_count = safe_count(prod_table)
            dev_schema = schema(dev_table)
            prod_schema = schema(prod_table)

            result["dev_row_count"] = dev_count
            result["prod_row_count"] = prod_count
            result["row_delta_pct"] = (
                (dev_count - prod_count) / prod_count * 100
                if prod_count and prod_count > 0
                else None
            )

            dev_cols = {c: t for c, t in dev_schema}
            prod_cols = {c: t for c, t in prod_schema}
            result["added_columns"] = sorted(set(dev_cols) - set(prod_cols))
            result["removed_columns"] = sorted(set(prod_cols) - set(dev_cols))
            result["type_changes"] = sorted(
                f"{c}: {prod_cols[c]} -> {dev_cols[c]}"
                for c in (set(dev_cols) & set(prod_cols))
                if dev_cols[c] != prod_cols[c]
            )
    return result
