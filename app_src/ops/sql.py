"""Run SQL on the demo workspace (profile = $DATABRICKS_CONFIG_PROFILE or -p): `python ops/sql.py "SELECT 1"` or `python ops/sql.py -f file.sql`.

Files are split on lines that contain only `;;` so statements may hold semicolons (YAML).
"""
import argparse
import os
import sys

from databricks.sdk import WorkspaceClient

WAREHOUSE = os.environ.get("DATABRICKS_WAREHOUSE_ID", "7464666eb7d50c27")


def run(w, stmt):
    r = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE, statement=stmt, wait_timeout="50s")
    while r.status.state.value in ("PENDING", "RUNNING"):
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state.value != "SUCCEEDED":
        print("FAILED:", r.status.error.message if r.status.error else r.status.state, file=sys.stderr)
        return False
    if r.result and r.result.data_array is not None and r.manifest and r.manifest.schema:
        cols = [c.name for c in r.manifest.schema.columns]
        print("\t".join(cols))
        for row in r.result.data_array:
            print("\t".join("" if v is None else str(v) for v in row))
    else:
        print("OK")
    return True


def main():
    p = argparse.ArgumentParser()
    p.add_argument("sql", nargs="?")
    p.add_argument("-f", "--file")
    p.add_argument("-p", "--profile", default=os.environ.get("DATABRICKS_CONFIG_PROFILE"))
    a = p.parse_args()
    w = WorkspaceClient(profile=a.profile)
    text = open(a.file).read() if a.file else a.sql
    stmts = [s.strip() for s in text.split("\n;;\n")] if a.file else [text]
    ok = all(run(w, s) for s in stmts if s)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
