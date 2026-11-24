"""Get a psycopg connection string for a Lakebase instance, using the
Databricks SDK to mint an OAuth token credential.

In jobs/apps the SDK auths automatically via the SP. Locally, supply
DATABRICKS_HOST + DATABRICKS_TOKEN, or use a profile.
"""

from __future__ import annotations

import os
import urllib.parse

from databricks.sdk import WorkspaceClient


def get_lakebase_conn_str(instance_name: str, database_name: str = "databricks_postgres") -> str:
    """Return a psycopg-compatible connection URL.

    Generates an OAuth credential, then constructs a `postgresql://...` URL.
    Both the username (an @-containing email) and the token are URL-encoded.
    """
    client = WorkspaceClient()
    # SDK renamed the Lakebase API surface (w.database → w.postgres in newer
    # versions). Probe whichever this SDK ships.
    db_api = getattr(client, "database", None) or getattr(client, "postgres", None)
    if db_api is None:
        raise RuntimeError("this databricks-sdk version exposes neither "
                           "WorkspaceClient.database nor .postgres")
    inst = db_api.get_database_instance(name=instance_name)
    cred = db_api.generate_database_credential(
        instance_names=[instance_name],
        request_id=os.environ.get("DATABRICKS_JOB_RUN_ID", "ops-cli"),
    )
    user = urllib.parse.quote(client.current_user.me().user_name, safe="")
    token = urllib.parse.quote(cred.token, safe="")
    host = inst.read_write_dns
    return f"postgresql://{user}:{token}@{host}:5432/{database_name}?sslmode=require"
