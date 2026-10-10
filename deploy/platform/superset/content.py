"""Import the starter BI content into Superset (REQ-BI-05); run inside the Superset pod.

``make superset-content`` copies ``content/`` (a Superset export bundle) to
/tmp/idp-content in the web server pod and runs this file with Superset's Python. It:
- imports the databases, datasets, charts and dashboards, owned by a technical user ``idp-content`` that can't sign in (sign-in is
  Keycloak only, REQ-BI-01). Datasets, charts and dashboards overwrite earlier
  versions with the same uuid; the dashboard importer alone would overwrite only the
  dashboards and keep existing charts and datasets as they were;
- creates the role ``IdpData``: database access to the two connections, given to
  tenant users at sign-in (``AUTH_ROLES_MAPPING``).
The bundle holds no credentials: the connection mutator supplies them per user.
"""
import re
import secrets
from pathlib import Path

from superset.app import create_app

ROOT = Path("/tmp/idp-content")
DATABASES = ("lake (Trino)", "serving (TimescaleDB)")
OWNER = "idp-content"

app = create_app()
with app.app_context():
    from flask import g

    from superset import security_manager as sm
    from superset.commands.chart.importers.v1 import ImportChartsCommand
    from superset.commands.dashboard.importers.dispatcher import ImportDashboardsCommand
    from superset.commands.dataset.importers.v1 import ImportDatasetsCommand
    from superset.extensions import db
    from superset.models.core import Database

    owner = sm.find_user(username=OWNER) or sm.add_user(
        OWNER, "IDP", "Content", f"{OWNER}@idp.invalid", [sm.find_role("Admin")],
        password=secrets.token_urlsafe(32))
    g.user = owner
    contents = {str(p.relative_to(ROOT)): p.read_text() for p in sorted(ROOT.rglob("*.yaml"))
                if not p.name.startswith(".")}            # e.g. macOS tar's ._* metadata files
    # each command checks the bundle's metadata type: datasets, then charts, then dashboards
    for command, kind in ((ImportDatasetsCommand, "SqlaTable"), (ImportChartsCommand, "Slice")):
        meta = re.sub(r"(?m)^type: .*$", f"type: {kind}", contents["metadata.yaml"])
        command({**contents, "metadata.yaml": meta}, overwrite=True).run()
    ImportDashboardsCommand(contents, overwrite=True).run()

    role = sm.find_role("IdpData") or sm.add_role("IdpData")
    for database in db.session.query(Database).filter(Database.database_name.in_(DATABASES)):
        sm.add_permission_role(role, sm.add_permission_view_menu("database_access", database.perm))
    db.session.commit()
    print(f"imported {len(contents)} files; IdpData grants: "
          f"{sorted(pv.view_menu.name for pv in role.permissions)}")
