"""End-to-end BI isolation check on the cluster (REQ-BI-*, REQ-TEN-06); run inside the
Superset pod (`make superset-smoke`), where `localhost:8088` is a registered Keycloak
redirect and Keycloak's issuer host resolves.

Signs in through the real Keycloak login form as a user of each tenant, then for every
chart of the starter dashboards: runs its stored query, and the same query grouped by
`tenant_id`, and checks only the signed-in user's tenant comes back (SCN-TEN-06.3,
SCN-BI-02.1, SCN-BI-03.1); opens the charts back to back as both users (SCN-BI-04.1);
and runs SQL Lab statements that try to widen the scope (SCN-BI-02.2, SCN-BI-03.2).
The SQL Lab statements land in each test user's query history; they carry a comment
saying they are this isolation check and should return nothing. Exits 1 on any failure.
"""
import copy
import html
import json
import os
import re
import sys

import requests

S = "http://localhost:8088"
USERS = {"alice": "tenant_a", "bob": "tenant_b"}          # dev users of the realm
PASSWORD = os.environ.get("IDP_SMOKE_PASSWORD", "dev-password")
DASHBOARDS = ("subscriber-overview", "usage-overview", "credit-scores", "fraud-scores")
failures = []


def check(ok, what):
    print(("ok   " if ok else "FAIL ") + what)
    if not ok:
        failures.append(what)


def login(user):
    s = requests.Session()
    r = s.get(f"{S}/login/keycloak?next=/", timeout=30)
    s.post(html.unescape(re.search(r'<form[^>]*action="([^"]+)"', r.text).group(1)),
           data={"username": user, "password": PASSWORD}, timeout=30)
    s.headers.update({"X-CSRFToken": s.get(f"{S}/api/v1/security/csrf_token/").json()["result"], "Referer": S})
    return s


def charts_of(s, slug):
    return [c["id"] for c in s.get(f"{S}/api/v1/dashboard/{slug}/charts").json()["result"]]


def chart_rows(s, chart_id, by_tenant=False):
    qc = json.loads(s.get(f"{S}/api/v1/chart/{chart_id}").json()["result"]["query_context"])
    if by_tenant:
        qc = copy.deepcopy(qc)
        for q in qc["queries"]:
            if "tenant_id" not in q.get("columns", []):
                q["columns"] = ["tenant_id", *q.get("columns", [])]
            q["metrics"] = q.get("metrics") or [{"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "n"}]
            q.pop("orderby", None)
            q.pop("post_processing", None)       # e.g. histogram bins drop the tenant column
    r = s.post(f"{S}/api/v1/chart/data", json=qc, timeout=120)
    assert r.status_code == 200, (chart_id, r.status_code, r.text[:300])
    return [row for res in r.json()["result"] for row in res["data"]]


LABEL = ("-- make superset-smoke: isolation check, run as this user on purpose. It asks for\n"
         "-- ANOTHER tenant's data; the expected result is 0 rows or a refusal.\n")


def sql(s, db, q):
    """Run ``q`` in SQL Lab, labelled so the user's query history explains it."""
    r = s.post(f"{S}/api/v1/sqllab/execute/", json={"database_id": db, "sql": LABEL + q, "runAsync": False},
               timeout=120)
    return r.json().get("data") if r.status_code == 200 else f"refused ({r.status_code})"


sessions = {u: login(u) for u in USERS}
for user, tenant in USERS.items():
    s = sessions[user]
    me = s.get(f"{S}/api/v1/me/roles/").json()["result"]
    check(me["username"] == f"{user}@{tenant}" and {"Gamma", "sql_lab", "IdpData"} <= set(me["roles"]),
          f"{user}: signed in as {me['username']} with roles {sorted(me['roles'])}")
    dbs = {d["database_name"]: d["id"] for d in s.get(f"{S}/api/v1/database/").json()["result"]}
    for slug in DASHBOARDS:
        ids = charts_of(s, slug)
        check(bool(ids), f"{user}: dashboard {slug} has {len(ids)} charts")
        for cid in ids:
            stored = chart_rows(s, cid)
            tenants = {str(r["tenant_id"]) for r in chart_rows(s, cid, by_tenant=True)}
            check(tenants <= {tenant} and (tenants or not stored),
                  f"{user}: chart {cid} returns tenants {sorted(tenants)} ({len(stored)} rows as stored)")
    other = next(t for t in USERS.values() if t != tenant)
    check(sql(s, dbs["lake (Trino)"], f"SELECT count(*) AS n FROM silver.fact_usage WHERE tenant_id = '{other}'") == [{"n": 0}],
          f"{user}: Silver rows of {other} through SQL Lab: none")
    check(sql(s, dbs["serving (TimescaleDB)"], f"SELECT count(*) AS n FROM gold.fraud_scores WHERE tenant_id = '{other}'") == [{"n": 0}],
          f"{user}: Gold rows of {other} through SQL Lab: none")
    check(isinstance(sql(s, dbs["serving (TimescaleDB)"], f"SET ROLE idp_bi_{other}"), str),
          f"{user}: SET ROLE idp_bi_{other} refused")

# SCN-BI-04.1: the same chart, back to back, for both users
first = charts_of(sessions["alice"], DASHBOARDS[0])[0]
seen = [{str(r["tenant_id"]) for r in chart_rows(sessions[u], first, by_tenant=True)} for u in ("alice", "bob")]
check(seen == [{"tenant_a"}, {"tenant_b"}], f"chart {first} back to back: alice {sorted(seen[0])}, bob {sorted(seen[1])}")

print(f">> {len(failures)} failure(s)")
sys.exit(1 if failures else 0)
