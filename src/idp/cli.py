"""Command-line entrypoint for the platform.

Usage (separated medallion stages):
    idp produce    # synthetic data -> Kafka topics (--from-landing: the TMF landing files)
    idp consume    # Kafka (TMF events) -> raw Bronze (S3)  [Bronze stage]
    idp silver     # new Bronze -> Silver (incremental; --full-refresh replays all)  [Silver stage]
    idp gold       # Silver (S3) -> Gold features  [Gold stage: features ONLY]
    idp score      # Gold features + models -> scores (S3)   [inference]
    idp publish    # Gold (S3) -> TimescaleDB      [serving stage]
    idp labels     # seed the labels/outcomes store (training input)
    idp train      # Gold features + labels -> saved models  [offline ML]
    idp run        # local dev: compose all stages end-to-end (generate->...->gold)
    idp generate   # write synthetic data to the landing zone (TM Forum resource files)
    idp serve      # start the FastAPI scoring service
    idp archive-policy  # render the Bronze archive (lifecycle) rule as JSON
    idp registry-schemas  # write each Kafka topic's TMF JSON Schema (wire contract)
    idp keycloak-realm    # render the Keycloak realm from the configured tenants (--check: compare)
    idp bi-roles          # BI tenant roles + row-level security on the gold serving tables
    idp maintain   # compact, expire snapshots, remove orphans on every lake table
    idp mcp        # start the MCP analytics server (stdio): 7 analytics tools (5 Silver, 2 Gold scores)
"""
from __future__ import annotations

import argparse
import json

from idp.common.config import load_config


def _cmd_generate(args: argparse.Namespace) -> None:
    from idp.common import storage
    from idp.ingestion.landing import write_landing
    from idp.ingestion.tmf.profile import load_profile
    from idp.synthetic.generator import generate

    cfg = load_config(args.config)
    syn = cfg.synthetic
    landing = storage.landing_uri(cfg)
    profile = load_profile(cfg.tmf["profile"])
    # One tenant at a time, like `idp produce`, to cap peak memory.
    total = 0
    for tenant_id in cfg.tenant_ids:
        dataset = generate(
            tenant_id,
            subscribers=int(syn.get("subscribers_per_tenant", 200)),
            days=int(syn.get("days", 60)),
            fraud_rate=float(syn.get("fraud_rate", 0.05)),
            default_rate=float(syn.get("default_rate", 0.12)),
            seed=int(syn.get("seed", 42)),
        )
        written = write_landing(dataset, landing, storage_options=storage.storage_options(cfg),
                                profile=profile)
        total += sum(len(v) for v in written.values())
        del dataset
    print(f"Wrote {total} TM Forum landing files ({profile.name} profile) for "
          f"{len(cfg.tenant_ids)} tenant(s) to {landing}")
    print("Next: 'idp produce --from-landing' publishes them to Kafka.")


def _cmd_run(args: argparse.Namespace) -> None:
    from idp.pipeline import run

    cfg = load_config(args.config)
    summary = run(cfg, persist=not args.no_persist)
    printable = {k: v for k, v in summary.items() if k not in ("credit_scores", "fraud_scores", "silver")}
    print(json.dumps(printable, indent=2, default=str))


def _cmd_produce(args: argparse.Namespace) -> int | None:
    from idp.ingestion.tmf.profile import load_profile
    from idp.streaming.producer import produce_to_kafka
    from idp.synthetic.generator import generate

    cfg = load_config(args.config)
    syn = cfg.synthetic
    bootstrap = args.bootstrap or cfg.kafka["bootstrap"]
    if args.from_landing:
        return _produce_from_landing(cfg, bootstrap)
    # Generate + publish one tenant at a time to cap peak memory (avoids OOM on
    # small nodes; the whole multi-tenant dataset would be large in RAM).
    totals: dict[str, int] = {}
    for tenant_id in cfg.tenant_ids:
        dataset = generate(
            tenant_id,
            subscribers=int(syn.get("subscribers_per_tenant", 200)),
            days=int(syn.get("days", 60)),
            fraud_rate=float(syn.get("fraud_rate", 0.05)),
            default_rate=float(syn.get("default_rate", 0.12)),
            seed=int(syn.get("seed", 42)),
        )
        counts = produce_to_kafka(dataset, bootstrap, profile=load_profile(cfg.tmf["profile"]),
                                  topic_prefix=cfg.kafka["topic_prefix"])
        for k, v in counts.items():
            totals[k] = totals.get(k, 0) + v
        del dataset
        print(f"[{tenant_id}] published {sum(counts.values())} messages")
    print(f"Published to Kafka at {bootstrap}:")
    print(json.dumps(totals, indent=2))
    return None


def _produce_from_landing(cfg, bootstrap: str) -> int:
    """Publish the landing zone's TM Forum files as create events (REQ-STR-01), one
    tenant at a time.

    A rejected file (unknown tenant or file, invalid JSON or resource, tenant
    mismatch, a rejected file it depends on, an unresolved reference) is not
    published at all; the rest is. Returns 1 when anything was rejected or the
    landing zone holds no tenant folder, else 0.
    """
    from itertools import groupby

    from idp.common import storage
    from idp.ingestion.landing import read_landing_resources
    from idp.ingestion.tmf.profile import load_profile
    from idp.streaming.producer import publish_tenants

    landing = storage.landing_uri(cfg)
    profile = load_profile(cfg.tmf["profile"])
    files = read_landing_resources(landing, cfg.tenant_ids, profile=profile,
                                   storage_options=storage.storage_options(cfg))
    rejected: list[tuple[str, str]] = []
    totals: dict[str, int] = {}
    tenants_seen = 0
    for tenant, group in groupby(files, key=lambda f: f.tenant):
        accepted: dict[str, list[dict]] = {}
        for f in group:
            if f.reason:
                rejected.append((f.path, f.reason))
            else:
                accepted[f.file] = f.resources
        if not tenant:                                  # entries outside any tenant folder
            continue
        tenants_seen += 1
        counts = publish_tenants([(tenant, accepted)], bootstrap, profile=profile,
                                 topic_prefix=cfg.kafka["topic_prefix"])
        for k, v in counts.items():
            totals[k] = totals.get(k, 0) + v
        print(f"[{tenant}] published {sum(counts.values())} messages from the landing zone")
    print(f"Published from {landing} to Kafka at {bootstrap}:")
    print(json.dumps(totals, indent=2))
    if rejected:
        print(f"REJECTED {len(rejected)} landing file(s) (not published):")
        for path, reason in rejected:
            print(f"  {path}: {reason}")
    if not tenants_seen:
        print(f"No tenant folders in {landing}. Run 'idp generate' first.")
    return 1 if rejected or not tenants_seen else 0


def _cmd_consume(args: argparse.Namespace) -> int | None:
    """Bronze stage only: Kafka (TMF events) -> raw Bronze, as received. Silver/Gold
    are separate stages. Returns 1 when any message was dead-lettered."""
    from idp.pipeline import to_bronze
    from idp.streaming.consumer import consume_to_bronze

    cfg = load_config(args.config)
    bootstrap = args.bootstrap or cfg.kafka["bootstrap"]
    print(f"Draining topics from {bootstrap} (idle timeout {args.idle_timeout}s)...")
    landed: dict = {}
    result = consume_to_bronze(
        bootstrap, cfg.tenant_ids,
        topic_prefix=cfg.kafka["topic_prefix"], idle_timeout=args.idle_timeout,
        land=lambda frames: landed.update(to_bronze(cfg, frames)),   # commit offsets after
    )
    total = sum(len(df) for df in result.frames.values())
    print(f"Consumed {total} events: { {k: len(v) for k, v in result.frames.items()} }")
    if total:
        print(f"Appended to Iceberg Bronze (raw): {landed.get('bronze_tables')}")
    if result.dead_letters:
        print(f"DEAD-LETTERED {len(result.dead_letters)} message(s) to "
              f"{cfg.kafka['topic_prefix']}.<tenant>.dlq (not stored):")
        for d in result.dead_letters[:20]:
            print(f"  {d.topic}[{d.partition}]@{d.offset}: {d.reason}")
        if len(result.dead_letters) > 20:
            print(f"  ... and {len(result.dead_letters) - 20} more")
        return 1
    if total == 0:
        print("No messages consumed (topics empty or producer not finished). "
              "Run 'idp produce' first.")
        return None
    print("Next: 'idp silver' to conform Bronze and build Silver.")
    return None


def _cmd_silver(args: argparse.Namespace) -> None:
    """Silver stage: Bronze (S3) -> canonical Silver (S3)."""
    from idp.pipeline import to_silver

    cfg = load_config(args.config)
    print(json.dumps(to_silver(cfg, full_refresh=args.full_refresh), indent=2))
    print("Next: 'idp gold' to build Gold from Silver.")


def _cmd_archive_policy(args: argparse.Namespace) -> None:
    """Render the Bronze archive rule (S3 lifecycle or MinIO ILM JSON)."""
    from idp.maintenance import archive_policy

    policy = archive_policy(load_config(args.config), storage_class=args.storage_class, fmt=args.format)
    print(json.dumps(policy, indent=2))


def _cmd_registry_schemas(args: argparse.Namespace) -> None:
    """Write each topic's wire contract (REQ-STR-03): <suffix>.schema.json, the TMF
    create-event JSON Schema registered as every tenant's t.<tenant>.<suffix>-value."""
    from pathlib import Path

    from idp.ingestion.tmf.schemas import registry_schema
    from idp.streaming import TOPIC_RESOURCES

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for suffix, resources in TOPIC_RESOURCES.items():
        schema = registry_schema(list(resources), f"t.<tenant>.{suffix} value: TMF create event "
                                                  f"of {' or '.join(resources)}")
        (out / f"{suffix}.schema.json").write_text(json.dumps(schema, indent=1) + "\n")
    print(f"Wrote {len(TOPIC_RESOURCES)} topic schemas to {out}")


def _cmd_bi_roles(args: argparse.Namespace) -> int:
    """Provision the BI tenant roles and Gold row-level security (REQ-BI-03)."""
    from idp.serving import bi_roles, timescale

    cfg = load_config(args.config)
    ts = cfg.timescale
    tenants = [t.id for t in cfg.tenants]
    done = bi_roles.provision(timescale.get_engine(cfg), tenants, bi_roles.passwords_from_env(tenants),
                              database=ts["database"], schema=ts.get("schema") or "gold")
    print(json.dumps(done, indent=2))
    return 0


def _cmd_keycloak_realm(args: argparse.Namespace) -> int:
    """Render the Keycloak realm from the configured tenants (REQ-TEN-02). With
    ``--check``, exit 1 when the committed realm differs from the rendering."""
    from pathlib import Path

    from idp.identity import realm

    out = Path(args.out) if args.out else realm.REALM_FILE
    text = realm.dumps(realm.render([t.id for t in load_config(args.config).tenants]))
    if args.check:
        same = out.exists() and out.read_text() == text
        print(f"{out}: " + ("matches the configured tenants" if same else
                            "differs from the rendering; run `idp keycloak-realm`"))
        return 0 if same else 1
    out.write_text(text)
    print(f"Wrote {out}")
    return 0


def _cmd_spark_manifest(args: argparse.Namespace) -> None:
    """Render a Spark stage's SparkApplication (REQ-PLT-10) from its profile."""
    from idp.transforms.sparkapp import render

    print(render(load_config(args.config), args.stage, args.stage_args), end="")


def _cmd_maintain(args: argparse.Namespace) -> None:
    """Lake maintenance: compaction, snapshot expiry, orphan cleanup (REQ-ORC-06)."""
    from idp.maintenance import maintain

    print(json.dumps(maintain(load_config(args.config)), indent=2))


def _cmd_gold(args: argparse.Namespace) -> None:
    """Gold stage: Silver (S3) -> feature tables ONLY (S3). Scoring is `idp score`."""
    from idp.pipeline import to_gold

    cfg = load_config(args.config)
    result = to_gold(cfg)
    print(json.dumps({"gold_tables": result["gold_tables"], "counts": result["counts"]},
                     indent=2, default=str))
    print("Next: 'idp labels' (once per dataset; training needs it), then 'idp train', "
          "'idp score' and 'idp publish'. With trained champion models: 'idp score', then 'idp publish'.")


def _cmd_score(args: argparse.Namespace) -> None:
    """Score stage: Gold features (S3) + models (store) -> score tables (S3)."""
    from idp.pipeline import to_scores

    cfg = load_config(args.config)
    result = to_scores(cfg, persist=True)
    print(json.dumps({"score_tables": result["score_tables"]}, indent=2, default=str))
    if not result["score_tables"]:
        print("No scores written — no trained model in the store. Run 'idp labels' and 'idp train' first.")
    else:
        print("Next: 'idp publish' to load the scores and features into the serving store.")


def _cmd_labels(args: argparse.Namespace) -> None:
    """Seed the labels/outcomes store (s3://<prefix>-ml/labels) for offline training."""
    from idp.pipeline import to_labels

    cfg = load_config(args.config)
    labels = to_labels(cfg, persist=True)
    print(json.dumps({"labels_written": int(len(labels)),
                      "tenants": sorted(labels["tenant_id"].unique().tolist())}, indent=2))
    print("Next: 'idp train' to train the models on the Gold features and these labels.")


def _cmd_publish(args: argparse.Namespace) -> None:
    """Serving stage: Gold (S3) -> TimescaleDB (indexed by tenant_id, service_id)."""
    from idp.pipeline import publish_serving

    cfg = load_config(args.config)
    result = publish_serving(cfg)
    print(json.dumps(result, indent=2, default=str))
    print("Next: 'idp serve' (or the cluster's scoring API) answers from these tables.")


def _cmd_train(args: argparse.Namespace) -> None:
    """Offline ML training: Gold features (S3) + labels store -> saved models."""
    from idp.pipeline import train

    cfg = load_config(args.config)
    result = train(cfg, persist=True)
    print(json.dumps({"credit_metrics": result["credit_metrics"],
                      "fraud_metrics": result["fraud_metrics"],
                      "registry": result["registry"]}, indent=2, default=str))
    print("Next: 'idp score' to score with the champion models, then 'idp publish'.")


def _cmd_serve(args: argparse.Namespace) -> None:
    import uvicorn

    uvicorn.run("idp.serving.api:app", host=args.host, port=args.port, reload=False)


def _cmd_mcp(args: argparse.Namespace) -> None:
    """Start the MCP analytics server (stdio): seven read-only, tenant-scoped tools."""
    from idp.mcp.server import main as mcp_main

    mcp_main(config_path=args.config)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="idp", description="Intelligent Data Platform CLI")
    parser.add_argument("--config", default=None, help="Path to platform.yaml")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("generate", help="Generate synthetic data into the landing zone as TM Forum resource files").set_defaults(func=_cmd_generate)

    p_run = sub.add_parser("run", help="Run the end-to-end batch pipeline")
    p_run.add_argument("--no-persist", action="store_true",
                       help="Run against a temporary local lake that is removed afterwards")
    p_run.set_defaults(func=_cmd_run)

    p_prod = sub.add_parser("produce", help="Publish synthetic data to Kafka as TM Forum create events")
    p_prod.add_argument("--bootstrap", default=None, help="Kafka bootstrap (default: config/env)")
    p_prod.add_argument("--from-landing", action="store_true",
                        help="Publish the landing zone's TM Forum files (written by 'idp generate'), "
                             "checked, as TMF create events, instead of generating in memory")
    p_prod.set_defaults(func=_cmd_produce)

    p_cons = sub.add_parser("consume", help="Bronze stage: drain the TMF event topics -> raw Bronze (S3), "
                                            "as received; messages that aren't events -> t.<tenant>.dlq")
    p_cons.add_argument("--bootstrap", default=None, help="Kafka bootstrap (default: config/env)")
    p_cons.add_argument("--idle-timeout", type=float, default=15.0,
                        help="Stop after N seconds with no new messages")
    p_cons.set_defaults(func=_cmd_consume)

    p_silver = sub.add_parser("silver", help="Silver stage: new Bronze (S3) -> canonical Silver (S3)")
    p_silver.add_argument("--full-refresh", action="store_true",
                          help="Reset the checkpoints and rebuild the facts from all of Bronze")
    p_silver.set_defaults(func=_cmd_silver)
    p_sm = sub.add_parser("spark-manifest", help="Render a Spark stage's SparkApplication YAML from its profile")
    from idp.transforms.sparkapp import SPARK_STAGES  # no pyspark: manifests are plain YAML
    p_sm.add_argument("stage", choices=SPARK_STAGES)
    p_sm.add_argument("stage_args", nargs="*", help="arguments for the stage, e.g. -- --full-refresh")
    p_sm.set_defaults(func=_cmd_spark_manifest)
    p_reg = sub.add_parser("registry-schemas",
                           help="Write each Kafka topic's TMF JSON Schema (the registered wire contract)")
    p_reg.add_argument("--out", required=True, help="Directory to write <suffix>.schema.json into")
    p_reg.set_defaults(func=_cmd_registry_schemas)
    sub.add_parser("bi-roles", help="BI tenant roles (idp_bi_<tenant>) and row-level security on the gold "
                   "serving tables; passwords from IDP_BI_PASSWORD_<TENANT>").set_defaults(func=_cmd_bi_roles)
    p_kc = sub.add_parser("keycloak-realm",
                          help="Render the Keycloak realm (groups, tenant clients) from the configured tenants")
    p_kc.add_argument("--out", help="File to write (default deploy/platform/keycloak/idp-realm.json)")
    p_kc.add_argument("--check", action="store_true", help="Only compare; exit 1 if the file differs")
    p_kc.set_defaults(func=_cmd_keycloak_realm)

    p_arch = sub.add_parser("archive-policy", help="Render the Bronze archive rule (S3 lifecycle / MinIO ILM)")
    p_arch.add_argument("--format", choices=("s3", "minio"), default="s3")
    p_arch.add_argument("--storage-class", default="GLACIER_IR",
                        help="S3 storage class, or the MinIO remote tier name")
    p_arch.set_defaults(func=_cmd_archive_policy)
    sub.add_parser("maintain", help="Compact, expire snapshots and remove orphan files on every lake table"
                   ).set_defaults(func=_cmd_maintain)
    sub.add_parser("gold", help="Gold stage: Silver (S3) -> feature tables ONLY (S3)").set_defaults(func=_cmd_gold)
    sub.add_parser("score", help="Score stage: Gold features + models -> score tables (S3)").set_defaults(func=_cmd_score)
    sub.add_parser("labels", help="Seed the labels/outcomes store (training input)").set_defaults(func=_cmd_labels)
    sub.add_parser("publish", help="Serving stage: Gold (S3) -> TimescaleDB").set_defaults(func=_cmd_publish)
    sub.add_parser("train", help="Offline ML training on labelled data -> saved models").set_defaults(func=_cmd_train)

    p_serve = sub.add_parser("serve", help="Start the scoring API")
    p_serve.add_argument("--host", default="0.0.0.0")
    p_serve.add_argument("--port", type=int, default=8000)
    p_serve.set_defaults(func=_cmd_serve)

    sub.add_parser("mcp", help="Start the MCP analytics server (stdio): 7 analytics tools (5 Silver, 2 Gold scores)").set_defaults(func=_cmd_mcp)

    args = parser.parse_args(argv)
    return args.func(args) or 0          # a command may return a failure status


if __name__ == "__main__":
    raise SystemExit(main())
