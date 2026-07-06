"""Command-line entrypoint for the platform.

Usage (separated medallion stages):
    idp produce    # synthetic data -> Kafka topics
    idp consume    # Kafka -> Bronze (S3)          [Bronze stage]
    idp silver     # Bronze (S3) -> Silver (S3)    [Silver stage]
    idp gold       # Silver (S3) -> Gold (S3)      [Gold stage: features + scores]
    idp train      # labelled data -> saved models [offline ML]
    idp run        # local dev: compose all stages end-to-end (generate->...->gold)
    idp generate   # write synthetic data to the landing zone (files)
    idp serve      # start the FastAPI scoring service
"""
from __future__ import annotations

import argparse
import json

from idp.common.config import load_config


def _cmd_generate(args: argparse.Namespace) -> None:
    from idp.ingestion.bronze import write_landing
    from idp.synthetic.generator import generate_for_tenants

    cfg = load_config(args.config)
    syn = cfg.synthetic
    dataset = generate_for_tenants(
        cfg.tenant_ids,
        subscribers=int(syn.get("subscribers_per_tenant", 200)),
        days=int(syn.get("days", 60)),
        fraud_rate=float(syn.get("fraud_rate", 0.05)),
        default_rate=float(syn.get("default_rate", 0.12)),
        seed=int(syn.get("seed", 42)),
    )
    written = write_landing(dataset, cfg.storage.landing)
    total = sum(len(v) for v in written.values())
    print(f"Wrote {total} landing files for {len(cfg.tenant_ids)} tenant(s) to {cfg.storage.landing}")


def _cmd_run(args: argparse.Namespace) -> None:
    from idp.pipeline import run

    cfg = load_config(args.config)
    summary = run(cfg, persist=not args.no_persist)
    printable = {k: v for k, v in summary.items() if k not in ("credit_scores", "fraud_scores", "silver")}
    print(json.dumps(printable, indent=2, default=str))


def _cmd_produce(args: argparse.Namespace) -> None:
    from idp.streaming.producer import produce
    from idp.synthetic.generator import generate

    cfg = load_config(args.config)
    syn = cfg.synthetic
    bootstrap = args.bootstrap or cfg.kafka["bootstrap"]
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
        counts = produce(dataset, bootstrap, topic_prefix=cfg.kafka["topic_prefix"])
        for k, v in counts.items():
            totals[k] = totals.get(k, 0) + v
        del dataset
        print(f"[{tenant_id}] published {sum(counts.values())} messages")
    print(f"Published to Kafka at {bootstrap}:")
    print(json.dumps(totals, indent=2))


def _cmd_consume(args: argparse.Namespace) -> None:
    """Bronze stage only: Kafka -> Bronze. Silver/Gold are separate stages."""
    from idp.streaming.consumer import consume_to_bronze
    from idp.pipeline import to_bronze

    cfg = load_config(args.config)
    bootstrap = args.bootstrap or cfg.kafka["bootstrap"]
    print(f"Draining topics from {bootstrap} (idle timeout {args.idle_timeout}s)...")
    bronze = consume_to_bronze(
        bootstrap, cfg.tenant_ids,
        topic_prefix=cfg.kafka["topic_prefix"], idle_timeout=args.idle_timeout,
    )
    total = sum(len(df) for df in bronze.values())
    print(f"Consumed {total} records: { {k: len(v) for k, v in bronze.items()} }")
    if total == 0:
        print("No messages consumed (topics empty or producer not finished). "
              "Run 'idp produce' first.")
        return
    result = to_bronze(cfg, bronze, persist=True)
    print(f"Wrote Bronze tables: {result['bronze_tables']}")
    print("Next: 'idp silver' to build Silver from Bronze.")


def _cmd_silver(args: argparse.Namespace) -> None:
    """Silver stage: Bronze (S3) -> canonical Silver (S3)."""
    from idp.pipeline import to_silver

    cfg = load_config(args.config)
    silver = to_silver(cfg, persist=True)
    print(json.dumps({"silver_tables": {k: len(v) for k, v in silver.items()}}, indent=2))
    print("Next: 'idp gold' to build Gold from Silver.")


def _cmd_gold(args: argparse.Namespace) -> None:
    """Gold stage: Silver (S3) -> feature tables (+ scores if models exist) (S3)."""
    from idp.pipeline import to_gold

    cfg = load_config(args.config)
    result = to_gold(cfg, persist=True)
    print(json.dumps({"gold_tables": result["gold_tables"], "counts": result["counts"]},
                     indent=2, default=str))


def _cmd_train(args: argparse.Namespace) -> None:
    """Offline ML training on a labelled dataset -> saved models."""
    from idp.pipeline import train

    cfg = load_config(args.config)
    result = train(cfg, persist=True)
    print(json.dumps({"credit_metrics": result["credit_metrics"],
                      "fraud_metrics": result["fraud_metrics"]}, indent=2, default=str))


def _cmd_serve(args: argparse.Namespace) -> None:
    import uvicorn

    uvicorn.run("idp.serving.api:app", host=args.host, port=args.port, reload=False)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="idp", description="Intelligent Data Platform CLI")
    parser.add_argument("--config", default=None, help="Path to platform.yaml")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("generate", help="Generate synthetic data into the landing zone").set_defaults(func=_cmd_generate)

    p_run = sub.add_parser("run", help="Run the end-to-end batch pipeline")
    p_run.add_argument("--no-persist", action="store_true", help="Run in-memory (skip writing the lake)")
    p_run.set_defaults(func=_cmd_run)

    p_prod = sub.add_parser("produce", help="Publish synthetic data to Kafka topics")
    p_prod.add_argument("--bootstrap", default=None, help="Kafka bootstrap (default: config/env)")
    p_prod.set_defaults(func=_cmd_produce)

    p_cons = sub.add_parser("consume", help="Bronze stage: drain Kafka topics -> Bronze (S3)")
    p_cons.add_argument("--bootstrap", default=None, help="Kafka bootstrap (default: config/env)")
    p_cons.add_argument("--idle-timeout", type=float, default=15.0,
                        help="Stop after N seconds with no new messages")
    p_cons.set_defaults(func=_cmd_consume)

    sub.add_parser("silver", help="Silver stage: Bronze (S3) -> canonical Silver (S3)").set_defaults(func=_cmd_silver)
    sub.add_parser("gold", help="Gold stage: Silver (S3) -> feature tables + scores (S3)").set_defaults(func=_cmd_gold)
    sub.add_parser("train", help="Offline ML training on labelled data -> saved models").set_defaults(func=_cmd_train)

    p_serve = sub.add_parser("serve", help="Start the scoring API")
    p_serve.add_argument("--host", default="0.0.0.0")
    p_serve.add_argument("--port", type=int, default=8000)
    p_serve.set_defaults(func=_cmd_serve)

    args = parser.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
