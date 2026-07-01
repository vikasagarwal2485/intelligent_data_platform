"""Command-line entrypoint for the platform.

Usage:
    idp generate   # write synthetic data to the landing zone (files)
    idp run        # batch: synthetic -> Bronze -> Silver -> Gold + ML (local)
    idp produce    # streaming: publish synthetic data to Kafka topics
    idp consume    # streaming: drain Kafka -> Bronze -> Silver -> Gold
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
    from idp.synthetic.generator import generate_for_tenants

    cfg = load_config(args.config)
    syn = cfg.synthetic
    bootstrap = args.bootstrap or cfg.kafka["bootstrap"]
    dataset = generate_for_tenants(
        cfg.tenant_ids,
        subscribers=int(syn.get("subscribers_per_tenant", 200)),
        days=int(syn.get("days", 60)),
        fraud_rate=float(syn.get("fraud_rate", 0.05)),
        default_rate=float(syn.get("default_rate", 0.12)),
        seed=int(syn.get("seed", 42)),
    )
    counts = produce(dataset, bootstrap, topic_prefix=cfg.kafka["topic_prefix"])
    print(f"Published to Kafka at {bootstrap}:")
    print(json.dumps(counts, indent=2))


def _cmd_consume(args: argparse.Namespace) -> None:
    from idp.streaming.consumer import consume_to_bronze
    from idp.pipeline import process_bronze
    from idp.common import storage

    cfg = load_config(args.config)
    bootstrap = args.bootstrap or cfg.kafka["bootstrap"]
    print(f"Draining topics from {bootstrap} (idle timeout {args.idle_timeout}s)...")
    bronze = consume_to_bronze(
        bootstrap, cfg.tenant_ids,
        topic_prefix=cfg.kafka["topic_prefix"], idle_timeout=args.idle_timeout,
    )
    # persist raw Bronze
    for name, df in bronze.items():
        storage.write_parquet(df, f"{storage.lake_uri(cfg, 'bronze')}/{name}", cfg,
                              partition_cols=["tenant_id"])
    summary = process_bronze(cfg, bronze, persist=True)
    print(json.dumps({"bronze_tables": list(bronze), "counts": summary["counts"],
                      "gold_tables": summary["gold_tables"]}, indent=2, default=str))


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

    p_cons = sub.add_parser("consume", help="Drain Kafka topics -> Bronze -> Silver -> Gold")
    p_cons.add_argument("--bootstrap", default=None, help="Kafka bootstrap (default: config/env)")
    p_cons.add_argument("--idle-timeout", type=float, default=15.0,
                        help="Stop after N seconds with no new messages")
    p_cons.set_defaults(func=_cmd_consume)

    p_serve = sub.add_parser("serve", help="Start the scoring API")
    p_serve.add_argument("--host", default="0.0.0.0")
    p_serve.add_argument("--port", type=int, default=8000)
    p_serve.set_defaults(func=_cmd_serve)

    args = parser.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
