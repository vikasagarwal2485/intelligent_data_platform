"""Command-line entrypoint for the platform.

Usage:
    idp generate   # write synthetic data to the landing zone
    idp run        # run the full Bronze->Silver->Features->ML pipeline
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

    p_serve = sub.add_parser("serve", help="Start the scoring API")
    p_serve.add_argument("--host", default="0.0.0.0")
    p_serve.add_argument("--port", type=int, default=8000)
    p_serve.set_defaults(func=_cmd_serve)

    args = parser.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
