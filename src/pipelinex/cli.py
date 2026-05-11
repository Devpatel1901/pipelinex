"""Command-line entry point for PipelineX.

Subcommands:

- ``pipelinex run <config.yaml>`` — execute a pipeline end-to-end.
- ``pipelinex serve`` — start the FastAPI query server.
- ``pipelinex info`` — print the build version and a sanity check.

The CLI is intentionally thin: heavy logic lives in ``core/builder.py``.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from pipelinex import __version__


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    _configure_logging(args.verbose)
    if args.command == "run":
        return asyncio.run(_run(args))
    if args.command == "serve":
        return _serve(args)
    if args.command == "info":
        return _info()
    return 2


# ---------- subcommands ----------


async def _run(args: argparse.Namespace) -> int:
    from pipelinex.core.builder import build_pipeline
    from pipelinex.core.config import load_config

    cfg = load_config(args.config)
    if args.source_path is not None:
        cfg.source.path = args.source_path
        cfg.source.kind = "file"
    if args.workers is not None:
        cfg.runtime.num_workers = args.workers

    built = build_pipeline(cfg)
    if built.sessionizer is not None:
        await built.sessionizer.start()
    try:
        result = await built.executor.run()
    finally:
        if built.sessionizer is not None:
            await built.sessionizer.stop()

    print(
        f"\nrun {result.run_id}: processed={result.records_processed:,} "
        f"failed={result.records_failed:,} "
        f"elapsed={(result.completed_at - result.started_at).total_seconds():.2f}s"
    )

    all_fn = getattr(built.repository, "all_anomalies", None)
    if all_fn is not None:
        anomalies = await all_fn()
        if anomalies:
            print(f"anomalies detected: {len(anomalies)}")

    return 0


def _serve(args: argparse.Namespace) -> int:
    try:
        import uvicorn
    except ImportError:
        print("uvicorn is required for `pipelinex serve`", file=sys.stderr)
        return 2
    uvicorn.run(
        "pipelinex.api.app:app",
        host=args.host,
        port=args.port,
        log_level="info",
    )
    return 0


def _info() -> int:
    print(f"pipelinex {__version__}")
    print(f"  python: {sys.version.split()[0]}")
    print(f"  cwd:    {Path.cwd()}")
    return 0


# ---------- argparse ----------


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="pipelinex")
    p.add_argument("-v", "--verbose", action="count", default=0)
    sub = p.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="execute a pipeline from a YAML config")
    run.add_argument("config", type=Path)
    run.add_argument("--source-path", type=str, default=None)
    run.add_argument("--workers", type=int, default=None)

    serve = sub.add_parser("serve", help="start the FastAPI query server")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)

    sub.add_parser("info", help="print build info")

    return p.parse_args(argv)


def _configure_logging(verbose: int) -> None:
    level = logging.WARNING
    if verbose == 1:
        level = logging.INFO
    elif verbose >= 2:
        level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )


if __name__ == "__main__":
    sys.exit(main())
