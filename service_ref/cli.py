from __future__ import annotations

import argparse
import logging
from pathlib import Path

from service_ref.config import BuildConfig, ReviewConfig
from service_ref.review.assistant import run as run_review_assist
from service_ref.steps import step_01_load, step_02_normalize, step_03_match, step_04_consolidate, step_05_publish


LOG = logging.getLogger("service_ref.cli")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m service_ref")
    parser.add_argument("--db", dest="db_path", type=Path, default=None)
    parser.add_argument("--out-dir", dest="out_dir", type=Path, default=None)
    parser.add_argument("--verbose", action="store_true")

    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ["load", "normalize", "match", "consolidate", "publish", "run-all"]:
        subparsers.add_parser(name)

    review = subparsers.add_parser("review-assist")
    review.add_argument("--dry-run", action="store_true", default=False)
    review.add_argument("--max-services", type=int, default=None)
    review.add_argument("--model", type=str, default="claude-sonnet")
    return parser


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO, format="%(levelname)s %(message)s")


def build_cfg(args: argparse.Namespace) -> BuildConfig:
    cfg = BuildConfig()
    if args.out_dir is not None:
        cfg.out_dir = args.out_dir
        if args.db_path is None:
            cfg.db_path = cfg.out_dir / "service_referential.sqlite"
    if args.db_path is not None:
        cfg.db_path = args.db_path
        if args.out_dir is None:
            cfg.out_dir = cfg.db_path.parent
    cfg.verbose = bool(args.verbose)
    return cfg


def _log_step_result(step_name: str, stats: dict[str, int]) -> None:
    summary = ", ".join(f"{key}={value}" for key, value in stats.items())
    LOG.info("Step %s completed: %s", step_name, summary)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.verbose)
    cfg = build_cfg(args)

    if args.command == "load":
        _log_step_result("load", step_01_load.run(cfg))
        return 0
    if args.command == "normalize":
        _log_step_result("normalize", step_02_normalize.run(cfg))
        return 0
    if args.command == "match":
        _log_step_result("match", step_03_match.run(cfg))
        return 0
    if args.command == "consolidate":
        _log_step_result("consolidate", step_04_consolidate.run(cfg))
        return 0
    if args.command == "publish":
        _log_step_result("publish", step_05_publish.run(cfg))
        return 0
    if args.command == "run-all":
        _log_step_result("load", step_01_load.run(cfg))
        _log_step_result("normalize", step_02_normalize.run(cfg))
        _log_step_result("match", step_03_match.run(cfg))
        _log_step_result("consolidate", step_04_consolidate.run(cfg))
        _log_step_result("publish", step_05_publish.run(cfg))
        return 0
    if args.command == "review-assist":
        review_cfg = ReviewConfig(
            out_dir=cfg.out_dir,
            db_path=cfg.db_path,
            max_services=args.max_services,
            dry_run=bool(args.dry_run),
            model=args.model,
        )
        _log_step_result("review-assist", run_review_assist(review_cfg))
        return 0
    parser.error(f"Unsupported command: {args.command}")
    return 2
