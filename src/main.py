from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from src.archiver import archive_url
from src.archiver.index import list_snapshots
from src.config import (
    load_job_by_name,
    load_jobs,
    load_source_by_domain,
    load_sources,
    resolve_job_start_urls,
)
from src.discovery import archive_discovered, discover_job_run
from src.jobs import list_job_runs, load_job_run_manifest, run_job
from src.parsers import parse_discovered, parse_job_run, parse_snapshot
from src.pipeline import list_pipeline_runs, load_pipeline_run_manifest, run_job_full
from src.publish import publish_daily, set_listing_status


def _print_json(data: object) -> None:
    print(json.dumps(data, indent=2, ensure_ascii=False))


def _cmd_archive(args: argparse.Namespace) -> int:
    result = archive_url(url=args.url, timeout=args.timeout)
    print(f"status={result.status}")
    print(f"snapshot_id={result.snapshot_id}")
    print(f"run_id={result.run_id}")
    print(f"meta={result.meta_path}")
    return 0 if result.status in {"ok", "partial"} else 1


def _cmd_run_job(args: argparse.Namespace) -> int:
    try:
        result = run_job(job_name=args.job)
    except KeyError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    summary = {
        "job_name": result.job_name,
        "run_id": result.run_id,
        "manifest_path": str(result.manifest_path),
        "total_urls": result.total_urls,
        "ok_count": result.ok_count,
        "partial_count": result.partial_count,
        "error_count": result.error_count,
    }
    _print_json(summary)
    return 0 if result.error_count == 0 else 1


def _cmd_run_job_full(args: argparse.Namespace) -> int:
    try:
        result = run_job_full(
            job_name=args.job,
            resume=args.resume,
            force_discovery=args.force_discovery,
            force_archive_discovered=args.force_archive_discovered,
            force_parse=args.force_parse,
        )
    except Exception as exc:
        print(f"run-job-full failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    _print_json(result.to_dict())
    return 0 if result.status in {"completed", "partial"} else 1


def _cmd_discover_job_run(args: argparse.Namespace) -> int:
    try:
        summary = discover_job_run(job_name=args.job, run_id=args.run_id)
    except Exception as exc:
        print(f"discover-job-run failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    _print_json(summary)
    return 0


def _cmd_archive_discovered(args: argparse.Namespace) -> int:
    try:
        summary = archive_discovered(job_name=args.job, run_id=args.run_id)
    except Exception as exc:
        print(f"archive-discovered failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    _print_json(summary)
    return 0 if summary.get("error_count", 0) == 0 else 1


def _cmd_parse_snapshot(args: argparse.Namespace) -> int:
    try:
        rec = parse_snapshot(args.path)
    except Exception as exc:
        print(f"parse-snapshot failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    _print_json(rec)
    return 0


def _cmd_parse_job_run(args: argparse.Namespace) -> int:
    try:
        summary = parse_job_run(job_name=args.job, run_id=args.run_id)
    except Exception as exc:
        print(f"parse-job-run failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    _print_json(summary)
    return 0


def _cmd_parse_discovered(args: argparse.Namespace) -> int:
    try:
        summary = parse_discovered(job_name=args.job, run_id=args.run_id)
    except Exception as exc:
        print(f"parse-discovered failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    _print_json(summary)
    return 0 if summary.get("error_count", 0) == 0 else 1


def _cmd_publish_daily(args: argparse.Namespace) -> int:
    try:
        summary = publish_daily(job_name=args.job)
    except Exception as exc:
        print(f"publish-daily failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    _print_json(summary)
    return 0


def _cmd_set_listing_status(args: argparse.Namespace) -> int:
    try:
        payload = set_listing_status(
            listing_key=args.listing_key,
            status=args.status,
            note=args.note,
        )
    except Exception as exc:
        print(f"set-listing-status failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    _print_json(payload)
    return 0


def _cmd_list_snapshots(args: argparse.Namespace) -> int:
    rows = list_snapshots(domain=args.domain, date=args.date, status=args.status)

    if args.as_json:
        _print_json(rows)
        return 0

    if not rows:
        print("No snapshots found")
        return 0

    for row in rows:
        print(
            " | ".join(
                [
                    row.get("timestamp_utc", ""),
                    row.get("status", ""),
                    row.get("domain", ""),
                    row.get("snapshot_id", ""),
                    row.get("run_id", ""),
                    row.get("snapshot_path", ""),
                ]
            )
        )
    return 0


def _cmd_list_pipeline_runs(args: argparse.Namespace) -> int:
    rows = list_pipeline_runs(job_name=args.job)

    if args.as_json:
        _print_json(rows)
        return 0

    if not rows:
        print("No pipeline runs found")
        return 0

    for row in rows:
        print(
            " | ".join(
                [
                    row.get("job_name", ""),
                    row.get("pipeline_run_id", ""),
                    row.get("status", ""),
                    row.get("timestamp_utc_start", ""),
                    row.get("timestamp_utc_end", ""),
                ]
            )
        )
    return 0


def _cmd_show_pipeline_run(args: argparse.Namespace) -> int:
    try:
        manifest = load_pipeline_run_manifest(job_name=args.job, pipeline_run_id=args.pipeline_run_id)
    except (KeyError, FileNotFoundError) as exc:
        print(str(exc), file=sys.stderr)
        return 2

    _print_json(manifest)
    return 0


def _cmd_list_sources(args: argparse.Namespace) -> int:
    sources = load_sources()
    if args.as_json:
        _print_json(sources)
        return 0

    for src in sources:
        print(f"{src['domain']} | enabled={src['enabled']} | mode={src['mode']}")
    return 0


def _cmd_show_source(args: argparse.Namespace) -> int:
    try:
        source = load_source_by_domain(args.domain)
    except KeyError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    _print_json(source)
    return 0


def _cmd_list_jobs(args: argparse.Namespace) -> int:
    jobs = load_jobs()
    if args.as_json:
        _print_json(jobs)
        return 0

    for job in jobs:
        print(f"{job['job_name']} | max_urls={job['max_urls']} | sources={len(job['sources'])}")
    return 0


def _cmd_show_job(args: argparse.Namespace) -> int:
    try:
        job = load_job_by_name(args.job)
    except KeyError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    resolved_urls = resolve_job_start_urls(args.job)
    payload = {
        **job,
        "resolved_start_urls": resolved_urls,
    }
    _print_json(payload)
    return 0


def _cmd_list_job_runs(args: argparse.Namespace) -> int:
    rows = list_job_runs(job_name=args.job)

    if args.as_json:
        _print_json(rows)
        return 0

    if not rows:
        print("No job runs found")
        return 0

    for row in rows:
        print(
            " | ".join(
                [
                    row.get("job_name", ""),
                    row.get("run_id", ""),
                    row.get("timestamp_utc_start", ""),
                    f"ok={row.get('ok_count', 0)}",
                    f"partial={row.get('partial_count', 0)}",
                    f"error={row.get('error_count', 0)}",
                ]
            )
        )
    return 0


def _cmd_show_job_run(args: argparse.Namespace) -> int:
    try:
        manifest = load_job_run_manifest(job_name=args.job, run_id=args.run_id)
    except (KeyError, FileNotFoundError) as exc:
        print(str(exc), file=sys.stderr)
        return 2

    _print_json(manifest)
    return 0


def _cmd_legacy(args: argparse.Namespace) -> int:
    repo_root = Path(__file__).resolve().parents[1]
    legacy_script = repo_root / "agent_naves_bizkaia_v14.py"
    if not legacy_script.exists():
        print(f"Legacy script not found: {legacy_script}", file=sys.stderr)
        return 2

    cmd = [sys.executable, str(legacy_script), *args.legacy_args]
    completed = subprocess.run(cmd, cwd=repo_root)
    return completed.returncode


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scraper Inmo CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    archive_parser = subparsers.add_parser("archive", help="Archive a single URL")
    archive_parser.add_argument("--url", required=True, help="Target URL to archive")
    archive_parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds")
    archive_parser.set_defaults(func=_cmd_archive)

    run_job_parser = subparsers.add_parser("run-job", help="Run batch archiving for a configured job")
    run_job_parser.add_argument("--job", required=True, help="Job name")
    run_job_parser.set_defaults(func=_cmd_run_job)

    run_job_full_parser = subparsers.add_parser("run-job-full", help="Run full pipeline for a configured job")
    run_job_full_parser.add_argument("--job", required=True, help="Job name")
    run_job_full_parser.add_argument("--resume", action="store_true", help="Resume last pipeline run for this job")
    run_job_full_parser.add_argument("--force-discovery", action="store_true", help="Force re-run discover-job-run")
    run_job_full_parser.add_argument("--force-archive-discovered", action="store_true", help="Force re-run archive-discovered")
    run_job_full_parser.add_argument("--force-parse", action="store_true", help="Force re-run parse-discovered")
    run_job_full_parser.set_defaults(func=_cmd_run_job_full)

    discover_job_parser = subparsers.add_parser("discover-job-run", help="Discover candidate detail URLs from a job run")
    discover_job_parser.add_argument("--job", required=True, help="Job name")
    discover_job_parser.add_argument("--run-id", required=True, help="Job run id")
    discover_job_parser.add_argument("--json", dest="as_json", action="store_true", help="Output JSON")
    discover_job_parser.set_defaults(func=_cmd_discover_job_run)

    archive_discovered_parser = subparsers.add_parser("archive-discovered", help="Archive discovered URLs of a job run")
    archive_discovered_parser.add_argument("--job", required=True, help="Job name")
    archive_discovered_parser.add_argument("--run-id", required=True, help="Job run id")
    archive_discovered_parser.add_argument("--json", dest="as_json", action="store_true", help="Output JSON")
    archive_discovered_parser.set_defaults(func=_cmd_archive_discovered)

    parse_snapshot_parser = subparsers.add_parser("parse-snapshot", help="Parse one archived snapshot")
    parse_snapshot_parser.add_argument("--path", required=True, help="Snapshot path or meta.json path")
    parse_snapshot_parser.add_argument("--json", dest="as_json", action="store_true", help="Output JSON")
    parse_snapshot_parser.set_defaults(func=_cmd_parse_snapshot)

    parse_job_run_parser = subparsers.add_parser("parse-job-run", help="Parse all snapshots in a job run manifest")
    parse_job_run_parser.add_argument("--job", required=True, help="Job name")
    parse_job_run_parser.add_argument("--run-id", required=True, help="Job run id")
    parse_job_run_parser.add_argument("--json", dest="as_json", action="store_true", help="Output JSON")
    parse_job_run_parser.set_defaults(func=_cmd_parse_job_run)

    parse_discovered_parser = subparsers.add_parser("parse-discovered", help="Parse snapshots generated by archive-discovered")
    parse_discovered_parser.add_argument("--job", required=True, help="Job name")
    parse_discovered_parser.add_argument("--run-id", required=True, help="Job run id")
    parse_discovered_parser.add_argument("--json", dest="as_json", action="store_true", help="Output JSON")
    parse_discovered_parser.set_defaults(func=_cmd_parse_discovered)

    publish_daily_parser = subparsers.add_parser("publish-daily", help="Publicar solo anuncios nuevos del dia")
    publish_daily_parser.add_argument("--job", required=True, help="Job name")
    publish_daily_parser.set_defaults(func=_cmd_publish_daily)

    set_status_parser = subparsers.add_parser("set-listing-status", help="Actualizar estado de trabajo de un anuncio")
    set_status_parser.add_argument("--listing-key", required=True, help="Listing key persistente")
    set_status_parser.add_argument(
        "--status",
        required=True,
        choices=["pending", "processed", "discarded"],
        help="Nuevo estado de workflow",
    )
    set_status_parser.add_argument("--note", help="Nota opcional")
    set_status_parser.set_defaults(func=_cmd_set_listing_status)

    list_parser = subparsers.add_parser("list-snapshots", help="List archived snapshots from global index")
    list_parser.add_argument("--domain", help="Filter by domain")
    list_parser.add_argument("--date", help="Filter by date YYYY-MM-DD")
    list_parser.add_argument("--status", help="Filter by status (ok, partial, error)")
    list_parser.add_argument("--json", dest="as_json", action="store_true", help="Output raw JSON")
    list_parser.set_defaults(func=_cmd_list_snapshots)

    list_pipeline_parser = subparsers.add_parser("list-pipeline-runs", help="List pipeline full-run executions")
    list_pipeline_parser.add_argument("--job", help="Filter by job name")
    list_pipeline_parser.add_argument("--json", dest="as_json", action="store_true", help="Output raw JSON")
    list_pipeline_parser.set_defaults(func=_cmd_list_pipeline_runs)

    show_pipeline_parser = subparsers.add_parser("show-pipeline-run", help="Show one pipeline run manifest")
    show_pipeline_parser.add_argument("--job", required=True, help="Job name")
    show_pipeline_parser.add_argument("--pipeline-run-id", required=True, help="Pipeline run id")
    show_pipeline_parser.set_defaults(func=_cmd_show_pipeline_run)

    list_sources_parser = subparsers.add_parser("list-sources", help="List configured sources")
    list_sources_parser.add_argument("--json", dest="as_json", action="store_true", help="Output raw JSON")
    list_sources_parser.set_defaults(func=_cmd_list_sources)

    show_source_parser = subparsers.add_parser("show-source", help="Show source config by domain")
    show_source_parser.add_argument("--domain", required=True, help="Source domain (e.g. pisos.com)")
    show_source_parser.set_defaults(func=_cmd_show_source)

    list_jobs_parser = subparsers.add_parser("list-jobs", help="List configured jobs")
    list_jobs_parser.add_argument("--json", dest="as_json", action="store_true", help="Output raw JSON")
    list_jobs_parser.set_defaults(func=_cmd_list_jobs)

    show_job_parser = subparsers.add_parser("show-job", help="Show job config")
    show_job_parser.add_argument("--job", required=True, help="Job name")
    show_job_parser.set_defaults(func=_cmd_show_job)

    list_job_runs_parser = subparsers.add_parser("list-job-runs", help="List job batch executions")
    list_job_runs_parser.add_argument("--job", help="Filter by job name")
    list_job_runs_parser.add_argument("--json", dest="as_json", action="store_true", help="Output raw JSON")
    list_job_runs_parser.set_defaults(func=_cmd_list_job_runs)

    show_job_run_parser = subparsers.add_parser("show-job-run", help="Show job run manifest")
    show_job_run_parser.add_argument("--job", required=True, help="Job name")
    show_job_run_parser.add_argument("--run-id", required=True, help="Run id")
    show_job_run_parser.set_defaults(func=_cmd_show_job_run)

    legacy_parser = subparsers.add_parser("legacy", help="Run legacy baseline script")
    legacy_parser.add_argument("legacy_args", nargs=argparse.REMAINDER, help="Args passed to legacy script")
    legacy_parser.set_defaults(func=_cmd_legacy)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
