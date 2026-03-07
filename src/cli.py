"""CLI entry point for the service-ref agent."""

from __future__ import annotations

import argparse
import asyncio
import sqlite3
import subprocess
import sys
from pathlib import Path

from rich.console import Console

console = Console()


def main():
    parser = argparse.ArgumentParser(
        description="Agent Service-Ref v0.1.0 — Reconciliation autonome BSS/OSS"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # prepare
    p_prepare = sub.add_parser("prepare", help="Prepare le workspace et cree les tables agent")
    p_prepare.add_argument("--workspace", "-w", default=".", help="Repertoire de travail")
    p_prepare.add_argument("--rebuild", action="store_true", help="Force le rebuild du pipeline service_ref run-all")

    # interactive
    p_inter = sub.add_parser("interactive", help="Mode interactif avec l'agent")
    p_inter.add_argument("--workspace", "-w", default=".", help="Repertoire de travail")

    # batch
    p_batch = sub.add_parser("batch", help="Execution non-interactive d'un prompt")
    p_batch.add_argument("--workspace", "-w", default=".", help="Repertoire de travail")
    p_batch.add_argument("--prompt", "-p", required=True, help="Prompt a executer")

    # baseline
    p_baseline = sub.add_parser("baseline", help="Compare les resolutions agent aux auto_valid du pipeline")
    p_baseline.add_argument("--workspace", "-w", default=".", help="Repertoire de travail")

    args = parser.parse_args()

    if args.command == "prepare":
        _prepare(args.workspace, rebuild=args.rebuild)
    elif args.command == "interactive":
        from .agent import interactive_session
        asyncio.run(interactive_session(workspace=args.workspace))
    elif args.command == "batch":
        from .batch import batch_run
        asyncio.run(batch_run(args.prompt, workspace=args.workspace))
    elif args.command == "baseline":
        _baseline(args.workspace)


def _prepare(workspace: str, rebuild: bool = False):
    """Verify workspace and create agent tables."""
    ws = Path(workspace).resolve()
    db_path = ws / "service_ref" / "output" / "service_referential.sqlite"
    config_dir = ws / "unzipped_equip"

    console.print(f"[bold]Workspace:[/bold] {ws}")

    # Run pipeline if DB missing or --rebuild requested
    if not db_path.exists() or rebuild:
        reason = "--rebuild demande" if rebuild else "SQLite introuvable"
        console.print(f"[yellow]{reason}[/yellow] — lancement du pipeline service_ref run-all...")
        result = subprocess.run(
            [sys.executable, "-m", "service_ref", "run-all"],
            cwd=str(ws),
        )
        if result.returncode != 0:
            console.print("[red]ERREUR:[/red] le pipeline service_ref run-all a echoue.")
            return

    # Check SQLite exists (post-pipeline)
    if not db_path.exists():
        console.print(f"[red]ERREUR:[/red] SQLite introuvable apres pipeline: {db_path}")
        return

    console.print(f"[green]SQLite:[/green] {db_path}")

    # Check config dir
    if config_dir.exists():
        txt_files = list(config_dir.rglob("*.txt"))
        if txt_files:
            console.print(f"[green]Configs reseau:[/green] {len(txt_files)} fichiers .txt")
        else:
            console.print(f"[yellow]AVERTISSEMENT:[/yellow] {config_dir} existe mais ne contient aucun fichier .txt")
            console.print("  -> Placer les exports RANCID (.txt) dans ce repertoire pour activer search_configs/read_config_file")
    else:
        console.print(f"[yellow]AVERTISSEMENT:[/yellow] {config_dir} introuvable")
        console.print("  -> Creer ce repertoire et y placer les exports RANCID (.txt) pour activer search_configs/read_config_file")

    # Check project_context.md
    ctx_path = ws / "project_context.md"
    if ctx_path.exists():
        console.print(f"[green]Contexte projet:[/green] {ctx_path}")
    else:
        console.print(f"[yellow]AVERTISSEMENT:[/yellow] project_context.md manquant")

    # Create agent tables
    from .tools.resolution_tools import ensure_agent_tables
    ensure_agent_tables(db_path)
    console.print("[green]Tables agent creees/verifiees[/green]")

    # Stats
    con = sqlite3.connect(str(db_path))
    try:
        total = con.execute("SELECT COUNT(*) FROM service_master_active").fetchone()[0]

        gold_states = con.execute(
            "SELECT match_state, COUNT(*) c FROM gold_service_active GROUP BY match_state"
        ).fetchall()
        state_map = {r[0]: r[1] for r in gold_states}
        auto_valid = state_map.get("auto_valid", 0)
        review_required = state_map.get("review_required", 0)

        agent_count = con.execute("SELECT COUNT(*) FROM agent_resolutions").fetchone()[0]

        console.print()
        console.print(f"[bold]{total} services[/bold], "
                       f"{auto_valid} auto-valides, "
                       f"{review_required} en attente, "
                       f"{agent_count} resolus par l'agent")
    finally:
        con.close()

    # Verify agent tables exist
    con = sqlite3.connect(str(db_path))
    try:
        tables = [r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'agent_%'"
        ).fetchall()]
        console.print(f"[green]Tables agent:[/green] {', '.join(tables)}")
    finally:
        con.close()

    console.print("\n[bold green]Workspace pret.[/bold green]")


def _baseline(workspace: str):
    """Compare agent resolutions against pipeline auto_valid services."""
    ws = Path(workspace).resolve()
    db_path = ws / "service_ref" / "output" / "service_referential.sqlite"

    if not db_path.exists():
        console.print(f"[red]ERREUR:[/red] SQLite introuvable: {db_path}")
        return

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    try:
        # Check agent tables exist
        has_agent = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='agent_resolutions'"
        ).fetchone()
        if not has_agent:
            console.print("[red]ERREUR:[/red] Pas de table agent_resolutions. Lancer 'prepare' d'abord.")
            return

        # Get auto_valid services with their gold data
        auto_valid = con.execute("""
            SELECT g.service_id, s.principal_client, s.nature_service,
                   e_a.matched_site_id AS gold_site_a,
                   e_z.matched_site_id AS gold_site_z,
                   p.party_id AS gold_party_id
            FROM gold_service_active g
            JOIN service_master_active s ON s.service_id = g.service_id
            LEFT JOIN service_endpoint e_a ON e_a.service_id = g.service_id AND e_a.endpoint_label = 'A'
            LEFT JOIN service_endpoint e_z ON e_z.service_id = g.service_id AND e_z.endpoint_label = 'Z'
            LEFT JOIN service_party p ON p.service_id = g.service_id AND p.role_name = 'client_final'
            WHERE g.match_state = 'auto_valid'
            ORDER BY g.service_id
        """).fetchall()

        if not auto_valid:
            console.print("[yellow]Aucun service auto_valid dans le pipeline.[/yellow]")
            return

        console.print(f"\n[bold]BASELINE — {len(auto_valid)} services auto_valid du pipeline[/bold]\n")

        total = len(auto_valid)
        agent_covered = 0
        site_match = 0
        party_match = 0
        site_total = 0
        party_total = 0

        lines = [
            "service_id | client | agent_status | site_ok | party_ok",
            "--- | --- | --- | --- | ---",
        ]

        for svc in auto_valid:
            sid = svc["service_id"]
            agent = con.execute(
                "SELECT confidence, status, site_a, site_z, party_final_id FROM agent_resolutions WHERE service_id = ? ORDER BY created_at DESC LIMIT 1",
                (sid,),
            ).fetchone()

            if not agent:
                lines.append(f"{sid} | {svc['principal_client']} | - | - | -")
                continue

            agent_covered += 1
            agent_status = f"{agent['confidence']}/{agent['status']}"

            # Compare sites
            s_ok = "-"
            if svc["gold_site_a"] or svc["gold_site_z"]:
                site_total += 1
                agent_sites = {(agent["site_a"] or "").upper(), (agent["site_z"] or "").upper()} - {""}
                gold_sites = {(svc["gold_site_a"] or "").upper(), (svc["gold_site_z"] or "").upper()} - {""}
                if agent_sites and gold_sites and agent_sites & gold_sites:
                    site_match += 1
                    s_ok = "OK"
                elif agent_sites:
                    s_ok = "DIFF"

            # Compare party
            p_ok = "-"
            if svc["gold_party_id"]:
                party_total += 1
                if agent["party_final_id"] and agent["party_final_id"] == svc["gold_party_id"]:
                    party_match += 1
                    p_ok = "OK"
                elif agent["party_final_id"]:
                    p_ok = "DIFF"

            lines.append(f"{sid} | {svc['principal_client']} | {agent_status} | {s_ok} | {p_ok}")

        console.print("\n".join(lines))

        console.print(f"\n[bold]Resume:[/bold]")
        console.print(f"  Services auto_valid: {total}")
        console.print(f"  Couverts par l'agent: {agent_covered}/{total} ({agent_covered/total*100:.0f}%)")
        if site_total:
            console.print(f"  Site match: {site_match}/{site_total} ({site_match/site_total*100:.0f}%)")
        if party_total:
            console.print(f"  Party match: {party_match}/{party_total} ({party_match/party_total*100:.0f}%)")
        if site_total and party_total:
            accord = (site_match + party_match) / (site_total + party_total) * 100
            console.print(f"  Accord global: {accord:.0f}%")

    finally:
        con.close()


if __name__ == "__main__":
    main()
