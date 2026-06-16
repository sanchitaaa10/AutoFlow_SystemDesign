from __future__ import annotations

import argparse
import json

from autoflow import AutoFlowEngine
from examples.demo_workflows import run_demo


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="AutoFlow workflow automation system demo"
    )
    subparsers = parser.add_subparsers(dest="command")

    demo = subparsers.add_parser("demo", help="Run the sample AutoFlow workflows")
    demo.add_argument(
        "--database",
        default="autoflow_demo.db",
        help="SQLite database path used for the demo",
    )

    dashboard = subparsers.add_parser(
        "dashboard", help="Print monitoring data for an existing AutoFlow database"
    )
    dashboard.add_argument(
        "--database",
        default="autoflow_demo.db",
        help="SQLite database path to inspect",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "demo":
        run_demo(args.database)
        return

    if args.command == "dashboard":
        engine = AutoFlowEngine(args.database, worker_count=0)
        print(json.dumps(engine.monitoring.dashboard(), indent=2))
        return

    parser.print_help()


if __name__ == "__main__":
    main()

