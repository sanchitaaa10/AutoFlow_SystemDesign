# AutoFlow - Workflow Automation System

AutoFlow is a Python implementation of a simplified enterprise workflow automation platform inspired by tools such as Zapier and Microsoft Power Automate. It demonstrates how workflows can be defined, triggered, queued, executed by workers, monitored, retried, and stored reliably.

Repository link: https://github.com/sanchitaaa10/AutoFlow_SystemDesign.git

## Project Overview

The project implements the main backend logic for a workflow automation system:

- Workflow definitions with triggers, steps, dependencies, retry policy, and optional conditions.
- Event based execution where incoming events start matching workflows.
- In-memory message broker that simulates RabbitMQ style asynchronous task dispatch.
- Worker threads that execute workflow steps concurrently.
- SQLite persistence for workflows, runs, step runs, event logs, audit trails, user configuration, and dead letter events.
- Monitoring dashboard data for success, failure, pending, queue depth, and recent logs.
- Built-in actions for payload validation, email sending, database updates, report generation, approval, payload transformation, and external API calls.

## Setup Instructions

Use Python 3.10 or newer.

```bash
cd AutoFlow_SystemDesign
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

The runtime uses only the Python standard library.

## Execution Steps

Run the complete demo:

```bash
python main.py demo
```

View the monitoring dashboard for the generated demo database:

```bash
python main.py dashboard --database autoflow_demo.db
```

Run tests:

```bash
python -m unittest discover -s tests
```

## Project Structure

```text
autoflow/
  actions.py       Built-in workflow action handlers
  broker.py        In-memory message queue
  database.py      SQLite persistence layer
  engine.py        Public engine API and worker lifecycle
  models.py        Workflow, event, task, and status models
  monitoring.py    Monitoring and run detail views
  orchestrator.py  Trigger matching and workflow coordination
  utils.py         JSON, templating, and condition helpers
examples/
  demo_workflows.py
tests/
  test_autoflow_engine.py
main.py
```

## Additional Project Details

The implementation is intentionally local and lightweight so it can be submitted and executed easily. In a production system, the in-memory broker can be replaced with RabbitMQ or Kafka, SQLite can be replaced with PostgreSQL, and the worker pool can be deployed as independent services.

