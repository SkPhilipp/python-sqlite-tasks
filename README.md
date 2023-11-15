# SkPhilipp/tasks

[![Status: Alpha](https://img.shields.io/badge/status-alpha-red)](https://release-engineers.com/open-source-badges/)

For serious task management on a budget. This is a Python library for orchestrating long-lived tasks without requiring any backing infrastructure. Tasks
are managed by both producers and consumers through a shared SQL database, which can even be a local SQLite file.

Notable architectural features:

- Both producers and consumers do not directly communicate with or amongst each other.
- Both producers and consumers do not need to be running at all times.
- No central broker or infrastructure other than a SQL database is required.

The [demo](demo) package includes an executable task producer and consumer combination sharing a local SQLite database.

### Producers

Producers configure and emit tasks as well as follow their progression, like so:

```python
#!/usr/bin/env python3
from tasks.console import Console
from tasks.sqlite import SqliteTaskService

task_service = SqliteTaskService("sqlite:///tasks.db")
task = task_service.queue("hello", {"name": "world"})
console = Console()
console.follow(task)    
```

### Consumers

Consumers can claim tasks and report back data, logs and status updates through a `Task` object:

```python
from tasks.framework import Task, TaskRegistry
from tasks.sqlite import SqliteTaskService

task_service = SqliteTaskService("sqlite:///tasks.db")
task_registry = TaskRegistry()


@task_registry.task_handler()
def hello(task: Task, name: str):
    task.log_info(f"Hello, {name}!")


task_registry.listen(task_service)

```

## Installation

Build the Python package with:

```bash
poetry install
poetry build --format wheel
```

Install it using pip:

```bash
pip install dist/tasks-*.whl
```

## Contributing

This is Python Poetry project.
See [Poetry](https://python-poetry.org/) for more information.

Development requires:

- [Python](https://www.python.org/)
- [Poetry](https://python-poetry.org/)

## Links

This project was created using [template-poetry](https://github.com/release-engineers/template-poetry).
