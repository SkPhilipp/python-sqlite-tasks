# SkPhilipp/tasks

[![Status: Alpha](https://img.shields.io/badge/status-alpha-red)](https://release-engineers.com/open-source-badges/)

For serious task management on a budget. This is a Python library for orchestrating long-lived tasks without requiring any backing infrastructure. Tasks
are managed by both producers and consumers through a shared SQL database.

Notable architectural features:

- Both producers and consumers do not communicate with or amongst each other.
- Both producers and consumers do not need to be running at all times.
- No central broker other than a SQL database is required, which could even be a SQLite file.

The [demo](demo) package includes an executable task producer and consumer combination sharing a local SQLite database.

### Producers

Producers configure and emit tasks as well as follow their progression, like so:

```commandline
#!/usr/bin/env python3
from tasks.framework import TaskFramework
from tasks.sqlite import SqliteTaskService

framework = TaskFramework(SqliteTaskService("sqlite:///tasks.db"))
task = framework.queue("hello", {"name": "world"})
for frame in framework.follow(task):
    print(frame)
```

### Consumers

Consumers can claim tasks and report back data, logs and status updates through a `Task` object:

```python
#!/usr/bin/env python3
from tasks.framework import TaskFramework, Task
from tasks.sqlite import SqliteTaskService

framework = TaskFramework(SqliteTaskService("sqlite:///tasks.db"))


@framework.handler()
def hello(task: Task, name: str):
    task.log_info(f"Hello, {name}!")


framework.main()
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
