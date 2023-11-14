# SkPhilipp/tasks

[![Status: Alpha](https://img.shields.io/badge/status-alpha-red)](https://release-engineers.com/open-source-badges/)

## Description

This is a Python package for managing long-lived tasks. Task management is done through a shared SQL database used by both producers and consumers of tasks.
Neither producers nor consumers are required to be running at all times or are in any way connected to each other.

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

Consumers can claim tasks and yield data, logs and status updates, like so:

```python
from tasks.framework import TaskFramework, Task
from tasks.sqlite import SqliteTaskService

framework = TaskFramework(SqliteTaskService("sqlite:///tasks.db"))


@framework.handler()
def hello(task: Task, name: str):
    task.log_info(f"Hello, {name}!")


framework.main()
```

Note that the `Task` interface contains many utilities to report back, including progress tracking, data transfer and logging.

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
