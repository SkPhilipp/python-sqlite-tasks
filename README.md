# SkPhilipp/tasks

[![Status: Alpha](https://img.shields.io/badge/status-alpha-red)](https://release-engineers.com/open-source-badges/)

## Description

This is a Python package for managing long-lived tasks. Task management is tracked in a SQL database shared by both producers and consumers of tasks.
Producers configure and emit tasks and can monitor their progression. Consumers can claim tasks and yield data, logs and status updates.

The [demo](demo) package includes an executable task producer and consumer combination sharing a local SQLite database.

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
