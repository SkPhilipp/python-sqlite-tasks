from tasks.console import Console
from tasks.sqlite import SqliteTaskService


def main():
    task_service = SqliteTaskService("sqlite:///tasks.db")
    task = task_service.queue("hello", {"name": "world"})
    console = Console()
    console.follow(task)


if __name__ == "__main__":
    main()
