from tasks.console import Console
from tasks.sqlite import SqliteTaskService


def main():
    task_service = SqliteTaskService("sqlite:///tasks.db")
    console = Console()
    for i in range(100):
        task = task_service.queue("hello", {"name": "world"})
        console.follow(task)


if __name__ == "__main__":
    main()
