from tasks.framework import TaskFramework
from tasks.sqlite import SqliteTaskService

if __name__ == "__main__":
    task_service = SqliteTaskService("sqlite:///tasks.db")
    framework = TaskFramework(task_service)
    task = framework.queue("hello", {"name": "world"})
    for frame in framework.follow(task):
        print(frame)
