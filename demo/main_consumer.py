from tasks.framework import TaskFramework, Task
from tasks.sqlite import SqliteTaskService

if __name__ == "__main__":
    task_service = SqliteTaskService("sqlite:///tasks.db")
    framework = TaskFramework(task_service)


    @framework.handler()
    def hello(task: Task, name: str):
        task.log_info(f"Hello, {name}!")


    framework.main()
