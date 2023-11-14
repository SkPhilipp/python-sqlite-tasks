from tasks.framework import TaskFramework, Task
from tasks.sqlite import SqliteTaskService

if __name__ == "__main__":
    task_service = SqliteTaskService("sqlite:///tasks.db")
    framework = TaskFramework(task_service)


    @framework.handler()
    def hello(task: Task, name: str):
        task.log_info(f"Hello, {name}! I'm going for a nap..")
        task.log_info("I'm back, here's some data:")
        task.data({"foo": "bar"})
        task.data({"123": [1, 2, 3]})
        task.progression(task.runs())
        raise Exception("Oh no! See you next time!")


    framework.main()
