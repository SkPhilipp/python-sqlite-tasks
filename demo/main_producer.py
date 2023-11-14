from blessings import Terminal

from tasks.framework import TaskFramework, TaskFrameType
from tasks.sqlite import SqliteTaskService

terminal = Terminal()


def column_type(frame_type: TaskFrameType):
    if frame_type == TaskFrameType.STATUS:
        return "       "
    elif frame_type == TaskFrameType.DATA:
        return terminal.blue("DATA   ")
    elif frame_type == TaskFrameType.PROGRESSION:
        return terminal.blue("PROG   ")
    elif frame_type == TaskFrameType.LOG_INFO:
        return terminal.blue("INFO   ")
    elif frame_type == TaskFrameType.LOG_ERROR:
        return terminal.red("ERROR  ")
    else:
        return "?      "


if __name__ == "__main__":
    task_service = SqliteTaskService("sqlite:///tasks.db")
    framework = TaskFramework(task_service)
    task = framework.queue("hello", {"name": "world"})

    print("Monitoring: " + terminal.bold(f"{task.id}"))
    with terminal.fullscreen():
        for frame in framework.follow(task):
            print(f"{frame.time.isoformat()} | " + column_type(frame.type) + " | " + f"{frame.data}")
