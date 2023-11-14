from datetime import datetime

from tasks.console import Console, TaskLine, RunLine, FrameLine
from tasks.framework import TaskFramework, TaskFrameType, TaskStatus
from tasks.sqlite import SqliteTaskService


def main():
    task_service = SqliteTaskService("sqlite:///tasks.db")
    framework = TaskFramework(task_service)
    task = framework.queue("hello", {"name": "world"})

    console = Console()
    console.print_line(TaskLine(datetime.now(), task.id, task.name, datetime.now()))
    run = None
    runs = 0
    for frame in framework.follow(task):
        if frame.type == TaskFrameType.STATUS:
            if run is None or frame.data == TaskStatus.RUN_SCHEDULED:
                runs += 1
                run = RunLine(frame.time, runs, frame.data)
                console.print_line(run)
            else:
                run.task_status = frame.data
        else:
            console.print_line(FrameLine(frame.time, frame.type, frame.data))


if __name__ == "__main__":
    main()
