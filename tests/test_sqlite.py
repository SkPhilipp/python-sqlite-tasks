from tasks.framework import Task, TaskRegistry, TaskStatus, TaskFrame, TaskFrameType
from tasks.sqlite import SqliteTaskService


def setup() -> (SqliteTaskService, TaskRegistry):
    service = SqliteTaskService("sqlite+pysqlite:///:memory:")
    registry = TaskRegistry()

    @registry.handler()
    def handler(option: str, task: Task):
        task.data(f"option={option}")

    @registry.handler()
    def handler_erring(task: Task):
        task.data(f"This is run {task.runs()}")
        raise Exception("Something went wrong")

    return service, registry


def test__service__create_next():
    service, registry = setup()
    task = service.queue(name="handler", parameters={"option": "a"})
    assert service.task_next(["handler"]) == task


def test__service__task_completed():
    service, registry = setup()
    task = service.queue(name="handler", parameters={"option": "a"})
    registry.run(task)
    assert service.frames(task) == [
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUN_ACTIVE),
        TaskFrame(TaskFrameType.DATA, "option=a"),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.TASK_COMPLETED)
    ]


def test__service__run_failed():
    service, registry = setup()
    task = service.queue(name="handler_erring", parameters={})
    registry.run(task)
    assert service.frames(task) == [
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUN_ACTIVE),
        TaskFrame(TaskFrameType.DATA, "This is run 1"),
        TaskFrame(TaskFrameType.LOG_ERROR, "Something went wrong"),
        TaskFrame(TaskFrameType.LOG_ERROR, "Failed 1 runs, rescheduling"),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUN_FAILED),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUN_SCHEDULED),
    ]


def test__service__task_failed():
    service, registry = setup()
    task = service.queue(name="handler_erring", parameters={})
    registry.run(task)
    registry.run(task)
    registry.run(task)
    registry.run(task)
    assert service.frames(task) == [
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUN_ACTIVE),
        TaskFrame(TaskFrameType.DATA, "This is run 1"),
        TaskFrame(TaskFrameType.LOG_ERROR, "Something went wrong"),
        TaskFrame(TaskFrameType.LOG_ERROR, "Failed 1 runs, rescheduling"),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUN_FAILED),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUN_SCHEDULED),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUN_ACTIVE),
        TaskFrame(TaskFrameType.DATA, "This is run 2"),
        TaskFrame(TaskFrameType.LOG_ERROR, "Something went wrong"),
        TaskFrame(TaskFrameType.LOG_ERROR, "Failed 2 runs, rescheduling"),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUN_FAILED),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUN_SCHEDULED),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUN_ACTIVE),
        TaskFrame(TaskFrameType.DATA, "This is run 3"),
        TaskFrame(TaskFrameType.LOG_ERROR, "Something went wrong"),
        TaskFrame(TaskFrameType.LOG_ERROR, "Failed 3 runs, rescheduling"),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUN_FAILED),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUN_SCHEDULED),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUN_ACTIVE),
        TaskFrame(TaskFrameType.DATA, "This is run 4"),
        TaskFrame(TaskFrameType.LOG_ERROR, "Something went wrong"),
        TaskFrame(TaskFrameType.LOG_ERROR, "Failed 4 runs, exceeded run limit of 4"),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUN_FAILED),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.TASK_FAILED)
    ]
