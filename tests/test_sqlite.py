from tasks.framework import Task, TaskRegistry, TaskStatus, TaskFrame, TaskFrameType
from tasks.sqlite import SqliteTaskService


def setup() -> (SqliteTaskService, TaskRegistry):
    service = SqliteTaskService("sqlite+pysqlite:///:memory:")
    registry = TaskRegistry()

    @registry.task_handler()
    def handler(option: str, task: Task):
        task.data(f"option={option}")

    @registry.task_handler()
    def handler_erring(task: Task):
        task.data(f"This is run {task.runs()}")
        raise Exception("Something went wrong")

    return service, registry


def test__service__create_next():
    service, registry = setup()
    task = service.task_create(name="handler", parameters={"option": "a"})
    assert service.task_next(["handler"]) == task


def test__service__run_completed():
    service, registry = setup()
    task = service.task_create(name="handler", parameters={"option": "a"})
    registry.run(task)
    assert service.frames(task) == [
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUNNING),
        TaskFrame(TaskFrameType.DATA, "option=a"),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.COMPLETED)
    ]


def test__service__run_failed():
    service, registry = setup()
    task = service.task_create(name="handler_erring", parameters={})
    registry.run(task)
    assert service.frames(task) == [
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUNNING),
        TaskFrame(TaskFrameType.DATA, "This is run 1"),
        TaskFrame(TaskFrameType.LOG_ERROR, "Something went wrong"),
        TaskFrame(TaskFrameType.LOG_ERROR, "Failed 1 runs, rescheduling"),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.SCHEDULED),
    ]


def test__service__run_rescheduled():
    service, registry = setup()
    task = service.task_create(name="handler_erring", parameters={})
    registry.run(task)
    registry.run(task)
    assert service.frames(task) == [
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUNNING),
        TaskFrame(TaskFrameType.DATA, "This is run 1"),
        TaskFrame(TaskFrameType.LOG_ERROR, "Something went wrong"),
        TaskFrame(TaskFrameType.LOG_ERROR, "Failed 1 runs, rescheduling"),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.SCHEDULED),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUNNING),
        TaskFrame(TaskFrameType.DATA, "This is run 2"),
        TaskFrame(TaskFrameType.LOG_ERROR, "Something went wrong"),
        TaskFrame(TaskFrameType.LOG_ERROR, "Failed 2 runs, rescheduling"),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.SCHEDULED),
    ]
