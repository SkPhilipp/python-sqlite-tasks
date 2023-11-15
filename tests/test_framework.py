from tasks.framework import Task, TaskRegistry, TaskStatus, TaskFrame, TaskFrameType, TaskService


class StubTaskService(TaskService):
    """
    Stub implementation of parts of TaskService intended for testing only.
    """

    def __init__(self):
        self._frames = []

    def frame_append(self, task: Task, frame: TaskFrame):
        self._frames.append(frame)

    def frames(self, task: Task, frame_type: TaskFrameType = None) -> list[TaskFrame]:
        return [frame for frame in self._frames if frame.type == frame_type]

    def queue(self, name: str, parameters: dict[str, any]) -> Task:
        return Task(_id=1, name=name, parameters=parameters, task_service=self)


def test__registry__run():
    service = StubTaskService()
    registry = TaskRegistry()

    @registry.task_handler()
    def long_task(option: str, task: Task):
        task.data(f"option={option}")

    demo_task = service.queue(name=long_task.__name__, parameters={"option": "a"})
    registry.run(demo_task)
    assert service._frames == [
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUN_ACTIVE),
        TaskFrame(TaskFrameType.DATA, "option=a"),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.TASK_COMPLETED)
    ]


def test__registry__run_failed():
    service = StubTaskService()
    registry = TaskRegistry()

    @registry.task_handler()
    def long_task(option: str, task: Task):
        task.data(f"option={option}")
        raise Exception("Something went wrong")

    demo_task = service.queue(name=long_task.__name__, parameters={"option": "a"})
    registry.run(demo_task)
    assert service._frames == [
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUN_ACTIVE),
        TaskFrame(TaskFrameType.DATA, "option=a"),
        TaskFrame(TaskFrameType.LOG_ERROR, "Something went wrong"),
        TaskFrame(TaskFrameType.LOG_ERROR, "Failed 1 runs, rescheduling"),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUN_FAILED),
        TaskFrame(TaskFrameType.STATUS, TaskStatus.RUN_SCHEDULED),
    ]
