from abc import ABC
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from time import sleep
from typing import Callable, Generator, TypeVar, Generic


class TaskFrameType(Enum):
    DATA = 0
    PROGRESSION = 1
    STATUS = 2
    LOG_INFO = 3
    LOG_ERROR = 4


T = TypeVar("T")


@dataclass
class TaskFrame(Generic[T]):
    type: TaskFrameType
    data: T
    time: datetime = field(default_factory=datetime.now)

    def __eq__(self, other):
        return self.type == other.type and self.data == other.data


class TaskService(ABC):
    """
    Service for interacting with task frames and scheduling tasks.
    """

    def frame_append(self, task: 'Task', frame: TaskFrame):
        """
        Append a frame to a task.

        :param task:
        :param frame:
        :return:
        """
        pass

    def frames(self, task: 'Task', frame_type: TaskFrameType = None) -> list[TaskFrame]:
        """
        Get all currently known frames of a certain type for a task.

        :param task:
        :param frame_type:
        :return:
        """
        pass

    def frames_follow(self, task: 'Task', resume_from_frame_id: int = -1, poll_interval: float = 0.05) -> Generator[TaskFrame, None, None]:
        """
        Generator that yields frames as they are appended to the task.

        :param task:
        :param resume_from_frame_id:
        :param poll_interval:
        :return:
        """
        pass

    def task_create(self, name: str, parameters: dict[str, any]) -> 'Task':
        """
        Create a new task for use with this service.

        :param name:
        :param parameters:
        :return:
        """
        pass

    def task_schedule(self, task: 'Task', delay: timedelta):
        """
        Schedule a task to be run after a certain delay.

        :param task:
        :param delay:
        :return:
        """
        pass

    def task_unschedule(self, task: 'Task'):
        """
        Remove a task from the schedule.

        :param task:
        :return:
        """

    def task_next(self, allowed_names: list[str]) -> 'Task | None':
        """
        Get the next task to be run.

        :param allowed_names:
        :return:
        """
        pass


class TaskStatus(Enum):
    RUN_SCHEDULED = 0
    RUN_ACTIVE = 1
    RUN_FAILED = 2
    TASK_COMPLETED = 3
    TASK_FAILED = 4


class Task:
    def __init__(self, _id: int, name: str, parameters: dict[str, any], task_service: TaskService):
        self.id = _id
        self.name = name
        self.parameters = parameters
        self._task_service = task_service

    def data(self, data: any):
        self._task_service.frame_append(self, TaskFrame(type=TaskFrameType.DATA, data=data))

    def progression(self, current: any):
        self._task_service.frame_append(self, TaskFrame(type=TaskFrameType.PROGRESSION, data=current))

    def log_info(self, message: str):
        self._task_service.frame_append(self, TaskFrame(type=TaskFrameType.LOG_INFO, data=message))

    def log_error(self, message: str):
        self._task_service.frame_append(self, TaskFrame(type=TaskFrameType.LOG_ERROR, data=message))

    def run_scheduled(self, delay: timedelta):
        self._task_service.task_schedule(self, delay)
        self._task_service.frame_append(self, TaskFrame(type=TaskFrameType.STATUS, data=TaskStatus.RUN_SCHEDULED))

    def run(self):
        self._task_service.task_unschedule(self)
        self._task_service.frame_append(self, TaskFrame(type=TaskFrameType.STATUS, data=TaskStatus.RUN_ACTIVE))

    def run_fail(self):
        self._task_service.frame_append(self, TaskFrame(type=TaskFrameType.STATUS, data=TaskStatus.RUN_FAILED))

    def task_complete(self):
        self._task_service.frame_append(self, TaskFrame(type=TaskFrameType.STATUS, data=TaskStatus.TASK_COMPLETED))

    def task_fail(self):
        self._task_service.frame_append(self, TaskFrame(type=TaskFrameType.STATUS, data=TaskStatus.TASK_FAILED))

    def runs(self):
        status_changes = self._task_service.frames(self, TaskFrameType.STATUS)
        return len([status_change for status_change in status_changes if status_change.data == TaskStatus.RUN_ACTIVE])

    def __eq__(self, other):
        return self.id == other.id

    def __str__(self):
        return f"Task(id={self.id}, name={self.name})"

    def __repr__(self):
        return self.__str__()


class TaskRegistry:
    def __init__(self):
        self.handlers: dict[str, Callable] = {}
        self.handlers_inverse: dict[Callable, str] = {}
        self.run_limit = 4
        self.run_reschedule_delay = 0

    def task_handler(self, name: str = None):
        """
        Decorator which registers a task by name.
        :param name: Name of the task, defaults to the name of the function
        :return:
        """

        def wrapper(func: Callable):
            effective_name = name or func.__name__
            self.handlers[effective_name] = func
            self.handlers_inverse[func] = effective_name
            return func

        return wrapper

    def run(self, task: Task):
        """
        Run a task by forwarding it to the appropriate handler.
        :param task:
        :return:
        """
        try:
            handler = self.handlers.get(task.name)
            if handler is None:
                raise ValueError(f"Task {task} has no known handler")
            task.run()
            effective_parameters = {}
            effective_parameters.update(task.parameters)
            effective_parameters["task"] = task
            handler(**effective_parameters)
            task.task_complete()
        except Exception as e:
            task.log_error(str(e))
            runs = task.runs()
            if runs >= self.run_limit:
                task.log_error(f"Failed {runs} runs, exceeded run limit of {self.run_limit}")
                task.run_fail()
                task.task_fail()
            else:
                task.log_error(f"Failed {runs} runs, rescheduling")
                task.run_fail()
                task.run_scheduled(timedelta(seconds=self.run_reschedule_delay))


class TaskFramework:
    """
    Main entry point for interacting with the task framework.
    """

    def __init__(self, task_service: TaskService):
        self._task_service = task_service
        self._task_registry = TaskRegistry()

    def handler(self, name: str = None):
        """
        Decorator which registers a task by name.
        :param name: Name of the task, defaults to the name of the function
        :return:
        """
        return self._task_registry.task_handler(name)

    def run(self, task: Task):
        """
        Run a task by forwarding it to the appropriate handler.
        :param task:
        :return:
        """
        self._task_registry.run(task)

    def queue(self, name: str, parameters: dict[str, any]) -> Task:
        """
        Create a new task and schedule it for execution.
        :param name:
        :param parameters:
        :return:
        """
        task = self._task_service.task_create(name, parameters)
        return task

    def follow(self, task: Task) -> Generator[TaskFrame, None, None]:
        """
        Generator that yields frames as they are appended to the task.
        :param task:
        :return:
        """
        return self._task_service.frames_follow(task)

    def main(self):
        """
        Run the main loop, processing tasks as they become available.
        :return:
        """
        names = list(self._task_registry.handlers.keys())
        print(f"Handling tasks: {names}")
        while True:
            task = self._task_service.task_next(names)
            if task is not None:
                self._task_registry.run(task)
            else:
                sleep(0.05)
