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

    def queue(self, name: str | Callable, parameters: dict[str, any], scheduled_at: datetime = None) -> 'Task':
        """
        Queue up a new task for retrieval by the scheduler.

        :param name:
        :param parameters:
        :param scheduled_at:
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
        self.id: int = _id
        self.name: str = name
        self.parameters: dict[str, any] = parameters
        self.task_service: TaskService = task_service

    def data(self, data: any):
        """
        Emits a data frame, generally the (partial) result of a task.

        :param data:
        :return:
        """
        self.task_service.frame_append(self, TaskFrame(type=TaskFrameType.DATA, data=data))

    def progression(self, current: any):
        """
        Emits a progression frame, generally used to later resume from a certain point when rescheduling a task.
        :param current:
        :return:
        """
        self.task_service.frame_append(self, TaskFrame(type=TaskFrameType.PROGRESSION, data=current))

    def log_info(self, message: str):
        """
        Emits an info-level log frame.

        :param message:
        :return:
        """
        self.task_service.frame_append(self, TaskFrame(type=TaskFrameType.LOG_INFO, data=message))

    def log_error(self, message: str):
        """
        Emits an error-level log frame.

        :param message:
        :return:
        """
        self.task_service.frame_append(self, TaskFrame(type=TaskFrameType.LOG_ERROR, data=message))

    def run_scheduled(self, delay: timedelta):
        """
        Reschedule a task to be run after a certain delay.

        :param delay:
        :return:
        """
        self.task_service.task_schedule(self, delay)
        self.task_service.frame_append(self, TaskFrame(type=TaskFrameType.STATUS, data=TaskStatus.RUN_SCHEDULED))

    def run(self):
        """
        Mark a task as currently running.

        :return:
        """
        self.task_service.task_unschedule(self)
        self.task_service.frame_append(self, TaskFrame(type=TaskFrameType.STATUS, data=TaskStatus.RUN_ACTIVE))

    def run_fail(self):
        """
        Mark a task run as failed, to be potentially rescheduled by the scheduler.

        :return:
        """
        self.task_service.frame_append(self, TaskFrame(type=TaskFrameType.STATUS, data=TaskStatus.RUN_FAILED))

    def task_complete(self):
        """
        Mark a task as completed.
        :return:
        """
        self.task_service.frame_append(self, TaskFrame(type=TaskFrameType.STATUS, data=TaskStatus.TASK_COMPLETED))

    def task_fail(self):
        """
        Mark a task as failed.

        :return:
        """
        self.task_service.frame_append(self, TaskFrame(type=TaskFrameType.STATUS, data=TaskStatus.TASK_FAILED))

    def runs(self):
        """
        Retrieve the number of times a `run` has been invoked on this task.

        :return:
        """
        status_changes = self.task_service.frames(self, TaskFrameType.STATUS)
        return len([status_change for status_change in status_changes if status_change.data == TaskStatus.RUN_ACTIVE])

    def queue(self, name: str | Callable, parameters: dict[str, any], scheduled_at: datetime = None) -> 'Task':
        """
        Queue up a new task for retrieval by the scheduler, and emits a log frame indicating it has been queued.

        :param name:
        :param parameters:
        :param scheduled_at:
        :return:
        """
        task = self.task_service.queue(name, parameters, scheduled_at)
        self.task_service.frame_append(self, TaskFrame(type=TaskFrameType.LOG_INFO, data=f"queued task {task.id} of type {name}"))
        return task

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

    def listen(self, task_service: TaskService):
        """
        Listen for tasks currently registered and run them as they become available.
        :param task_service:
        :return:
        """
        names = list(self.handlers.keys())
        while True:
            task = task_service.task_next(names)
            if task is not None:
                self.run(task)
            else:
                sleep(0.01)
