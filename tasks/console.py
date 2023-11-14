from abc import ABC, abstractmethod
from datetime import datetime

from blessings import Terminal

from tasks.framework import TaskFrameType, TaskStatus


class Line(ABC):

    @abstractmethod
    def draw(self, terminal: Terminal):
        pass

    def draw_limited(self, terminal: Terminal):
        return self.draw(terminal)[:terminal.width]


class TaskLine(Line):
    def __init__(self, time: datetime, task_id: int, task_name: str, time_start: datetime):
        self.time = time
        self.task_id = task_id
        self.task_name = task_name
        self.time_start = time_start

    def draw(self, terminal: Terminal):
        time_elapsed = (datetime.now() - self.time).total_seconds()
        return f"[+] following task {self.task_name} #{self.task_id} ({time_elapsed:.1f}s)"


class RunLine(Line):
    def __init__(self, time: datetime, run: int, task_status: TaskStatus):
        self.time = time
        self.run = run
        self.task_status = task_status

    def draw(self, terminal: Terminal):
        return f" => [+] run {self.run} -- {self.task_status.name}"


class FrameLine(Line):
    def __init__(self, time: datetime, frame_type: TaskFrameType, frame_data: any):
        self.time = time
        self.frame_type = frame_type
        self.frame_data = frame_data

    def draw(self, terminal: Terminal):
        time_formatted = self.time.strftime("%H:%M:%S:%f")[:-3]
        return f" => => {time_formatted} {self.frame_type.name} {self.frame_data}"


class Console:
    """
    Utility for displaying tasks in the console.
    """

    def __init__(self):
        self.terminal = Terminal()
        self.lines = []

    def _unpin_first(self, line_type) -> int:
        for i, line in enumerate(self.lines):
            if isinstance(line, line_type):
                self.lines.pop(i)
                return i

    def _unpin_one(self) -> int:
        """
        Unpin the first line deemed to be the least important.
        :return: The index of the line unpinned.
        """
        if len([line for line in self.lines if isinstance(line, TaskLine)]) > 4:
            return self._unpin_first(TaskLine)
        if len([line for line in self.lines if isinstance(line, RunLine)]) > 4:
            return self._unpin_first(RunLine)
        return self._unpin_first(FrameLine)

    def _redraw_from(self, line_offset: int):
        """
        Draw all lines from the specified index onwards.
        :param line_offset: The index in lines to start drawing from.
        """
        for i, line in enumerate(self.lines[line_offset:]):
            with self.terminal.location(0, self.terminal.height - len(self.lines) - 1 + i + line_offset):
                line_text = line.draw_limited(self.terminal)
                print(line_text + " " * (self.terminal.width - len(line_text)))

    def print_line(self, line: Line):
        self.lines.append(line)
        if len(self.lines) >= self.terminal.height:
            self._unpin_one()
        else:
            print()
        self._redraw_from(0)
