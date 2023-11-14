import json
import time
from datetime import datetime, timedelta
from typing import Generator

from sqlalchemy import Column, Enum, Integer, String, DateTime, ForeignKey, create_engine
from sqlalchemy.orm import sessionmaker, relationship, declarative_base

from tasks.framework import Task, TaskFrame, TaskFrameType, TaskStatus, TaskService

Base = declarative_base()


class DbTask(Base):
    __tablename__ = 'tasks'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    parameters = Column(String)
    scheduled_at = Column(DateTime)

    def parameters_write(self, parameters: dict[str, any]):
        self.parameters = json.dumps(parameters)

    def parameters_read(self):
        return json.loads(self.parameters)


class DbTaskFrame(Base):
    __tablename__ = 'task_frames'

    id = Column(Integer, primary_key=True)
    task_id = Column(Integer, ForeignKey('tasks.id'))
    type = Column(Enum(TaskFrameType))
    data = Column(String)
    time = Column(DateTime)
    task = relationship('DbTask', backref='frames')

    def data_write(self, data: any):
        if self.type == TaskFrameType.DATA:
            self.data = json.dumps(data)
        elif self.type == TaskFrameType.PROGRESSION:
            self.data = json.dumps(data)
        elif self.type == TaskFrameType.STATUS and isinstance(data, TaskStatus):
            self.data = data.name
        else:
            self.data = data

    def data_read(self) -> any:
        if self.type == TaskFrameType.DATA:
            return json.loads(self.data)
        elif self.type == TaskFrameType.PROGRESSION:
            return json.loads(self.data)
        elif self.type == TaskFrameType.STATUS:
            return TaskStatus[self.data]
        return self.data


class SqliteTaskService(TaskService):
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def frame_append(self, task: Task, frame: TaskFrame):
        with self.Session() as session:
            db_frame = DbTaskFrame(task_id=task.id, type=frame.type, time=frame.time)
            db_frame.data_write(frame.data)
            session.add(db_frame)
            session.commit()

    def frames(self, task: Task, frame_type: TaskFrameType = None) -> list[TaskFrame]:
        with self.Session() as session:
            db_frames = session.query(DbTaskFrame) \
                .filter_by(task_id=task.id) \
                .order_by(DbTaskFrame.time.asc())
            if frame_type is not None:
                db_frames = db_frames.filter_by(type=frame_type)
            return [TaskFrame(db_frame.type, db_frame.data_read(), db_frame.time) for db_frame in db_frames]

    def frames_follow(self, task: Task, resume_from_frame_id: int = -1, poll_interval: float = 0.05) -> Generator[TaskFrame, None, None]:
        finished = False
        while not finished:
            frames = []
            with self.Session() as session:
                db_frames = session.query(DbTaskFrame) \
                    .filter_by(task_id=task.id) \
                    .filter(DbTaskFrame.id > resume_from_frame_id) \
                    .order_by(DbTaskFrame.id.asc())
                for db_frame in db_frames:
                    resume_from_frame_id = db_frame.id
                    frame = TaskFrame(db_frame.type, None, db_frame.time)
                    frame.data = db_frame.data_read()
                    if db_frame.type == TaskFrameType.STATUS and (frame.data == TaskStatus.COMPLETED or frame.data == TaskStatus.FAILED):
                        finished = True
                    frames.append(frame)
            for frame in frames:
                yield frame
            if not finished:
                time.sleep(poll_interval)

    def task_create(self, name: str, parameters: dict[str, any]) -> Task:
        with self.Session() as session:
            scheduled_at = datetime.now()
            db_task = DbTask(name=name, scheduled_at=scheduled_at)
            db_task.parameters_write(parameters)
            session.add(db_task)
            session.commit()
            return Task(db_task.id, name, parameters, self)

    def task_schedule(self, task: Task, delay: timedelta):
        with self.Session() as session:
            db_task = session.query(DbTask).filter_by(id=task.id).first()
            db_task.scheduled_at = datetime.now() + delay
            session.commit()

    def task_unschedule(self, task: Task):
        with self.Session() as session:
            db_task = session.query(DbTask).filter_by(id=task.id).first()
            db_task.scheduled_at = None
            session.commit()

    def task_next(self, allowed_names: list[str]) -> Task | None:
        with self.Session() as session:
            db_task = session.query(DbTask) \
                .filter(DbTask.scheduled_at <= datetime.now()) \
                .filter(DbTask.name.in_(allowed_names)) \
                .order_by(DbTask.scheduled_at.asc()) \
                .first()
            if db_task is None:
                return None
            return Task(db_task.id, db_task.name, db_task.parameters_read(), self)
