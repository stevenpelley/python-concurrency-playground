import abc
import logging
import typing

from pyconcurrencyplayground.profileprocess.event import (
    RecordEvent,
)

logger = logging.getLogger(__name__)


class ProcessesStartedFn(typing.Protocol):
    def __call__(self) -> None: ...


class Runner(abc.ABC):
    @abc.abstractmethod
    def run(
        self,
        popen_args: list[list[str]],
        record_event: RecordEvent,
        on_processes_started: ProcessesStartedFn,
    ) -> None:
        pass

    @abc.abstractmethod
    def send_signal(self, signal_num: int) -> None:
        pass
