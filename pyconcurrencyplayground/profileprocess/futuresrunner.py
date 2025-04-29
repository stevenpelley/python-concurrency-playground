import abc
import concurrent.futures
import dataclasses
import functools
import logging
import queue
import subprocess
import typing

import pyconcurrencyplayground.futuretypes
from pyconcurrencyplayground.profileprocess.event import (
    EndEvent,
    ProcessCreatedEvent,
    ProcessExitedEvent,
    ProcessTerminatedEvent,
    RecordEvent,
    SignalEvent,
)
from pyconcurrencyplayground.profileprocess.runner import ProcessesStartedFn, Runner
from pyconcurrencyplayground.utils import log_extra

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class SignalTaskData:
    """No data, just a signal"""

    def __str__(self) -> str:
        return "(SignalTaskData)"


@dataclasses.dataclass(frozen=True)
class ProcessTaskData:
    ordinal: int
    popen: subprocess.Popen[bytes]

    def terminate(self) -> None:
        logger.info(
            "terminating process",
            extra=log_extra(ordinal=self.ordinal, pid=self.popen.pid),
        )
        self.popen.terminate()

    def __str__(self) -> str:
        return f"(ProcessTaskData: ordinal={self.ordinal}. pid={self.popen.pid}"

    def obj_json_default(self) -> typing.Any:
        """Overrides json serialization for logging"""
        return {"ordinal": self.ordinal, "popen": {"pid": self.popen.pid}}


type SignalFutureAndTaskData = pyconcurrencyplayground.futuretypes.FutureAndTaskData[
    int, SignalTaskData
]
type ProcessFutureAndTaskData = pyconcurrencyplayground.futuretypes.FutureAndTaskData[
    tuple[bytes, bytes], ProcessTaskData
]

# "|" with additional event types if needed
type FuturesRunnerEvent = ProcessTerminatedEvent


class RecordFuturesRunnerEvent(typing.Protocol):
    def __call__(self, event: FuturesRunnerEvent) -> None:
        """
        Limited to those events that the concrete FuturesRunner controls.

        Call prior to performing the actual action.  Otherwise if the action may
        enable other actions/events to proceed there is a race and the order of
        recording may not match the order of events.
        """


class FuturesRunner(Runner, abc.ABC):
    _sig_queue: queue.Queue[int]

    def __init__(self) -> None:
        self._sig_queue = queue.Queue()

    @abc.abstractmethod
    def run_futures(
        self,
        sig: SignalFutureAndTaskData,
        process_under_test: ProcessFutureAndTaskData,
        supporting_processes: list[ProcessFutureAndTaskData],
        record_event: RecordFuturesRunnerEvent,
    ) -> None:
        pass

    @staticmethod
    def _start_popens(
        popen_args: list[list[str]],
        record_event: RecordEvent,
    ) -> list[subprocess.Popen[bytes]]:
        procs: list[subprocess.Popen[bytes]] = []
        for i, args in enumerate(popen_args):
            popen = subprocess.Popen(args)
            record_event(ProcessCreatedEvent(pid=popen.pid, ordinal=i))
            procs.append(popen)

        logger.info(
            "started processes",
            extra=log_extra(pids=[p.pid for p in procs]),
        )
        return procs

    def _signal_task(
        self,
        record_event: RecordEvent,
    ) -> int:
        sig_num = self._sig_queue.get()
        record_event(SignalEvent(sig_num))
        return sig_num

    def _process_wait_task(
        self,
        task_data: ProcessTaskData,
        record_event: RecordEvent,
    ) -> tuple[bytes, bytes]:
        (stdout, stderr) = task_data.popen.communicate()
        record_event(
            ProcessExitedEvent(
                pid=task_data.popen.pid,
                ordinal=task_data.ordinal,
                exit_code=task_data.popen.returncode,
                stdout=stdout,
                stderr=stderr,
            )
        )
        return (stdout, stderr)

    def run(
        self,
        popen_args: list[list[str]],
        record_event: RecordEvent,
        on_processes_started: ProcessesStartedFn,
    ) -> None:
        procs = FuturesRunner._start_popens(popen_args, record_event)
        on_processes_started()
        ex = concurrent.futures.ThreadPoolExecutor()
        signal_ftd: (
            pyconcurrencyplayground.futuretypes.FutureAndTaskData[int, SignalTaskData]
            | None
        ) = None
        process_under_test_ftd: ProcessFutureAndTaskData | None = None
        supporting_processes_ftd: list[ProcessFutureAndTaskData] = []
        try:
            sig_future = ex.submit(
                functools.partial(self._signal_task, record_event=record_event)
            )
            signal_ftd = pyconcurrencyplayground.futuretypes.FutureAndTaskData(
                future=sig_future, task_data=SignalTaskData()
            )

            for i, p in enumerate(procs):
                task_data = ProcessTaskData(ordinal=i, popen=p)
                future = ex.submit(
                    functools.partial(
                        self._process_wait_task,
                        task_data=task_data,
                        record_event=record_event,
                    )
                )
                ftd = pyconcurrencyplayground.futuretypes.FutureAndTaskData(
                    future=future,
                    task_data=task_data,
                )
                if i == 0:
                    process_under_test_ftd = ftd
                else:
                    supporting_processes_ftd.append(ftd)
            if process_under_test_ftd is None:
                raise ValueError("not process under test")
            self.run_futures(
                signal_ftd,
                process_under_test_ftd,
                supporting_processes_ftd,
                record_event,
            )

            # verify that all process futures have completed and reraise any exception
            if not process_under_test_ftd.future.done():
                raise ValueError(
                    "process under test not done at end of FuturesRunner.run"
                )
            process_under_test_ftd.future.result()

            for ftd in supporting_processes_ftd:
                if not ftd.future.done():
                    raise ValueError(
                        "supporting process not done at end of FuturesRunner.run. "
                        f"ordinal={ftd.task_data.ordinal}"
                    )
                ftd.future.result()

        finally:
            record_event(EndEvent())
            # make sure that all threads complete or this process will never exit
            self._sig_queue.put_nowait(-1)
            if signal_ftd is not None:
                signal_ftd.future.result(timeout=0.1)
            for p in procs:
                p.kill()
            if process_under_test_ftd is not None:
                process_under_test_ftd.future.result(timeout=0.1)
            for ftd in supporting_processes_ftd:
                ftd.future.result(timeout=0.1)

    def send_signal(self, signal_num: int) -> None:
        self._sig_queue.put_nowait(signal_num)


class FuturesRunnerFn(typing.Protocol):
    def __call__(
        self,
        sig: SignalFutureAndTaskData,
        process_under_test: ProcessFutureAndTaskData,
        supporting_processes: list[ProcessFutureAndTaskData],
        record_event: RecordFuturesRunnerEvent,
    ) -> None: ...


def futures_runner_to_runner(run_fn: FuturesRunnerFn) -> Runner:
    class TheRunner(FuturesRunner):
        def run_futures(
            self,
            sig: SignalFutureAndTaskData,
            process_under_test: ProcessFutureAndTaskData,
            supporting_processes: list[ProcessFutureAndTaskData],
            record_event: RecordFuturesRunnerEvent,
        ) -> None:
            run_fn(sig, process_under_test, supporting_processes, record_event)

    return TheRunner()
