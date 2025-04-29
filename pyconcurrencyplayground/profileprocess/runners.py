import collections.abc
import concurrent.futures
import functools
import logging
import queue
import subprocess
import threading
import typing

import pyconcurrencyplayground.futuretypes
from pyconcurrencyplayground.profileprocess._base import (
    ProcessesStartedFn,
    ProcessFutureAndTaskData,
    RecordFuturesRunnerEvent,
    Runner,
    SignalFutureAndTaskData,
    futures_runner_to_runner,
)
from pyconcurrencyplayground.profileprocess.event import (
    ProcessCreatedEvent,
    ProcessExitedEvent,
    ProcessTerminatedEvent,
    RecordEvent,
    SignalEvent,
)
from pyconcurrencyplayground.utils import Once, log_extra

logger = logging.getLogger(__name__)


class RawThreadRunner(Runner):
    """Runner that does not use futures or executors and instead runs tasks
    directly on constructed threads"""

    _sig_queue: queue.Queue[int]

    def __init__(self):
        self._sig_queue = queue.Queue()

    class SharedState:
        """
        State protected by the lock
        """

        _record_event: RecordEvent

        _lock: threading.Lock
        _popens: list[subprocess.Popen[bytes]]
        _supporting_processes_terminated = False
        _num_supporting_processes_finished: int = 0

        def __init__(
            self, popens: list[subprocess.Popen[bytes]], record_event: RecordEvent
        ):
            self._record_event = record_event
            self._lock = threading.Lock()
            self._popens = popens

        def _terminate_supporting_processes(self) -> None:
            for i, p in enumerate(self._popens[1:]):
                ordinal = i + 1
                logger.info(
                    "terminating supporting process",
                    extra=log_extra(pid=p.pid, ordinal=ordinal),
                )
                self._record_event(ProcessTerminatedEvent(pid=p.pid, ordinal=ordinal))
                p.terminate()

        def _terminate_process_under_test(self) -> None:
            if self._popens:
                logger.info(
                    "terminating process under test",
                    extra=log_extra(pid=self._popens[0].pid),
                )
                self._record_event(
                    ProcessTerminatedEvent(pid=self._popens[0].pid, ordinal=0)
                )
                self._popens[0].terminate()

        def _any_finisher(self) -> None:
            if not self._supporting_processes_terminated:
                self._supporting_processes_terminated = True
                self._terminate_supporting_processes()

        def received_signal(self, signal_num: int) -> None:
            with self._lock:
                self._record_event(SignalEvent(signal_num=signal_num))
                self._any_finisher()

        def process_under_test_finished(
            self,
            popen: subprocess.Popen[bytes],
            stdout: bytes,
            stderr: bytes,
        ) -> None:
            with self._lock:
                self._record_event(
                    ProcessExitedEvent(
                        pid=popen.pid,
                        ordinal=0,
                        exit_code=popen.returncode,
                        stdout=stdout,
                        stderr=stderr,
                    )
                )
                self._any_finisher()

        def supporting_process_finished(
            self,
            popen: subprocess.Popen[bytes],
            ordinal: int,
            stdout: bytes,
            stderr: bytes,
        ) -> None:
            with self._lock:
                self._record_event(
                    ProcessExitedEvent(
                        pid=popen.pid,
                        ordinal=ordinal,
                        exit_code=popen.returncode,
                        stdout=stdout,
                        stderr=stderr,
                    )
                )
                self._any_finisher()
                self._num_supporting_processes_finished += 1
                if self._num_supporting_processes_finished == len(self._popens) - 1:
                    self._terminate_process_under_test()

    def run(
        self,
        popen_args: list[list[str]],
        record_event: RecordEvent,
        on_processes_started: ProcessesStartedFn,
    ) -> None:
        popens: list[subprocess.Popen[bytes]] = []
        sig_thread: threading.Thread | None = None
        proc_threads: list[threading.Thread] = []

        try:
            for i, args in enumerate(popen_args):
                popen = subprocess.Popen(args)
                record_event(ProcessCreatedEvent(pid=popen.pid, ordinal=i))
                popens.append(popen)
            shared_state = RawThreadRunner.SharedState(popens, record_event)
            on_processes_started()

            def sig_thread_fn() -> None:
                try:
                    sig = self._sig_queue.get()
                    shared_state.received_signal(sig)
                    logger.info("received signal", extra=log_extra(signal_num=sig))
                except Exception as ex:
                    logger.error(
                        "exception in RawThreadRunner sig_thread_fn", exc_info=ex
                    )
                    raise

            sig_thread = threading.Thread(target=sig_thread_fn)
            sig_thread.start()

            def proc_thread_fn(ordinal: int, popen: subprocess.Popen[bytes]) -> None:
                try:
                    (stdout, stderr) = popen.communicate()
                    if ordinal == 0:
                        shared_state.process_under_test_finished(popen, stdout, stderr)
                    else:
                        shared_state.supporting_process_finished(
                            popen, ordinal, stdout, stderr
                        )
                    logger.info(
                        "process exited",
                        extra=log_extra(ordinal=ordinal, pid=popen.pid),
                    )
                except Exception as ex:
                    logger.error(
                        "exception in RawThreadRunner proc_thread_fn", exc_info=ex
                    )
                    raise

            proc_threads = [
                threading.Thread(target=proc_thread_fn, args=(ordinal, popen))
                for ordinal, popen in enumerate(popens)
            ]

            for t in proc_threads:
                t.start()
            for t in proc_threads:
                t.join()

        finally:
            # make sure that all threads complete or this process will never exit
            self._sig_queue.put_nowait(-1)
            if sig_thread is not None:
                sig_thread.join()
            for p in popens:
                p.kill()
            for t in proc_threads:
                t.join()

    def send_signal(self, signal_num: int) -> None:
        self._sig_queue.put_nowait(signal_num)


def term_supporting_processes(
    supporting_processes: list[ProcessFutureAndTaskData],
    record_event: RecordFuturesRunnerEvent,
) -> None:
    logger.info("terminating supporting processes")
    for ftd in supporting_processes:
        pid = ftd.task_data.popen.pid
        ordinal = ftd.task_data.ordinal
        logger.info(
            "terminating supporting process", extra=log_extra(pid=pid, ordinal=ordinal)
        )
        record_event(ProcessTerminatedEvent(pid=pid, ordinal=ordinal))
        ftd.task_data.terminate()
    logger.info("terminated supporting processes")


def term_process_under_test(
    process_under_test: ProcessFutureAndTaskData,
    record_event: RecordFuturesRunnerEvent,
) -> None:
    pid = process_under_test.task_data.popen.pid
    ordinal = 0
    logger.info(
        "terminating process under test", extra=log_extra(pid=pid, ordinal=ordinal)
    )
    record_event(
        ProcessTerminatedEvent(
            pid=process_under_test.task_data.popen.pid, ordinal=ordinal
        )
    )
    process_under_test.task_data.terminate()


def one_loop_runner(
    sig: SignalFutureAndTaskData,
    process_under_test: ProcessFutureAndTaskData,
    supporting_processes: list[ProcessFutureAndTaskData],
    record_event: RecordFuturesRunnerEvent,
) -> None:
    terminate_supporting_processes_once = Once(
        functools.partial(term_supporting_processes, supporting_processes, record_event)
    )

    terminate_process_under_test_once = Once(
        functools.partial(term_process_under_test, process_under_test, record_event)
    )

    futures_and_labels: list[
        pyconcurrencyplayground.futuretypes.FutureAndTaskData[typing.Any, typing.Any]
    ] = [
        sig,
        process_under_test,
        *supporting_processes,
    ]

    def supporting_processes_exited() -> bool:
        return all(ftd.future.done() for ftd in supporting_processes)

    def process_under_test_exited() -> bool:
        return process_under_test.future.done()

    while not supporting_processes_exited() or not process_under_test_exited():
        (_, futures_and_labels) = pyconcurrencyplayground.futuretypes.wait_and_zip(
            futures_and_labels, return_when=concurrent.futures.FIRST_COMPLETED
        )

        # on anything completing terminate supporting processes.
        terminate_supporting_processes_once.run()

        if supporting_processes_exited():
            terminate_process_under_test_once.run()


def multiple_waits_runner(
    sig: SignalFutureAndTaskData,
    process_under_test: ProcessFutureAndTaskData,
    supporting_processes: list[ProcessFutureAndTaskData],
    record_event: RecordFuturesRunnerEvent,
) -> None:
    futures_and_labels: list[
        pyconcurrencyplayground.futuretypes.FutureAndTaskData[typing.Any, typing.Any]
    ] = [
        sig,
        process_under_test,
        *supporting_processes,
    ]
    pyconcurrencyplayground.futuretypes.wait_and_zip(
        futures_and_labels, return_when=concurrent.futures.FIRST_COMPLETED
    )

    term_supporting_processes(supporting_processes, record_event)

    pyconcurrencyplayground.futuretypes.wait_and_zip(
        supporting_processes, return_when=concurrent.futures.ALL_COMPLETED
    )
    logger.info("supporting processes exited")

    term_process_under_test(process_under_test, record_event)

    pyconcurrencyplayground.futuretypes.wait_and_zip(
        [process_under_test], return_when=concurrent.futures.ALL_COMPLETED
    )


def structureless_runner(
    sig: SignalFutureAndTaskData,
    process_under_test: ProcessFutureAndTaskData,
    supporting_processes: list[ProcessFutureAndTaskData],
    record_event: RecordFuturesRunnerEvent,
) -> None:
    def print_completed_future(f: concurrent.futures.Future[typing.Any]) -> None:
        if not f.done():
            raise ValueError("cannot print value of incomplete future")

        val: typing.Any
        if f.cancelled():
            val = "CANCELLED"
        elif f.exception() is not None:
            val = f.exception()
        else:
            val = f.result()

        if f == sig.future:
            logger.info("received signal", extra=log_extra(signal_num=val))
        elif f == process_under_test.future:
            logger.info("process under test exited", extra=log_extra(val=val))
        else:
            ordinal: int
            for i, sp in enumerate(supporting_processes):
                if sp.future == f:
                    ordinal = i + 1
                    break
            else:
                raise ValueError("completed future not found")
            logger.info(
                "supporting process exited", extra=log_extra(ordinal=ordinal, val=val)
            )

    not_done: set[concurrent.futures.Future[typing.Any]] = {
        sig.future,
        process_under_test.future,
        *(sp.future for sp in supporting_processes),
    }
    (done, not_done) = concurrent.futures.wait(
        not_done, return_when=concurrent.futures.FIRST_COMPLETED
    )
    for f in done:
        print_completed_future(f)

    term_supporting_processes(supporting_processes, record_event)

    remaining_supporting_processes = [
        sp.future for sp in supporting_processes if sp.future not in done
    ]
    (done, _) = concurrent.futures.wait(
        remaining_supporting_processes, return_when=concurrent.futures.ALL_COMPLETED
    )
    for f in done:
        print_completed_future(f)

    term_process_under_test(process_under_test, record_event)

    concurrent.futures.wait(
        [process_under_test.future], return_when=concurrent.futures.ALL_COMPLETED
    )
    print_completed_future(process_under_test.future)


def chaining_futures_runner(
    sig: SignalFutureAndTaskData,
    process_under_test: ProcessFutureAndTaskData,
    supporting_processes: list[ProcessFutureAndTaskData],
    record_event: RecordFuturesRunnerEvent,
) -> None:
    first_done: concurrent.futures.Future[
        pyconcurrencyplayground.futuretypes.DoneAndNotDoneFutures[typing.Any]
    ] = pyconcurrencyplayground.futuretypes.future_wait(
        [
            sig.future,
            process_under_test.future,
            *[ftd.future for ftd in supporting_processes],
        ],
        concurrent.futures.FIRST_COMPLETED,
    )

    supporting_procs_termed = pyconcurrencyplayground.futuretypes.future_then(
        first_done,
        lambda _: term_supporting_processes(supporting_processes, record_event),
    )

    supporting_procs_done: concurrent.futures.Future[
        pyconcurrencyplayground.futuretypes.DoneAndNotDoneFutures[typing.Any]
    ] = pyconcurrencyplayground.futuretypes.future_wait(
        [ftd.future for ftd in supporting_processes],
        concurrent.futures.ALL_COMPLETED,
    )

    process_under_test_termed = pyconcurrencyplayground.futuretypes.future_then(
        supporting_procs_done,
        lambda _: term_process_under_test(process_under_test, record_event),
    )

    combined_future: concurrent.futures.Future[
        pyconcurrencyplayground.futuretypes.DoneAndNotDoneFutures[typing.Any]
    ] = pyconcurrencyplayground.futuretypes.future_wait(
        [
            process_under_test.future,
            supporting_procs_termed,
            process_under_test_termed,
        ],
        concurrent.futures.ALL_COMPLETED,
    )
    combined_future.result()


type RunnerFactory = collections.abc.Callable[[], Runner]

RUNNERS: dict[str, RunnerFactory] = {
    "multiple-waits": lambda: futures_runner_to_runner(multiple_waits_runner),
    "one-loop": lambda: futures_runner_to_runner(one_loop_runner),
    "structureless": lambda: futures_runner_to_runner(structureless_runner),
    "raw-thread-runner": RawThreadRunner,
}
