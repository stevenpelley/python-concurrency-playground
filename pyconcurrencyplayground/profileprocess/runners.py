import collections.abc
import logging
import queue
import subprocess
import threading

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
            record_event(EndEvent())
            self._sig_queue.put_nowait(-1)
            if sig_thread is not None:
                sig_thread.join()
            for p in popens:
                p.kill()
            for t in proc_threads:
                t.join()

    def send_signal(self, signal_num: int) -> None:
        self._sig_queue.put_nowait(signal_num)


type RunnerFactory = collections.abc.Callable[[], Runner]
