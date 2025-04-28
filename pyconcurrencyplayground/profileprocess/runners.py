import collections.abc
import concurrent.futures
import queue
import subprocess
import threading
import typing

import pyconcurrencyplayground.futuretypes
from pyconcurrencyplayground.profileprocess._base import (
    ProcessFutureAndTaskData,
    Runner,
    SignalFutureAndTaskData,
    futures_runner_to_runner,
)


class RawThreadRunner(Runner):
    """Runner that does not use futures or executors and instead runs tasks
    directly on constructed threads"""

    _sig_queue: queue.Queue[int]

    def __init__(self):
        self._sig_queue = queue.Queue()

    def run(self, popen_args: list[list[str]]) -> None:
        popens: list[subprocess.Popen[bytes]] = []
        sig_thread: threading.Thread | None = None
        proc_threads: list[threading.Thread] = []

        try:

            class SharedState:
                _lock: threading.Lock
                _popens: list[subprocess.Popen[bytes]]
                _supporting_processes_terminated = False
                _num_supporting_processes_finished: int = 0

                def __init__(self, popens: list[subprocess.Popen[bytes]]):
                    self._lock = threading.Lock()
                    self._popens = popens

                def _terminate_supporting_processes(self) -> None:
                    for p in self._popens[1:]:
                        p.terminate()

                def _terminate_process_under_test(self) -> None:
                    if self._popens:
                        self._popens[0].terminate()

                def _any_finisher(self) -> None:
                    if not self._supporting_processes_terminated:
                        self._supporting_processes_terminated = True
                        self._terminate_supporting_processes()

                def received_signal(self):
                    with self._lock:
                        self._any_finisher()

                def process_under_test_finished(self):
                    with self._lock:
                        self._any_finisher()

                def supporting_process_finished(self):
                    with self._lock:
                        self._any_finisher()
                        self._num_supporting_processes_finished += 1
                        if (
                            self._num_supporting_processes_finished
                            == len(self._popens) - 1
                        ):
                            self._terminate_process_under_test()

            popens = [subprocess.Popen(args) for args in popen_args]
            shared_state = SharedState(popens)

            def sig_thread_fn() -> None:
                sig = self._sig_queue.get()
                shared_state.received_signal()
                print(f"received signal: {sig}")

            sig_thread = threading.Thread(target=sig_thread_fn)

            def proc_thread_fn(ordinal: int, popen: subprocess.Popen[bytes]) -> None:
                (_stdout, _stderr) = popen.communicate()
                if ordinal == 0:
                    shared_state.process_under_test_finished()
                else:
                    shared_state.supporting_process_finished()
                print(f"process exited. ordinal: {ordinal}. pid: {popen.pid}")

            proc_threads = [
                threading.Thread(target=proc_thread_fn, args=(ordinal, popen))
                for ordinal, popen in enumerate(popens)
            ]

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


class Once:
    _has_run: bool = False
    _fn: collections.abc.Callable[[], None]

    def __init__(self, fn: collections.abc.Callable[[], None]) -> None:
        self._fn = fn

    def run(self) -> None:
        if not self._has_run:
            self._fn()
            self._has_run = True


def one_loop_runner(
    sig: SignalFutureAndTaskData,
    process_under_test: ProcessFutureAndTaskData,
    supporting_processes: list[ProcessFutureAndTaskData],
) -> None:
    def term_supporting_processes():
        print("terminating supporting processes")
        for fl in supporting_processes:
            fl.task_data.terminate()

    terminate_supporting_processes_once = Once(term_supporting_processes)

    def term_process_under_test():
        print("terminating process under test")
        process_under_test.task_data.terminate()

    terminate_process_under_test_once = Once(term_process_under_test)

    futures_and_labels: list[
        pyconcurrencyplayground.futuretypes.FutureAndTaskData[typing.Any, typing.Any]
    ] = [
        sig,
        process_under_test,
        *supporting_processes,
    ]

    def supporting_processes_exited():
        return all(fl.future.done() for fl in supporting_processes)

    def process_under_test_exited():
        return process_under_test.future.done()

    while not supporting_processes_exited() and not process_under_test_exited():
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

    print("terminating supporting processes")
    for fl in supporting_processes:
        fl.task_data.terminate()

    pyconcurrencyplayground.futuretypes.wait_and_zip(
        supporting_processes, return_when=concurrent.futures.ALL_COMPLETED
    )
    print("supporting processes exited")

    print("terminating process under test")
    process_under_test.task_data.terminate()

    pyconcurrencyplayground.futuretypes.wait_and_zip(
        [process_under_test], return_when=concurrent.futures.ALL_COMPLETED
    )


def structureless_runner(
    sig: SignalFutureAndTaskData,
    process_under_test: ProcessFutureAndTaskData,
    supporting_processes: list[ProcessFutureAndTaskData],
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
            print(f"received signal: {val}")
        elif f == process_under_test.future:
            print(f"process under test exited: {val}")
        else:
            ordinal: int
            for i, sp in enumerate(supporting_processes):
                if sp.future == f:
                    ordinal = i + 1
                    break
            else:
                raise ValueError("completed future not found")
            print(f"supporting process exited: ordinal={ordinal}: {val}")

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

    print("terminating supporting processes")
    for sp in supporting_processes:
        sp.task_data.terminate()

    remaining_supporting_processes = [
        sp.future for sp in supporting_processes if sp.future not in done
    ]
    (done, _) = concurrent.futures.wait(
        remaining_supporting_processes, return_when=concurrent.futures.ALL_COMPLETED
    )
    for f in done:
        print_completed_future(f)

    print("terminating process under test")
    process_under_test.task_data.terminate()

    concurrent.futures.wait(
        [process_under_test.future], return_when=concurrent.futures.ALL_COMPLETED
    )
    print_completed_future(process_under_test.future)


def chaining_futures_runner(
    sig: SignalFutureAndTaskData,
    process_under_test: ProcessFutureAndTaskData,
    supporting_processes: list[ProcessFutureAndTaskData],
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

    def term_supporting_procs():
        for sp in supporting_processes:
            sp.task_data.terminate()

    supporting_procs_termed = pyconcurrencyplayground.futuretypes.future_then(
        first_done,
        lambda _: term_supporting_procs,
    )

    supporting_procs_done: concurrent.futures.Future[
        pyconcurrencyplayground.futuretypes.DoneAndNotDoneFutures[typing.Any]
    ] = pyconcurrencyplayground.futuretypes.future_wait(
        [ftd.future for ftd in supporting_processes],
        concurrent.futures.ALL_COMPLETED,
    )

    process_under_test_termed = pyconcurrencyplayground.futuretypes.future_then(
        supporting_procs_done,
        lambda _: process_under_test.task_data.terminate(),
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
