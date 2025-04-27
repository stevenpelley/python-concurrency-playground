"""
NOTES:

I am examining patterns for concurrency when waiting for multiple actions to
finish.
In this example we are building a test/profiling driver intended to be run as
pid 1 of a container.  The driver will:
1. start a "process under test", the first process
2. start any number of "supporting processes" that will observe and profile
   execution (e.g., perf, java mission control)
3. wait and monitor execution.
  i.   if this driver receives SIGTERM or any process ends then SIGTERM all
       supporting processes.
  ii.  once all supporting processes have exited then SIGTERM the process under
       test if it hasn't yet exited.
  iii. Once all processes have exited then exit this process.

Originally I approached this as a run loop waiting on the the next future but
this suggests that it should be 3 separate waits that resemble exactly this
logic.

There is no timeout.  It is expected that the caller (container) will have a
timeout after SIGTERM, after which it will SIGKILL all processes or pid 1 to
kill all processes.

This is boiling down to a problem of "figuring out what to do next based on
which future completes and its value." Here are some options and some thoughts:

This can be "managed as a side effect." That is, put the follow-up actions in
each future task/thread, possibly synchronizing state with locks.  Logic is
inside of each task, not in the code that joins futures.  This is what we're
trying to avoid but sometimes this is going to be the most intuitive.

Otherwise we need to figure out which future completed and what to do about it.
Here are some choices:
1. match on the future by comparing via equality or an "in" statement.  E.g.,
   if completed_future == process_1_future.  Requires that you store the
   futures in an appropriate structure to test this.  Also requires that you
   then unpack the future's value or exception.
2. structural matching on the result of the future.  Requires that the future's
   return value include an indicator of which task it is (e.g., the process
   futures providing the process ordinal).  Unclear if a future can be
   structurally unpacked to get at a value vs an exception.
3. structural matching on some label data that is linked to the future and then
   zipped back together once we know which future has completed.  Does not
   require that the future task return something naming the task.  The value
   and exception can be unwrapped and prepared for matching by the joiner.
   Does require some wrapping code.



Opinions after working on structural matching and providing all the classes:
the single most helpful thing you can do here is to pass groups of futures --
the signal, process under test, and all other supporting processes.
The logic here needs to wait on:
the first of _all_ of these
all supporting processes
process under test

By having the futures pre-grouped it's easy to wait on each condition, or to
explicitly test the status of all futures in each group (or the single future
in a group)

Structural matching is needed when you need the complete context of a task upon
its completion but the next bit of work couldn't be chained to the future
(e.g., because it would require complex synchronization that you are trying
to avoid)

Structure around futures is also helpful for logging, especially when the
context about this logging isn't available from within the future task itself.

TODO:
raw thread version (no futures): coordinate in-tasks via locking
async with signals and processes managed by blocking
real async
"""

import abc
import collections.abc
import concurrent.futures
import dataclasses
import functools
import os
import queue
import signal
import subprocess
import sys
import threading
import typing

import pyconcurrencyplayground.futuretypes


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
        print(f"terminating process. ordinal={self.ordinal}. pid={self.popen.pid}")
        self.popen.terminate()

    def __str__(self) -> str:
        return f"(ProcessTaskData: ordinal={self.ordinal}. pid={self.popen.pid}"


type SignalFutureAndTaskData = pyconcurrencyplayground.futuretypes.FutureAndTaskData[
    int, SignalTaskData
]
type ProcessFutureAndTaskData = pyconcurrencyplayground.futuretypes.FutureAndTaskData[
    tuple[bytes, bytes], ProcessTaskData
]


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


def start_popens(
    popen_args: list[list[str]],
) -> list[subprocess.Popen[bytes]]:
    procs: list[subprocess.Popen[bytes]] = []
    for args in popen_args:
        procs.append(subprocess.Popen(args))
    print(f"child pids: {[p.pid for p in procs]}")
    return procs


def handler(
    sig_num: int,
    _stack_frame: typing.Any,
    sig_callback: collections.abc.Callable[[int], None],
) -> None:
    sig_callback(sig_num)


def start_handler(sig_callback: collections.abc.Callable[[int], None]) -> None:
    signal.signal(signal.SIGTERM, functools.partial(handler, sig_callback=sig_callback))


class Once:
    _has_run: bool = False
    _fn: collections.abc.Callable[[], None]

    def __init__(self, fn: collections.abc.Callable[[], None]) -> None:
        self._fn = fn

    def run(self) -> None:
        if not self._has_run:
            self._fn()
            self._has_run = True


def wait_for_queue_signal(sig_queue: queue.Queue[int]) -> int:
    return sig_queue.get()


def print_this_pid() -> None:
    print(f"pid: {os.getpid()}")


class Runner(abc.ABC):
    @abc.abstractmethod
    def run(self, popen_args: list[list[str]]) -> None:
        pass

    @abc.abstractmethod
    def send_signal(self, signal_num: int) -> None:
        pass


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
    ) -> None:
        pass

    def run(self, popen_args: list[list[str]]) -> None:
        procs = start_popens(popen_args)
        ex = concurrent.futures.ThreadPoolExecutor()
        try:
            sig_future = ex.submit(lambda: wait_for_queue_signal(self._sig_queue))
            signal_fl = pyconcurrencyplayground.futuretypes.FutureAndTaskData(
                future=sig_future, task_data=SignalTaskData()
            )

            process_under_test_fl: ProcessFutureAndTaskData | None = None
            supporting_processes_fl: list[ProcessFutureAndTaskData] = []
            for i, p in enumerate(procs):
                future = ex.submit(p.communicate)
                fl = pyconcurrencyplayground.futuretypes.FutureAndTaskData(
                    future=future,
                    task_data=ProcessTaskData(ordinal=i, popen=p),
                )
                if i == 0:
                    process_under_test_fl = fl
                else:
                    supporting_processes_fl.append(fl)
            if process_under_test_fl is None:
                raise ValueError("not process under test")
            self.run_futures(signal_fl, process_under_test_fl, supporting_processes_fl)
        finally:
            # make sure that all threads complete or this process will never exit
            self._sig_queue.put_nowait(-1)
            for p in procs:
                p.kill()

    def send_signal(self, signal_num: int) -> None:
        self._sig_queue.put_nowait(signal_num)


class FuturesRunnerFn(typing.Protocol):
    def __call__(
        self,
        sig: SignalFutureAndTaskData,
        process_under_test: ProcessFutureAndTaskData,
        supporting_processes: list[ProcessFutureAndTaskData],
    ) -> None: ...


def futures_runner_to_runner(run_fn: FuturesRunnerFn) -> Runner:
    class TheRunner(FuturesRunner):
        def run_futures(
            self,
            sig: SignalFutureAndTaskData,
            process_under_test: ProcessFutureAndTaskData,
            supporting_processes: list[ProcessFutureAndTaskData],
        ) -> None:
            run_fn(sig, process_under_test, supporting_processes)

    return TheRunner()


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


def get_runner() -> Runner:
    runners: dict[str, collections.abc.Callable[[], Runner]] = {
        "multiple-waits": lambda: futures_runner_to_runner(multiple_waits_runner),
        "one-loop": lambda: futures_runner_to_runner(one_loop_runner),
        "structureless": lambda: futures_runner_to_runner(structureless_runner),
        "raw-thread-runner": RawThreadRunner,
    }
    if len(sys.argv) < 2:
        sys.stderr.write("too few arguments.  Runner name required\n")
        sys.exit(1)
    name = sys.argv[1]
    if name not in runners:
        sys.stderr.write(f"unknown runner: [{name}]\n")
        sys.exit(1)
    return runners[name]()


def main() -> None:
    runner = get_runner()
    start_handler(runner.send_signal)
    print_this_pid()
    process_args = [["sleep", "999"], ["sleep", "999"]]
    runner.run(process_args)
