import collections.abc
import functools
import os
import signal
import sys
import typing

import pyconcurrencyplayground.profileprocess
import pyconcurrencyplayground.profileprocess.runners
from pyconcurrencyplayground.profileprocess.event import (
    Event,
    ProcessCreatedEvent,
    ProcessExitedEvent,
    ProcessTerminatedEvent,
    SignalEvent,
)


def print_this_pid() -> None:
    print(f"pid: {os.getpid()}")


def handler(
    sig_num: int,
    _stack_frame: typing.Any,
    sig_callback: collections.abc.Callable[[int], None],
) -> None:
    sig_callback(sig_num)


def start_handler(sig_callback: collections.abc.Callable[[int], None]) -> None:
    signal.signal(signal.SIGTERM, functools.partial(handler, sig_callback=sig_callback))


def print_event(event: Event) -> None:
    match event:
        case SignalEvent(signal_num):
            print(f"signal received: signal_num={signal_num}")
        case ProcessCreatedEvent(pid=pid, ordinal=ordinal):
            print(f"process created: pid={pid}, ordinal={ordinal}")
        case ProcessTerminatedEvent(pid=pid, ordinal=ordinal):
            print(f"process terminated: pid={pid}, ordinal={ordinal}")
        case ProcessExitedEvent(pid=pid, ordinal=ordinal, exit_code=exit_code):
            # not bothering to print stdout and stderr for now
            print(
                f"process terminated: pid={pid}, "
                f"ordinal={ordinal}, exit_code={exit_code}"
            )


def get_runner() -> pyconcurrencyplayground.profileprocess.Runner:
    if len(sys.argv) < 2:
        sys.stderr.write("too few arguments.  Runner name required\n")
        sys.exit(1)
    name = sys.argv[1]
    runner_factory: (
        pyconcurrencyplayground.profileprocess.runners.RunnerFactory | None
    ) = pyconcurrencyplayground.profileprocess.runners.RUNNERS.get(name)

    if runner_factory is None:
        sys.stderr.write(f"unknown runner: [{name}]\n")
        sys.exit(1)
    return runner_factory()


def main() -> None:
    runner = get_runner()
    start_handler(runner.send_signal)
    print_this_pid()
    process_args = [["sleep", "999"], ["sleep", "999"]]
    runner.run(process_args, print_event, lambda: None)


if __name__ == "__main__":
    main()
