import collections.abc
import functools
import os
import signal
import typing

import pyconcurrencyplayground.profileprocess


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


def main() -> None:
    runner = pyconcurrencyplayground.profileprocess.get_runner()
    start_handler(runner.send_signal)
    print_this_pid()
    process_args = [["sleep", "999"], ["sleep", "999"]]
    runner.run(process_args)


if __name__ == "__main__":
    main()
