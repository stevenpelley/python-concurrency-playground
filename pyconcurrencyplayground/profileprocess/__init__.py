from pyconcurrencyplayground.profileprocess import futuresrunner, futuresrunners
from pyconcurrencyplayground.profileprocess.runners import (
    RawThreadRunner,
    RunnerFactory,
)

RUNNERS: dict[str, RunnerFactory] = {
    "multiple-waits": lambda: futuresrunner.futures_runner_to_runner(
        futuresrunners.multiple_waits_runner
    ),
    "one-loop": lambda: futuresrunner.futures_runner_to_runner(
        futuresrunners.one_loop_runner
    ),
    "structureless": lambda: futuresrunner.futures_runner_to_runner(
        futuresrunners.structureless_runner
    ),
    "raw-thread-runner": RawThreadRunner,
}
