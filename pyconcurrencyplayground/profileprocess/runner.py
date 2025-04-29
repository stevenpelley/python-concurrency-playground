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
