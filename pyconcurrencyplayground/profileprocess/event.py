"""
Defines the events expected to occur throughout profiling a process.  These are
called/annotated in each runner and can be used to log/output event occurences
or to later test that an execution was valid.
"""

import dataclasses
import typing


@dataclasses.dataclass(frozen=True)
class ProcessEvent:
    pid: int
    ordinal: int


@dataclasses.dataclass(frozen=True)
class ProcessCreatedEvent(ProcessEvent): ...


@dataclasses.dataclass(frozen=True)
class ProcessTerminatedEvent(ProcessEvent): ...


@dataclasses.dataclass(frozen=True)
class ProcessExitedEvent(ProcessEvent):
    exit_code: int
    stdout: bytes
    stderr: bytes


@dataclasses.dataclass(frozen=True)
class SignalEvent:
    signal_num: int


class EndEvent: ...


type Event = (
    ProcessCreatedEvent
    | ProcessTerminatedEvent
    | ProcessExitedEvent
    | SignalEvent
    | EndEvent
)


class RecordEvent(typing.Protocol):
    def __call__(self, event: Event) -> None:
        """
        Call prior to performing the actual action.  Otherwise if the action may
        enable other actions/events to proceed there is a race and the order of
        recording may not match the order of events.
        """
