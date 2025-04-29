import itertools
import os
import queue
import signal
import threading
import typing

import pytest

from pyconcurrencyplayground.profileprocess import Runner, RunnerFactory
from pyconcurrencyplayground.profileprocess.event import (
    Event,
    ProcessCreatedEvent,
    ProcessExitedEvent,
    ProcessTerminatedEvent,
    SignalEvent,
)
from pyconcurrencyplayground.profileprocess.runners import (
    RUNNERS,
)

type Trigger = typing.Literal[
    "SIGNAL", "TERM_SUPPORTING_PROCESS", "TERM_PROCESS_UNDER_TEST"
]


def _run(
    runner: Runner,
    trigger: Trigger,
) -> list[Event]:
    event_queue: queue.Queue[Event] = queue.Queue()

    lock = threading.Lock()
    ordinals_and_pids: list[tuple[int, int]] = []

    def record_event(event: Event) -> None:
        if isinstance(event, ProcessCreatedEvent):
            with lock:
                ordinals_and_pids.append((event.ordinal, event.pid))
        event_queue.put(event)

    def on_processes_started() -> None:
        ordinals_and_pids.sort()
        assert len(ordinals_and_pids) >= 2, (
            f"fewer than 2 processes. (ordinal, pid): {ordinals_and_pids}"
        )
        for i, (ordinal, _) in enumerate(ordinals_and_pids):
            assert ordinal == i, (
                f"unexpected started process ordinal. expected {i} "
                f"found {ordinal}. all: {ordinals_and_pids}"
            )
        match trigger:
            case "SIGNAL":
                runner.send_signal(signal.SIGTERM)
            case "TERM_SUPPORTING_PROCESS":
                os.kill(ordinals_and_pids[1][1], signal.SIGTERM)
            case "TERM_PROCESS_UNDER_TEST":
                os.kill(ordinals_and_pids[0][1], signal.SIGTERM)

    popen_args = [["sleep", "999"], ["sleep", "999"]]

    runner.run(
        popen_args,
        record_event,
        on_processes_started,
    )

    event_list: list[Event] = []
    try:
        while True:
            event_list.append(event_queue.get_nowait())
    except queue.Empty:
        pass
    return event_list


def _read_process_created_events_at_beginning(events: list[Event]) -> int:
    """
    Read until some event is not a ProcessCreatedEvent.
    Assert that observed process ordinals are 0 through the max with no gaps or
    duplicates.
    Return the number of process created.
    """
    created_process_ordinals: set[int] = set()
    for i, event in enumerate(events):
        match event:
            case ProcessCreatedEvent(ordinal=ordinal):
                assert ordinal not in created_process_ordinals, (
                    f"duplicate ProcessCreatedEvent ordinal: {ordinal}. "
                    f"event index: {i}. events: {events}"
                )
                created_process_ordinals.add(ordinal)
            case _:
                break
    for i in range(len(created_process_ordinals)):
        assert i in created_process_ordinals, (
            f"missing ProcessCreatedEvent ordinal: {i}"
            ". Suggests event ordinals are not contiguous. "
            f"created ordinals: {created_process_ordinals}. events: {events}"
        )
    return len(created_process_ordinals)


def _read_process_terminated_and_exited_events(
    events: list[Event],
    starting_idx: int,
    expected_ordinals: set[int],
    optionally_terminated_ordinal: int | None = None,
) -> int:
    """
    Read events starting at starting_idx until
    ProcessTerminatedEvent/ProcessExitedEvent for ordinal 0 or some event other
    than ProcessTerminatedEvent or ProcessExitedEvent.  Return the number of
    events observed.

    For these observed events assert that all expected ordinals are first
    terminated and then exit.  The order of terminations and exits is not
    defined, including that some processes may exit before others are
    terminated.

    the optionally_terminated_ordinal, if not None, may appear as a terminated
    process ordinal, but is not required to.  This is intended for cases where
    that process already exited and was the trigger for the remaining processes
    to be terminated.

    No other
    """
    terminated_ordinals: set[int] = set()
    exited_ordinals: set[int] = set()
    events_processed = 0

    for i, event in enumerate(events[starting_idx:]):
        idx = i + starting_idx
        match event:
            case (
                ProcessTerminatedEvent(ordinal=ordinal)
                | ProcessExitedEvent(ordinal=ordinal)
            ) if ordinal == 0:
                # we don't process ordinal 0, the process under test, here
                break
            case ProcessTerminatedEvent(ordinal=ordinal):
                assert ordinal not in terminated_ordinals, (
                    f"ordinal terminated twice: {ordinal}. idx: {idx}. events: {events}"
                )
                assert (
                    ordinal in expected_ordinals
                    or ordinal == optionally_terminated_ordinal
                ), (
                    f"unexpected ordinal terminated: {ordinal}. idx: {idx}"
                    f". events: {events}"
                )
                terminated_ordinals.add(ordinal)
            case ProcessExitedEvent(ordinal=ordinal):
                assert ordinal not in exited_ordinals, (
                    f"ordinal exited twice: {ordinal}. idx: {idx}. events: {events}"
                )
                assert ordinal in expected_ordinals, (
                    f"unexpected ordinal exited: {ordinal}. idx: {idx}"
                    f". events: {events}"
                )
                assert ordinal in terminated_ordinals, (
                    f"ordinal exited but not yet terminated: {ordinal}. idx: {idx}"
                    f". events: {events}"
                )
                exited_ordinals.add(ordinal)
            case _:
                break
        events_processed += 1

    # make sure we saw all the expected ordinals both terminated and exited
    if optionally_terminated_ordinal is not None:
        terminated_ordinals.discard(optionally_terminated_ordinal)
    assert expected_ordinals == terminated_ordinals
    assert expected_ordinals == exited_ordinals
    return events_processed


def _read_signal_event(events: list[Event], idx: int) -> None:
    assert len(events) > idx, (
        f"no more events, expecting signal. idx: {idx}. events: {events}"
    )
    event = events[idx]
    assert isinstance(event, SignalEvent), (
        f"expected signal event.  Found: {event}. idx: {idx}. events: {events}"
    )


def _read_process_under_test_event(
    events: list[Event],
    idx: int,
    event_kind: typing.Literal["TERMINATED", "EXITED"],
) -> None:
    label: str
    event_type: type[ProcessTerminatedEvent] | type[ProcessExitedEvent]
    if event_kind == "TERMINATED":
        label = "terminated"
        event_type = ProcessTerminatedEvent
    else:
        label = "exited"
        event_type = ProcessExitedEvent

    assert len(events) > idx, (
        f"no more events, expecting process under test {label}. idx: {idx}. "
        f"events: {events}"
    )
    event = events[idx]
    assert isinstance(event, event_type), (
        f"expected process under test {label}.  Found: {event}. idx: {idx}. "
        f"events: {events}"
    )
    # convince the type checker that event must have attribute ordinal
    assert isinstance(event, (ProcessTerminatedEvent, ProcessExitedEvent))
    ordinal = event.ordinal
    assert ordinal == 0, (
        f"expected process under test {label}. Incorrect ordinal. "
        f"ordinal: {ordinal}. idx: {idx}. events: {events}"
    )


def _assert_signal_event_order(events: list[Event]) -> None:
    # processes created.  Must be contiguous.  Record number/max ordinal
    num_processes = _read_process_created_events_at_beginning(events)
    next_idx = num_processes

    # signal arrives
    _read_signal_event(events, next_idx)
    next_idx += 1

    # supporting processes are terminated and exit.  Possible that some exit
    # before others are terminated. For each ordinal must observe that it is
    # terminated before it exits.
    terminated_and_exited_events = _read_process_terminated_and_exited_events(
        events, next_idx, set(range(1, num_processes))
    )
    next_idx += terminated_and_exited_events

    # process under test is terminated
    _read_process_under_test_event(events, next_idx, "TERMINATED")
    next_idx += 1

    # process under test exits
    _read_process_under_test_event(events, next_idx, "EXITED")
    next_idx += 1

    # must be no more events
    assert next_idx == len(events), (
        f"Extraneous events.  idx: {next_idx}. events: {events}"
    )


def _assert_process_under_test_termed_event_order(events: list[Event]) -> None:
    # processes created.  Must be contiguous.  Record number/max ordinal

    # process under test exits

    # supporting processes are terminated and exit.  Possible that some exit
    # before others are terminated

    # process under test optionally terminated (no harm)
    pass


def _assert_supporting_process_termed_event_order(events: list[Event]) -> None:
    # processes created.  Must be contiguous.  Record number/max ordinal

    # some supporting process exits

    # supporting processes are terminated and exit.  Possible that some exit
    # before others are terminated.  The process that previously exited need not
    # be terminated.

    # process under test is terminated

    # process under test exits
    pass


tests: list[tuple[tuple[str, RunnerFactory], Trigger]] = list(
    itertools.product(
        sorted(RUNNERS.items()),
        # ["SIGNAL", "TERM_SUPPORTING_PROCESS", "TERM_PROCESS_UNDER_TEST"],
        ["SIGNAL"],
    )
)


test_ids: list[str] = [
    f"{runner_name}-{trigger}" for ((runner_name, _runner_factory), trigger) in tests
]
test_args: list[tuple[RunnerFactory, Trigger]] = [
    (runner_factory, trigger) for ((_runner_name, runner_factory), trigger) in tests
]


@pytest.mark.parametrize("runner_factory,trigger", test_args, ids=test_ids)
def test_runners(runner_factory: RunnerFactory, trigger: Trigger) -> None:
    runner = runner_factory()
    events = _run(runner, trigger)
    _assert_signal_event_order(events)
