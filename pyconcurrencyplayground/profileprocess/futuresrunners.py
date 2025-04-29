import concurrent.futures
import functools
import logging
import typing

import pyconcurrencyplayground.futuretypes
from pyconcurrencyplayground.profileprocess.event import (
    ProcessTerminatedEvent,
)
from pyconcurrencyplayground.profileprocess.futuresrunner import (
    ProcessFutureAndTaskData,
    RecordFuturesRunnerEvent,
    SignalFutureAndTaskData,
)
from pyconcurrencyplayground.utils import Once, log_extra

logger = logging.getLogger(__name__)


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
