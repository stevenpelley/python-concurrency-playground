import asyncio
import collections.abc
import logging
import typing
from asyncio.subprocess import Process

import pyconcurrencyplayground.futuretypes
from pyconcurrencyplayground.profileprocess.asyncrunner import (
    AsyncProcessesStartedFn,
    AsyncRecordEvent,
    AsyncRunnerDefinition,
)
from pyconcurrencyplayground.profileprocess.event import (
    EndEvent,
    ProcessCreatedEvent,
    ProcessExitedEvent,
    ProcessTerminatedEvent,
    SignalEvent,
)
from pyconcurrencyplayground.profileprocess.futuresrunner import (
    ProcessReturn,
    ProcessTaskData,
    SignalTaskData,
)
from pyconcurrencyplayground.utils import log_extra

logger = logging.getLogger(__name__)


type AsyncSignalTaskAndTaskData = (
    pyconcurrencyplayground.futuretypes.AsyncTaskAndTaskData[int, SignalTaskData]
)

type AsyncProcessTaskData = ProcessTaskData[Process]
type AsyncProcessTaskAndTaskData = (
    pyconcurrencyplayground.futuretypes.AsyncTaskAndTaskData[
        ProcessReturn, AsyncProcessTaskData
    ]
)
type AsyncTaskAndTaskDatas = pyconcurrencyplayground.futuretypes.AsyncTaskAndTaskData[
    typing.Any, typing.Any
]


class MyCancelError(Exception):
    """
    Used to cancel a TaskGroup
    """


class SimpleAsyncRunner(AsyncRunnerDefinition):
    _signal_queue: asyncio.queues.Queue[int]

    def __init__(self) -> None:
        self._signal_queue = asyncio.queues.Queue()

    @staticmethod
    async def _start_processes(
        popen_args: list[list[str]],
        record_event: AsyncRecordEvent,
    ) -> list[Process]:
        processes: list[Process] = []
        for i, popen_arg in enumerate(popen_args):
            process: Process = await asyncio.create_subprocess_exec(
                popen_arg[0], *popen_arg[1:]
            )
            await record_event(ProcessCreatedEvent(pid=process.pid, ordinal=i))
            processes.append(process)

        logger.info(
            "started processes",
            extra=log_extra(pids=[p.pid for p in processes]),
        )
        return processes

    async def _wait_for_signal(self, record_event: AsyncRecordEvent) -> int:
        sig_num = await self._signal_queue.get()
        await record_event(SignalEvent(sig_num))
        return sig_num

    async def _wait_for_process(
        self, task_data: AsyncProcessTaskData, record_event: AsyncRecordEvent
    ) -> tuple[bytes, bytes]:
        """Wait for the process, terminating and then killing on
        CancelledError."""

        process = task_data.process
        ordinal = task_data.ordinal
        cancel_count = 0
        while True:
            try:
                (stdout, stderr) = await process.communicate()
                break
            except asyncio.CancelledError:
                cancel_count += 1
                if cancel_count == 1:
                    process.terminate()
                elif cancel_count == 2:
                    process.kill()
                else:
                    process.kill()
                    raise

        returncode: int | None = process.returncode
        await record_event(
            ProcessExitedEvent(
                pid=process.pid,
                ordinal=ordinal,
                exit_code=-999 if returncode is None else returncode,
                stdout=stdout,
                stderr=stderr,
            )
        )
        return (stdout, stderr)

    def _signal_task(
        self, tg: asyncio.TaskGroup, record_event: AsyncRecordEvent
    ) -> AsyncSignalTaskAndTaskData:
        signal_task: asyncio.Task[int] = tg.create_task(
            self._wait_for_signal(record_event)
        )
        return pyconcurrencyplayground.futuretypes.AsyncTaskAndTaskData(
            signal_task, SignalTaskData()
        )

    def _process_task(
        self,
        tg: asyncio.TaskGroup,
        record_event: AsyncRecordEvent,
        process: Process,
        ordinal: int,
    ) -> AsyncProcessTaskAndTaskData:
        aptd: AsyncProcessTaskData = ProcessTaskData(ordinal=ordinal, process=process)
        task: asyncio.Task[ProcessReturn] = tg.create_task(
            self._wait_for_process(aptd, record_event)
        )
        return pyconcurrencyplayground.futuretypes.AsyncTaskAndTaskData(task, aptd)

    async def _cancel_processes(
        self,
        processes_and_data: list[AsyncProcessTaskAndTaskData],
        record_event: AsyncRecordEvent,
    ) -> None:
        for future_and_task_data in processes_and_data:
            await self._cancel_process(future_and_task_data, record_event)

    async def _cancel_process(
        self,
        process_and_data: AsyncProcessTaskAndTaskData,
        record_event: AsyncRecordEvent,
    ) -> None:
        await record_event(
            ProcessTerminatedEvent(
                process_and_data.task_data.process.pid,
                process_and_data.task_data.ordinal,
            )
        )
        process_and_data.get_futurish().cancel()

    async def _run(
        self,
        tg: asyncio.TaskGroup,
        popen_args: list[list[str]],
        record_event: AsyncRecordEvent,
        on_processes_started: AsyncProcessesStartedFn,
    ) -> None:
        processes = await self._start_processes(popen_args, record_event)

        await on_processes_started()

        signal_future_and_data: AsyncSignalTaskAndTaskData = self._signal_task(
            tg, record_event
        )
        process_futures_and_data: list[AsyncProcessTaskAndTaskData] = [
            self._process_task(tg, record_event, p, i)
            for (i, p) in enumerate(processes)
        ]
        process_under_test = process_futures_and_data[0]
        supporting_processes = process_futures_and_data[1:]

        all_tasks: list[AsyncTaskAndTaskDatas] = []
        all_tasks.append(signal_future_and_data)
        all_tasks.extend(process_futures_and_data)
        # if initialized from a list the type checker still takes its type as
        # list, not Sequence.  list is invariant and so passing to
        # async_wait_and_zip gives an error.
        not_done: collections.abc.Sequence[AsyncTaskAndTaskDatas] = all_tasks
        done: collections.abc.Sequence[AsyncTaskAndTaskDatas]

        (done, _) = await pyconcurrencyplayground.futuretypes.async_wait_and_zip(
            not_done, return_when=asyncio.FIRST_COMPLETED
        )
        # do something with "done" just for fun
        logger.info("first completed", extra=log_extra(futures_and_task_data=done))

        await self._cancel_processes(supporting_processes, record_event)
        await pyconcurrencyplayground.futuretypes.async_wait_and_zip(
            supporting_processes, return_when=asyncio.ALL_COMPLETED
        )

        await self._cancel_process(process_under_test, record_event)
        try:
            await process_futures_and_data[0].get_futurish()
        except asyncio.CancelledError:
            pass

        await record_event(EndEvent())

    async def run(
        self,
        popen_args: list[list[str]],
        record_event: AsyncRecordEvent,
        on_processes_started: AsyncProcessesStartedFn,
    ) -> None:
        try:
            async with asyncio.TaskGroup() as tg:
                await self._run(tg, popen_args, record_event, on_processes_started)

                # cancels anything remaining in the TaskGroup.  If any TaskGroup
                # Task ends in another exception that hasn't been noticed yet it
                # will be added to the TaskGroup's ExceptionGroup and that other
                # exception will not match the except* below.  It will be reraised
                raise MyCancelError()
        except* MyCancelError:
            pass

    async def send_signal(self, signal_num: int) -> None:
        await self._signal_queue.put(signal_num)
