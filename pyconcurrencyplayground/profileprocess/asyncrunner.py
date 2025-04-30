import abc
import asyncio
import functools
import logging
import types
import typing

from pyconcurrencyplayground.profileprocess.event import (
    Event,
    RecordEvent,
)
from pyconcurrencyplayground.profileprocess.runner import ProcessesStartedFn, Runner

logger = logging.getLogger(__name__)


class AsyncProcessesStartedFn(typing.Protocol):
    async def __call__(self) -> None: ...


class AsyncRecordEvent(typing.Protocol):
    async def __call__(self, event: Event) -> None:
        """
        Call prior to performing the actual action.  Otherwise if the action may
        enable other actions/events to proceed there is a race and the order of
        recording may not match the order of events.
        """


class AsyncRunnerDefinition(abc.ABC):
    async def run(
        self,
        popen_args: list[list[str]],
        record_event: AsyncRecordEvent,
        on_processes_started: AsyncProcessesStartedFn,
    ) -> None:
        pass

    async def send_signal(self, signal_num: int) -> None:
        pass


class AsyncRunner(Runner):
    """
    Run an AsyncRunnerDefinition in such a way that an outside thread could call
    its methods.
    """

    _runner_def: AsyncRunnerDefinition
    _asyncio_runner: asyncio.Runner

    def __init__(self, runner_def: AsyncRunnerDefinition) -> None:
        self._runner_def = runner_def
        self._asyncio_runner = asyncio.Runner()

    def __enter__(self) -> typing.Self:
        self._asyncio_runner.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        self._asyncio_runner.__exit__(exc_type, exc_val, exc_tb)

    async def _async_on_processes_started(
        self,
        on_processes_started: ProcessesStartedFn,
    ) -> None:
        logger.info("async on process started")
        await self._asyncio_runner.get_loop().run_in_executor(
            executor=None, func=on_processes_started
        )
        logger.info("async on process started returned")

    async def _async_record_event(
        self, event: Event, record_event: RecordEvent
    ) -> None:
        logger.info("async record_event")
        await self._asyncio_runner.get_loop().run_in_executor(
            executor=None, func=functools.partial(record_event, event)
        )
        logger.info("async record_event returned")

    def run(
        self,
        popen_args: list[list[str]],
        record_event: RecordEvent,
        on_processes_started: ProcessesStartedFn,
    ) -> None:
        logger.info("running async loop")
        self._asyncio_runner.run(
            self._runner_def.run(
                popen_args,
                functools.partial(self._async_record_event, record_event=record_event),
                functools.partial(
                    self._async_on_processes_started, on_processes_started
                ),
            )
        )
        logger.info("async loop returned")

    def send_signal(self, signal_num: int) -> None:
        logger.info("async send_signal")
        future = asyncio.run_coroutine_threadsafe(
            self._runner_def.send_signal(signal_num),
            loop=self._asyncio_runner.get_loop(),
        )
        future.result()
        logger.info("async send_signal returned")
