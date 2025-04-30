import abc
import asyncio
import collections.abc
import concurrent.futures
import dataclasses
import functools
import logging
import threading
import typing

import pyconcurrencyplayground.utils
from pyconcurrencyplayground.utils import log_extra

logger = logging.getLogger(__name__)


class FutureOutcomes:
    @dataclasses.dataclass(frozen=True)
    class FutureResult[T]:
        result: T

        def __str__(self) -> str:
            return f"(FutureResult: {self.result})"

    @dataclasses.dataclass(frozen=True)
    class FutureException:
        exception: BaseException

        def __str__(self) -> str:
            return f"(FutureException: {self.exception})"

    @dataclasses.dataclass(frozen=True)
    class FutureCancelled:
        def __str__(self) -> str:
            return "(FutureCancelled)"


type FutureOutcome[FutureOutcomeT] = (
    FutureOutcomes.FutureResult[FutureOutcomeT]
    | FutureOutcomes.FutureException
    | FutureOutcomes.FutureCancelled
)


class Futurish[T](typing.Protocol):
    def done(self) -> bool: ...
    def cancelled(self) -> bool: ...
    def exception(self) -> BaseException | None: ...
    def result(self) -> T: ...


def futurish_to_outcome[T](
    future: Futurish[T],
) -> FutureOutcome[T]:
    if not future.done():
        raise ValueError("incomplete future cannot generate outcome")
    ex = future.exception()
    if future.cancelled():
        return FutureOutcomes.FutureCancelled()
    elif ex is not None:
        return FutureOutcomes.FutureException(exception=ex)
    else:
        return FutureOutcomes.FutureResult(future.result())


def futurish_json_default[T](f: Futurish[T]) -> typing.Any:
    is_done = f.done()
    outcome: FutureOutcome[T] | None = None
    if is_done:
        outcome = futurish_to_outcome(f)
    return {"is_done": is_done, "outcome": outcome}


@dataclasses.dataclass(frozen=True)
class BaseFutureAndTaskData[T, TaskDataT](abc.ABC):
    task_data: TaskDataT

    @abc.abstractmethod
    def get_futurish(self) -> Futurish[T]:
        pass

    def outcome(self) -> FutureOutcome[T]:
        return futurish_to_outcome(self.get_futurish())

    def obj_json_default(self) -> dict[str, typing.Any]:
        """override json serialization"""
        future_name = type(self.get_futurish()).__name__
        return {
            future_name: futurish_json_default(self.get_futurish()),
            "task_data": pyconcurrencyplayground.utils.json_default(self.task_data),
        }


@dataclasses.dataclass(frozen=True, init=False)
class FutureAndTaskData[
    T,
    TaskDataT,
](BaseFutureAndTaskData[T, TaskDataT]):
    future: concurrent.futures.Future[T]

    def __init__(
        self, future: concurrent.futures.Future[T], task_data: TaskDataT
    ) -> None:
        """Dataclasses inheriting from dataclass with type parameter do not
        properly recognize inherited fields in constructor"""
        object.__setattr__(self, "future", future)
        object.__setattr__(self, "task_data", task_data)

    def get_futurish(self) -> concurrent.futures.Future[T]:
        return self.future


@dataclasses.dataclass(frozen=True)
class AsyncTaskAndTaskData[
    T,
    TaskDataT,
](BaseFutureAndTaskData[T, TaskDataT]):
    task: asyncio.Task[T]

    def __init__(self, task: asyncio.Task[T], task_data: TaskDataT) -> None:
        """Dataclasses inheriting from dataclass with type parameter do not
        properly recognize inherited fields in constructor"""
        object.__setattr__(self, "task", task)
        object.__setattr__(self, "task_data", task_data)

    def get_futurish(self) -> asyncio.Task[T]:
        return self.task


def wait_and_zip[T, TaskDataT](
    future_data_pairs: collections.abc.Sequence[FutureAndTaskData[T, TaskDataT]],
    timeout: float | None = None,
    return_when: str = concurrent.futures.ALL_COMPLETED,
) -> tuple[
    collections.abc.Sequence[FutureAndTaskData[T, TaskDataT]],
    collections.abc.Sequence[FutureAndTaskData[T, TaskDataT]],
]:
    future_to_pair = {p.future: p for p in future_data_pairs}
    futures = [p.future for p in future_data_pairs]
    return _zip_and_log_wait_results(
        future_to_pair,
        concurrent.futures.wait(futures, timeout=timeout, return_when=return_when),
    )


async def async_wait_and_zip[T, TaskDataT](
    future_data_pairs: collections.abc.Sequence[AsyncTaskAndTaskData[T, TaskDataT]],
    timeout: float | None = None,
    return_when: str = concurrent.futures.ALL_COMPLETED,
) -> tuple[
    collections.abc.Sequence[AsyncTaskAndTaskData[T, TaskDataT]],
    collections.abc.Sequence[AsyncTaskAndTaskData[T, TaskDataT]],
]:
    future_to_pair = {p.task: p for p in future_data_pairs}
    futures = [p.task for p in future_data_pairs]

    return _zip_and_log_wait_results(
        future_to_pair,
        await asyncio.wait(futures, timeout=timeout, return_when=return_when),
    )


def _zip_and_log_wait_results[F, T](
    d: dict[F, T], done_and_not_done: tuple[set[F], set[F]]
) -> tuple[collections.abc.Sequence[T], collections.abc.Sequence[T]]:
    done_pairs = [d[f] for f in done_and_not_done[0]]
    not_done_pairs = [d[f] for f in done_and_not_done[1]]
    for p in done_pairs:
        logger.info(
            "future completed",
            extra=log_extra(future_and_data=p),
        )
    return done_pairs, not_done_pairs


def _chain_future[T, U](
    source_future: concurrent.futures.Future[T],
    destination_future: concurrent.futures.Future[U],
    fn: collections.abc.Callable[[concurrent.futures.Future[T]], U],
) -> None:
    if source_future.cancelled():
        destination_future.cancel()
    else:
        try:
            result: U = fn(source_future)
            destination_future.set_result(result)
        except BaseException as ex:  # pylint: disable=W0718:broad-exception-caught
            destination_future.set_exception(ex)


def future_then[T, U](
    future: concurrent.futures.Future[T],
    fn: collections.abc.Callable[[concurrent.futures.Future[T]], U],
    executor: concurrent.futures.Executor | None = None,
) -> concurrent.futures.Future[U]:
    """
    Simple, raw future chaining.  Chains function fn to run when future
    completes and returns a Future corresponding to the additional work.

    If future is cancelled the returned future will be cancelled.  If future
    completes otherwise exceptionally or normally it is provided to fn.

    If the returned future is cancelled the source future will _not_ be
    cancelled (follows Java CompletableFuture semantics).  Yes, cancelling is
    hard to coordinate, but the semantics get tricky if we allow cancelling
    upstream.

    If executor is not None then once future completes fn will run on a thread
    in the provided executor.
    """
    new_future = concurrent.futures.Future[U]()
    if executor is None:
        future.add_done_callback(
            functools.partial(_chain_future, destination_future=new_future, fn=fn)
        )
    else:

        def callback(f: concurrent.futures.Future[T]) -> None:
            if f.cancelled():
                new_future.cancel()
            else:
                executor.submit(
                    _chain_future, source_future=f, destination_future=new_future, fn=fn
                )

        future.add_done_callback(callback)

    return new_future


class DoneAndNotDoneFutures[T](typing.NamedTuple):
    done: set[concurrent.futures.Future[T]]
    not_done: set[concurrent.futures.Future[T]]


def _chain_many[T](
    futures: list[concurrent.futures.Future[T]],
    done_callback: collections.abc.Callable[[DoneAndNotDoneFutures[T]], bool],
) -> concurrent.futures.Future[DoneAndNotDoneFutures[T]]:
    """
    Chain the result of several futures together into a single future.

    done_callback takes the current done/not done tuple and returns true if the
    composed future should be completed.
    """
    all_futures_set = set(futures)
    new_future = concurrent.futures.Future[DoneAndNotDoneFutures[T]]()

    lock: threading.Lock = threading.Lock()
    done: set[concurrent.futures.Future[T]] = set()

    def _callback(future: concurrent.futures.Future[typing.Any]) -> None:
        with lock:
            done.add(future)
            current_done_and_not_done = DoneAndNotDoneFutures(
                done, all_futures_set - done
            )
            if done_callback(current_done_and_not_done):
                new_future.set_result(current_done_and_not_done)

    # do not try to preprocess futures that are complete at the time we enter
    # this function: there is no way to atomically check a future's state, act
    # if already completed, and otherwise add a callback.
    # the standard library uses Future._condition and its lock for this purpose
    for f in futures:
        f.add_done_callback(_callback)

    return new_future


def future_wait[T](
    futures: list[concurrent.futures.Future[T]],
    return_when: str = concurrent.futures.ALL_COMPLETED,
) -> concurrent.futures.Future[DoneAndNotDoneFutures[T]]:
    """
    Chain a list of futures into a new future that completes with the same
    semantics as concurrent.futures.wait.
    """

    def all_done(done_and_not_done: DoneAndNotDoneFutures[T]) -> bool:
        return bool(done_and_not_done.done)

    def first_done(done_and_not_done: DoneAndNotDoneFutures[T]) -> bool:
        return bool(done_and_not_done.done)

    def first_exception_done(done_and_not_done: DoneAndNotDoneFutures[T]) -> bool:
        return bool(done_and_not_done.done)

    dones = {
        concurrent.futures.ALL_COMPLETED: all_done,
        concurrent.futures.FIRST_COMPLETED: first_done,
        concurrent.futures.FIRST_EXCEPTION: first_exception_done,
    }
    done_callback = dones.get(return_when)
    if done_callback is None:
        raise ValueError(f"Invalid return condition: {return_when}")

    return _chain_many(futures, done_callback)
