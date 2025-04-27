import collections.abc
import concurrent.futures
import dataclasses
import functools
import threading
import typing


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


def future_to_outcome[T](
    future: concurrent.futures.Future[T],
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


@dataclasses.dataclass(frozen=True)
class FutureAndTaskData[FutureT, TaskDataT]:
    future: concurrent.futures.Future[FutureT]
    task_data: TaskDataT


@dataclasses.dataclass(frozen=True, init=False)
class ResultAndTaskData[FutureT, TaskDataT](FutureAndTaskData[FutureT, TaskDataT]):
    """
    Unpacks the result and exception from future for structural matching.
    """

    result: FutureOutcome[FutureT]

    def __init__(
        self,
        future: concurrent.futures.Future[FutureT],
        task_data: TaskDataT,
        result: FutureOutcome[FutureT],
    ):
        """Due to a bug in pylance it cannot recognize kwargs generated from a
        parent constructor that has generic type var.  So here we manually
        create the constructor and then use object.__setattr__ to assign to
        frozen fields"""
        object.__setattr__(self, "future", future)
        object.__setattr__(self, "task_data", task_data)
        object.__setattr__(self, "result", result)

    def __str__(self) -> str:
        return f"{self.task_data}: {self.result}"


def wait_and_zip[FutureT, TaskDataT](
    future_label_pairs: collections.abc.Sequence[FutureAndTaskData[FutureT, TaskDataT]],
    timeout: float | None = None,
    return_when: str = concurrent.futures.ALL_COMPLETED,
) -> tuple[
    list[ResultAndTaskData[FutureT, TaskDataT]],
    list[FutureAndTaskData[FutureT, TaskDataT]],
]:
    future_labels = list(future_label_pairs)
    future_to_pair = {p.future: p for p in future_labels}

    futures = [p.future for p in future_labels]
    (done, not_done) = concurrent.futures.wait(
        futures, timeout=timeout, return_when=return_when
    )
    done_results: list[ResultAndTaskData[FutureT, TaskDataT]] = []
    for f in done:
        future_and_label = future_to_pair[f]
        result: FutureOutcome[FutureT] = future_to_outcome(f)
        result_and_task_data = ResultAndTaskData(
            future=future_and_label.future,
            task_data=future_and_label.task_data,
            result=result,
        )
        print(f"future completed. {result_and_task_data}")
        done_results.append(result_and_task_data)
    return (
        done_results,
        [future_to_pair[f] for f in not_done],
    )


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
