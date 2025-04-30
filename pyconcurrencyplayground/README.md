Profile Process

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

implementations:

+ runners.RawThreadRunner - wait for a signal and each process's completion in a
  separate thread.  Perform follow-up actions in those threads, accessing shared
  state via locks.
+ futuresrunner - runners placing the wait for a signal and each process's
  completion in a concurrent.futures.Future.
  + futuresrunners.one_loop_runner - waits for the next finished future in a
    loop.  Examines the state of futures on each iteration and takes any actions.
    Returns when all processes have exited.
  + futuresrunner.multiple_waits_runner - waits for 1. any event to finish and
    then terminates all supporting processes, 2. all supporting processes to exit
    and then terminates the process under test, 3. the process under test to exit
    and then returns.
  + futuresrunner.structureless_runner - same as multiple_waits_runner but does
    not compose richer types containing futures and data describing those futures.
+ asyncrunner - runners using python asyncio

notes/opinions:
+ runners.RawThreadRunner is difficult to follow.  Using shared state makes it
hard to understand the overall state machine and the order in which events
happen and code runs.  The futuresrunners all act from the main thread, making
clear the order of future tasks and the code that this triggers.
+ futures and context: the challenge with using futures is understanding the
full context of which task completed based on a future.
concurrent.futures.wait(return_when=FIRST_COMPLETED) indicates which futures are
completed.  But since you're waiting on several futures you need to identify
that completed task in order to figure out what to do next.  The alternative is
that all tasks are equivalent and you don't care which one finished, but that's
not the case here. Here are a few options:
  + allow the result of the future to describe the task.  The result value, in
  addition to its computed result, can pass along data provided to the future
  task that describes it (e.g., pid and process ordinal). Making an Exception
  identify the task is perhaps a bit harder.  There's not a good way to add
  arbitrary data to an Exception.  You can add an attribute dynamically and have
  no typing context or wrap in some other exception and likely mess up later
  error handling.  Additionally, this requires that the future task be modified
  or chained to pass along the identifying data.  The called function shouldn't
  need to be aware of this since it's for the benefit of the caller.
  + extend future to contain the identifying data.  This is difficult, or at
  least tedious.  You can add an attribute dynamically or subclass the future to
  contain additional data.  This is tedious because you don't control creation
  of futures from executors so your new future type needs to construct from a
  future instance and delegate all method calls to that other future.
  + maintain a mapping of future to identifying data and be done.  Definitely
  the most straightfoward.  This is implies a convention, not necessarily a tool
  that provides the functionality for you.
  + in some cases you don't even need this and instead at each step fo the way
  you can inspect the state of all futures to decide what to do next.  Assuming
  you keep that set of futures structured (ordered in a list, organized in a
  map, or kept in separate sets by different task types) you can use this
  structure to determine if conditions have been met to take new actions.
  + the only reason I actually need to know exactly which future has completed
  here is to log the result of the specific task.  This could arguably be done
  in a chain/callback or by wrapping a coroutine in a new call -- it doesn't
  require any shared data access
  + "waiting for first task to finish" and then taking action depending on the specific task and its data is not easy in python.
    + you can get the future that completed, but you need to tie this to an
    identity, and "what to do next"
    + I'm looking for something like golang select - describe the event (get or
    put a channel) and any resulting data (is channel closed? data returned from
    channel) and then perform a task (the code in the select case).  Closest is
    a match or if-else block on the futures or conditions (e.g., is the
    completed futures in some specific set of futures).
    + Look at python standard library's asyncio.TaskGroup (asyncio.taskgroups.TaskGroup).  It's incredibly complex.
      + Exception management is insane
      + cancellation is done via add_done_callbacks so this looks like multithreading
+ on async:
    + I can see how this makes some things a bit simpler, like cancellation, and
    how it avoids context switching for many concurrent tasks with repeated
    blocking io.
    + I appreciate functions being declared async as meaning "this function may
    block".  That's helpful for reading and documenting.
    + I don't think it meaningfully helps the case of "wait for first task to
    finish and then do something" -- it still looks like multithreading or the
    raw wait() primitive.
    + there's an aspect of golang channels that I don't see addressed: take from
    _exactly 1_ queue.  asyncio.Queue has an async get(), but if you try to
    get() from multiple Queues concurrently you cannot guarantee after the first
    completes that you can cancel or stop all the others before they pop from
    other queues.  None of the "build golang channels in python" I see on github
    address this.
    + It is common in the standard library asyncio code that itself writes
    coroutines to access data from multiple tasks but not pay any special
    attention to atomicity/ordering of accesses.  For now all tasks in the same
    loop run on the same thread (and with the GIL being removed this could
    change at some point!) so there's no actual data race.  But any variable
    accesses separated by an await could have interveaning code run.  This is
    similar to the rust "mutable" argument where to accesses with an
    interveaning function call need to know that the function call doesn't
    modify any of the relevant data.  I just find it a bit harder to reason
    about if that await/function call involves several concurrent tasks using
    asyncio.wait or gather.  It looks like multi-threaded code and I think needs
    some organization if not outright asyncio.Locks

