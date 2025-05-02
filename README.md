# python-concurrency-playground
design experiments for concurrency patterns driven by example use cases

## pyconcurrentplayground.profileprocess
In the past I've needed to automate experiments and performance profiling.  The
general pattern is to set everything up in a container with a "pid 1"
coordinator process that starts the "process under test" and then a number of
supporting/profiling processes that typically take the process under test's pid
as an input.  It then waits for any process to finish or SIGTERM, terminates all
the supporting processes and waits for them to exit, and then terminates the
process under test and waits for it to exit.

## CSP/Go channels
Go channels are essentially a pipe or queue that integrates with coroutines to pass control to the coroutine on the other end.  There are a number of other properties that distinguish them from queues.

One property that I see missing from python libraries on github attempting to
build channels is that one can select from a number of channels actions (puts
and gets of specific channels) and can be sure that exactly one will occur.

If we look at Python's asyncio.Queue as a similar mechanism it has an async
get() to retrieve an item from the Queue.  But if you were to
`asyncio.wait([loop.run_task(q1.get()), loop.run_task(q2.get())], return_when=asyncio.FIRST_COMPLETED)`
there is no way to prevent the case that you get an item from each queue.

The experiment is to build a channel that is async, may be buffered, and has the following operations:
1. send an item into a channel
2. get an item from a channel (and also read if it is closed)
3. optionally return immediately if none can be performed.
4. close a channel (from the send end)

There should be a select function accepting a collection of sends and gets onto
channels and _only one must complete_

In the case of a full/empty buffer and coroutines must wait, then items from a
channel should be send/gotten in the order that coroutines arrive.

If several channels in a select group are immediately available it may either
choose an item in a deterministic (but not necessarily intuitive) order (e.g.,
some total log ordering) or select a random ready channel, as golang does

### Proposed implementation

#### Multi-channel Select
This is the challenge: we must never allow a race where 2 or more operations
from coroutines simultaneously complete different operations for a single Select
from some other coroutine.  Consequently there needs to be a lock associated
with each send and get that cannot be fulfilled immediately.

A `Select` object contains the following:
+ asyncio.Condition, additionally containing a lock.
+ `completed` -  `(Channel, Operation) | None` indicating which channel operation completed
+ `gotten_value` providing the value if the completed operation was a `get`
+ `sends` - a map of `Channel` to value for `send` operations in the `Select`.
+ `gets` - a set of `Channel` on which this `Select` is enqueued to perform `get`.

Note that `completed` could potentially be an atomic reference so that to claim
some other `Select` and satisfy your own you can set it without needing to
acquire a lock.

#### Channel
Each channel gets a (asyncio) lock protecting the following data and methods:
+ a list/buffer of available items, assuming this is a buffered channel
+ a deque of `Select` for senders to this channel (if the buffer is full)
+ a deque of `Select` for getters of this channel (if the buffer is empty)

Note that each of these can have their own lock.  It is important to make sure
that there remains a total order of all channel locks to prevent deadlock.

This may benefit from using some sort of linked list map so that `Select`s can
be looked up by id/address and removed quickly.

#### Deadlocks
Registering and unregistering (on completion) `Selects` will intuitively require
acquiring all the locks on the associated channels.  Care must be taken to avoid
deadlock.  Assume that every channel is assigned some 64 bit number called `id`.
This can be virtual memory address or a number assigned from an atomic long.
Whenever acquiring channel locks they must be acquired in `id`-order.

#### Creating a Select
Acquire locks for all channels in `id`-order.

If the select can be satisfied then choose some channel and operation (randomly,
deterministically, however).  Update the selected channel to perform the
operation.  Release all locks.  Complete any `Select` from the other end of the
channel that satisfied your operation (below).  Note that if a value is taken
from a channel buffer there will be no `Select` to complete.

Note that the above can be checked for each channel as soon as its lock is
acquired -- if the first channel to be locked can satisfy this operation then
you may proceed without acquiring additional channel locks.  You might always choose to satisfy operations in channel id order.

If no operation can be satisfied then create a `Select`.  Enqueue your select
into the senders and getters deques of each channel for each channel/operation
of this `Select`.  Release all channel locks.  Wait for notification on the
`Select` condition, releasing the `Select` lock.  When notified the `Select`'s
`completed` will indicate how the `Select` was completed.  If the completed
operation is a `get` the return value will be in `gotten_value`.  The completed
channel and operation will also be returned to the caller.

#### Completing a Select
When completing some other `Select` you will already hold locks for all the
channels of your own `Select` and have just acquired the lock for the other
`Select` object.

Remove the other `Select` from the deque of the channel you took it from.

If `completed` is already non-None then release the `Select` lock and resume
trying to satisfy your own `Select` from its other operations.

Otherwise release all channel locks.  Set `completed` indicating the channel and
the operation _from the perspective of that other `Select`_.  If your operation
is a send then write the value you are sending into the `Select`'s
`gotten_value`.  If your operation is a get then retrieve a value from `sends`
using the channel as the lookup key.  In both cases notify the condition
variable to wake up that `Select`'s coroutine.  Release the `Select` lock.

At this point both you and the other `Select`'s coroutine can correctly
continue.  That `Select` may be referenced from the deques of other channels.
If other operations try to use this `Select` they will acquire the lock, see
that `completed` is set, release the lock, and remove the `Select` from the
deque.

You may also choose to remove references from every channel.  This can now be
done by acquiring the channel locks one at a time.  Do not hold the `Select`'s
locks while you do this.

#### Correctness

This will not deadlock.  Lock order is always channels by-`id` and then one
`Select` from those channel queues at time until something satisfies an
operation or there is no such `Select`.  If no such `Select` then a new `Select`
is made and registered to all (already-locked) channels.

Removing references after a `Select` has been completed can be done without holding that `Select`'s lock and getting channel locks one at a time.

There are 2 properties that ensure correctness:
+ all channel locks are held with checking for satisfying conditions or registering a `Select` to all of them.  We will either satisfy our `Select` or simultaneously register it with all channels.
+ a `Select` is completed by acquiring its lock.  A `Select` can be
completed/satisfied exactly one time.

This flexes (and adds complexity) by not requiring that a `Select` be atomically
unregistered from all channels when completed.  Other coroutines will still find
the `Select` in channel queues.  However, they will immediately see that it is
completed and simply ignore and dequeue it.

#### Optimizations
make `completed` an atomic ref, or else separate it into an atomic bool and a
separate field indicated the channel and operation.

regarding asyncio there should be no need to acquire channel locks.  There is no
await until awaiting Condition.wait.  And so I think even the `Select` locks are
unnecessary as everything before and everything after the wait will already be
atomic (as there is no awaiting) and Condition.wait releases the lock anyways.

### Channel methods:
+ __init__(self, size: int=0)
+ close() - may not add any more senders.  Any read once there is no more
buffered data or queued sender returns `Closed`

### Operation:
An enum/union with at least the following:
+ Send(Channel, Value, ignore_on_closed=False)
+ Get(Channel, ignore_on_closed=False)
Where ignore_on_closed indicates that this Operation will never be selected if
it is closed (or closed and empty in the case of a Get).

### Select methods:
+ __init__(self, operation...: Operation)
+ async select() -> SendResult(channel, operation) | GetResult(channel, operation, value) | Closed(channel, operation) - note that Closed needs an operation as you may have a select that sends and gets from the same closed channel and we need to pick one.
+ add(operation) - useful when using the same `Select` in a loop
+ remove(operation) - useful when using the same `Select` in a loop

### Observation: async locking
while there is an `asyncio.Lock` class for locking in asyncio (which yields the
coroutine on contended lock acquire and notifies/schedules on lock release),
there are many situations where multiple variables can be accessed and even
functions called without any possibility of interveaning coroutines running.
Specifically, any block of code in which nothing is `await`ed is guaranteed to
run atomically with respect to the asyncio event loop.

This is somewhat similar to Rust's mutability/immutability type checking: one
may not hold an immutable reference and pass a mutable reference to a function
(which may modify it) and one may not hold a mutable reference and pass an
immutable reference to a function (which may store it somewhere that outlives
the function call).  In python, whenever you `await` you lose immediate, obvious
control of what code will run in other coroutines -- the order of events might
be well prescribed, but it is less clear and less intuitive.

In the library code I've read these non-premptable-and-thus-atomic blocks of
code are never made apparent.  There are no comments and no structure to
indicate that this is intentional and required for correct execution.  The
following might be useful mechanisms to convey this intent and to prevent
changes from introducing `await` points in what should be non-yielding, atomic
blocks of code:
+ asyncio.Lock: if nothing is ever `await`ed while a lock is held then the lock
will always be available when acquired.  This hints at the true utility of
asyncio.Lock -- to `await` in a critical section and still guarantee that it
runs atomically.
+ wrap all mandatory atomic/non-awaiting blocks in non-async functions or
non-async `with`.  This prevents code inside them from awaiting (both a runtime
and static-checking error).
+ decorate the above functions/`with` with a decorator that makes it clear that
the block is non-async for the purpose of non-yielding atomicity.
+ create a decorator for a class that enforces that no methods of the class may
return an `Awaitable`, such as an `async` coroutine.  Place the variables that
must be protected by this pseudo-lock in the class, and only access them through
(non-async) methods of the class.  This resembles a java-style monitor
(`synchronized` blocks or methods on a class).

None of this matters for code that immediately `await`s coroutines and resembles
classical blocking code within a single task.  It only matters when tasks are
scheduled concurrently and those tasks access common data.

## Efficient transactional batched tree traversal

Consider FoundationDB: a distributed, ordered Key-Value store with time-bounded
(5s) strictly serializable transactions.  Imagine storing a tree in FDB, some
hierarchical data structure.  An example might be storing a namespace in a
distributed system, such as a file system with a root, directories, and files,
all organized by those directory and file names.

The challenge here is to traverse a contiguous (for some definition) portion of
the tree and act on the nodes with some multi-node transactional consistency.
When traversing a simple 1-dimensional list the problem is simpler (but can
still be complex): visit items in order using a streaming/pre-fetching scan and
process the items as they arrive.  Stop scanning new items with some duration in
the transaction remaining to give time to process the remaining items.

When traversing a tree this becomes harder given some constraints that make the traversal more useful:
+ Guarantee that every node from the root to a node is read in the same
transaction that traverses/processes the node.
    + This guarantees that you know the complete path of the node in the
    transaction that it is processed, preventing you from interpreting phantom
    paths that never actually exist.
    + This is some measure of isolated consistency for file and its path.  If
    files are created, deleted, and moved while some large multi-transaction
    traversal executes in parallel we might visit some files multiple times (it
    is visited, moved later in traversal order, and visited again) or fail to
    visit some files (it is moved earlier in traveral order, skipping over the
    traversal cursor at that point in time)
+ Provide some notion of traversal order between transactions (order within a
transaction doesn't matter from the perspective of data consistency -- it's a
transaction and so appears as a snapshot).
    + For example, you may want to visit/process all files in directory tree,
    processing files within a directory, in lexicographical order, and then
    recursively visit each directory in lexicographical order.
    + Doing so can make it easier to reason about how nodes will be visited
    across multiple transactions.  As a contrived example, if files can only be
    moved to directories that sort later within this nested lexicographical
    order then you can be guaranteed that all files existing at the start of the
    multi-transaction traversal will be visited at least once.
    + Within a single batch/transaction you still want to retrieve data
    concurrently, out of traversal order, for improved performance.  When the
    transaction runs out of time there will gaps in what has been
    retrieved and processed (e.g., you've processed files in directory "/b" but
    have not processed all files in directory "/a") and this must be prevented.
    Alternatively, the allowed traversal ordering can be relaxed somewhat, with
    a cross-transaction cursor remembering where in directory "/a" to begin
    traversal, as well as to continue in directory "/c" -- this state gets
    complex and potentially large.
    + the overall concurrency of accesses to FDB must be limited to prevent
    overwhelming the storage servers and stealing all network bandwidth and CPU
    time in a presumably-multi tenant client.

The goal here is to highlight some specific visitors/traversals and how they would be best implemented.
+ visiting all directories and files of a file system to export this list and
properties of each node.
    + a strict (inter-transactional) in-order traversal exporting a directory,
    all immediately-contained files in lexicographical order, and then all
    contained directories in lexicographical order.
    + example relaxation: files immediately located in the same directory must be processed in lexicographical order but directories may be processed in parallel.
    + example relaxation: files and directories may be visited in any order so
    long as each path is visited exactly once (including if there is no
    directory/file at that path at the time it is visited), the complete path
    from root is visited int he same transaction in which the node is processed,
    any inter-transaction/batch cursor has a bounded size, and the amount of
    work/requests in a transaction that must be discarded and repeated in a
    future batch/transaction is bounded.

### Concepts and Data Structures

#### Request Semaphore (or Scheduler?)
Constraints concurrent requests to FDB.  Use asyncio.Semaphore or
asyncio.BoundedSemaphore.  The challenge is to associate it with all coroutines
and requests in the traversal and with the FDB client.  Some options to
integrate with FDB accesses:
+ manually as an `async with` surrounding each access.  This is tedious and
might be easy to forget.
+ via context vars that the FDB client uses.  For example, a stack of semaphores
per context for the various limits (a per-process limit, per traversal limit,
potentially per user or account limit).  Semaphores must always be acquired in a
defined total order to prevent deadlock.
+ wrapping the FDB client (see Struct Embedding below) and passing along an
instance of the effective client throughout coroutines or placing this effective
client in a context var.  Similar to placing context var in the fdb client but
allows isolation and extensibility without changing the client code.

#### Priority Semaphore
While traversing the tree we want to do as little work that will be wasted and
must be repeated in a future batch because of gaps.  For example, if the root
directory contains subdirectories `/a` and `/b` we may start to traverse both
concurrently.  If it turns out that `/a` contains a large number of files and
directories then we want to prioritize traversing items in `/a` before `/b` and
at some point we will need to suspend processing `/b`.  This is also beneficial
because it limits the number of coroutines at any point in time (ignoring the
depth of traversal).

Assume that we create new coroutines each time we traverse into a new directory,
start scanning files and directories from an FDB iterator in a directory, and
process a file.  We can gate task creation with a priority semaphore that
accepts the path in its `acquire` and prioritizes tasks by lexicographical
ordering of the paths.  Once created a task may run, but if this task creates a
new subtask with insufficient priority it will block.

We still need a request semaphore so long as FDB requests are not 1:1 with
tasks.  That is, if any task can itself issue concurrent FDB requests we'll
still need that request semaphore.

For deep trees it's possible that at each level of traversal we create a number
of tasks that then get blocked creating their subtasks because of insufficient
priority.  This might need to cancel these tasks in a way that let's their
parent pause and potentially recreate those tasks if the traversal progresses
enough that they have high priority.

##### Implementation

We need to define the set of tasks that create coroutines and that require a
permit, and these may not be the same.
+ process directory or file -- process it as if it is a leaf.  This is a
non-recursing action that can complete without having to acquire additional
permits.
+ get next child of directory -- once retrieved it will generate new tasks to process that child (file or directory) and to get the next child if it is a directory.
+ NOT "enter directory."  It might be advantageous to structure coroutines this
way, but it does not require a permit.  Doing so would result in deep tree
traversals that exhaust all permits simply traversing down the tree at which
point it couldn't make progress.

The priority key is (path, action) where actions are:
+ retrieve next child
+ process file
+ process directory

A comparator on this tuple need not be a literal python tuple ordering.  By modifying the order we control the traversal order.

Protect the semaphore class with a lock.  It will contain a priority queue of
coroutines to continue in the form of (key, asyncio.Event).  There is also a
counter of in-flight tasks.  The method `acquire(path, action)`:
1. locks
2. if the count of in-flight tasks is less than the limit
    1. increment the count of in-flight tasks
    2. release the lock and continue without blocking
3. otherwise, add to the priority queue a key constructed from `path` and
    `action` along with a new `asyncio.Event`
4. release the lock
5. block on the `asyncio.Event`

The method `release()`:
1. locks
2. if the priority queue is empty decrement the count of in-flight tasks, unlock, return.
3. otherwise take the next item from the priority queue.
4. set the `asyncio.Event` for that item.
5. unlock and return

If at any point we see that the number of items in the priority queue is too
large we have an opportunity to cancel some of those tasks.  Some things to consider:
+ We would need a handle to the associated task to cancel it.  This can be done
by calling `get_current_task` in `acquire` and putting it in the priority queue
as well.
+ We need the parent awaiting that task to know that it has not itself been
cancelled.  The child task might be rescheduled later once the curser advances
and these tasks have sufficient priority.  How do we indicate this?  The
`Task.cancel` method accepts a str `msg` but I feel like this isn't a good use.
We _do_ want to associate some data with the CancelledError but unfortunately
don't have any other means to do that.  I think we'd need to create some
object/data every time a parent creates a task that describes the cancellation,
and this object gets passed to the priority queue.  When the parent catches the
CancelledError it checks that data to determine what kind of cancellation it is.
When the task is cancelled this data is first written.
+ How do we resume recreating these tasks?  The parent that scheduled them needs
to examine which tasks were cancelled and clean up its state.  This pruning is
only effective if in some directory multiple tasks were cancelled.  For example,
if the "get next child" is cancelled and the 2 previous children listed were
files and the "process files" tasks for both were also cancelled then we may
schedule a "get next child" to retrieve those files next.  This new task will
block on the semaphore and we've replaced 3 scheduled tasks with 1.

See Reorder Buffer below.  The priority of tasks may depend on the overall state
of task progression.  Specifically, when to allow a "process" task to proceed
depends on whether there are process tasks earlier in key-order that have not
completed and whether there are any "list children" earlier in key-order that
might spawn new "process tasks" with higher priority.  With this introduced
complexity this really turns more into a "Scheduler" than a priority queue.

#### Inter-Batch Cursor

Similarly to the priority semaphore the inter-batch cursor lists what tasks/work
must be done next in the next batch.  Unlike the priority semaphore this cursor
lasts between transactions and in some cases may even want to be persisted into
FDB.  If the processing of files and directories is always done in order (or by
the end of every transaction the set of processed files and directories is
contiguous) then the cursor is simply a `key(path, action)` of where to start the
next transaction, assuming a well-defined order on this key.

If the traversal processing need not happen in order then this cursor can become
a set of traversal intervals that have already completed, or equivalently
intervals that still need to be traversed.  This can be pair resembling
`(key(path, action), key(path, action))` naming the interval as traversed or not.

The size of this compound cursor needs to be considered and enforcing it should
be integrated with the priority semaphore.

The number of ranges being traversed at any time within a transaction may exceed
the intended size of the inter-batch cursor.  What is important when allowing
this is that you are "committing to process a range" by the time the batch ends
and the transaction commits, otherwise the transaction must fail and retry.
Thus you may only choose to do this when all items have been listed and need
only be processed (which you control and might be a constant amount of work per
item) or when you somehow know the number of items that will be listed.  See
Reorder Buffer below.

#### Reorder Buffer

The above mechanisms and data structures allow for concurrency within a
transaction.  However, we often want to restrict the apparent concurrency or
ordering of processing between transactions/batches.  For example, we may want
to process files and directories in a strict order, or to restrict the number of
intervals of this order that must be tracked in an inter-batch cursor.  This reordering and concurrency within a batch transaction, but enforcement of ordering by the end of the transaction somewhat resembles a reorder buffer in computer architecture:
+ the order of instructions is recorded -- here the order of directories and
files is recorded as they are retrieved/traversed
+ instructions are only `committed` when they reach the head of the ROB (they
are next to commit) -- files and directories are only `processed` when we have
retrieved all files and directories earlier in key order (and there are no
remaining children earlier in order that might be retrieved!) and all earlier
files and directories have been processed.
+ multiple instructions may be committed in the same cycle so long as all of
their effects take place (e.g., writing to a register file) and none of them
trap; a contiguous segment of the ROB commits together -- we similarly want a
contiguous interval of the tree to be traversed and processed in a transaction
and have freedom to introduce concurrency so long as this happens.

Implementing this requires:
+ details on the priority semaphore to control the level of concurrency and the
likelihood of gap intervals.
+ details on the priority semaphore and priority of "process key/directory"
events so that we do not process events that will cause a gap interval at the
end of a transaction.  "Process" events should only proceed when all files and
directories that fall earlier in key-order have been listed, and when there is a
bounded number of such files and directories that have not been themselves
processed.
+ Some duration of the transaction should be reserved to "drain" the queue
    + Stop listing new children.
        + Possibly restricted by current depth to allow leaf and near-leaf directories to complete.
        + Possibly restrict according to key ordering -- don't list new children in greater key-order but do list children whose processing is expected to close a gap.
    + Only "process" tasks remain.  These do not generate new work and so the queue drains.
+ If the order of "process" tasks is always constrained to limit the resulting
inter-batch cursor then this quiescing is really an optimization to avoid wasted
work that will be repeated in the next batch/transaction

# Python and Asyncio Notes, Observations, Ideas

## CSP Channels

there's an aspect of golang channels that I don't see addressed: take from
_exactly 1_ queue.  asyncio.Queue has an async get(), but if you try to
get() from multiple Queues concurrently you cannot guarantee after the first
completes that you can cancel or stop all the others before they pop from
other queues.  None of the "build golang channels in python" I see on github
address this.

## Asyncio Atomic, Critical Sections, and Locking

It is common in the standard library asyncio code that itself writes
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

## Struct Embedding
Several times I've encountered the need to wrap-and-extend an object whose
construction and initialization I don't control.  This is a use case for Golang
struct embedding -- the new struct/class delegates all methods to the embedded
class, and I construct the new class by passing an instance of the embedded
class instead of calling a super constructor.

There's not a good way to do this in Python (or Java):
+ compose the base class and manually delegate all methods.  New class does not
extend the base class, but would implement a shared protocol (duck typing).
+ compose by extending the base class, accepting an instance of the base class,
not calling super().__init__, and overriding every public method.  In this case
the new class "is a" base class, for whatever that's worth.
+ compose and provide a __getattr__ that delegates to the embedded __getattr__
if not found in the new class.  Very dynamic.  Difficult to understand.  No
static checking.
+ Take an instance of the base class and modify or add attributes directly.
Instance remains the base class.  Very dynamic.  No static checking.  Quite
confusing as instances do not behave according to their class definition.

I'm looking for a combination of these: automatically delegate non-overridden
methods, construct via an instance of the base class, provide static checking
including type checking.

Might this be solved by https://peps.python.org/pep-0638/ - "Syntactic Macros"
if accepted?