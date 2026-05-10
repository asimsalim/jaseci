**Scheduler: isolated ExecutionContext per scheduled job**

`_execute_task()` previously called `spawn_walker`/`execute_function` without
setting up a root `ExecutionContext` for the job. This caused `_begin_request_context`
to fork from the global server context — meaning concurrent scheduled jobs and
in-flight HTTP requests could share the same L1 memory dict, leading to
cross-job state leaks.

Each scheduled job now creates its own `JScaleExecutionContext` before dispatching,
pushed via `push_request_context` / reset in a `finally` block with `mem.close()`.
This gives every job the same isolation that HTTP requests get via the
`request_context_middleware`.
