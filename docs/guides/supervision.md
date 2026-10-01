# Supervision

Pykka lets you build supervision trees by combining the
[`Supervisor`](../../reference/supervision.md#pykka.Supervisor) mixin with an
actor implementation, for example [`ThreadingActor`](../reference/runtimes.md).

A supervisor declares the child actors it is responsible for using
[`Child`](../../reference/supervision.md#pykka.Child):

```python
import pykka


class Worker(pykka.ThreadingActor):
    ...


class Manager(pykka.Supervisor, pykka.ThreadingActor):
    children = (
        pykka.Child(Worker, name="worker-1"),
        pykka.Child(Worker, name="worker-2"),
    )
```

When the manager starts, the children are started in declaration order, before
the manager's own `on_start()` hook runs. When the manager stops, the children
are stopped in reverse order.

## Restart strategies

Use
[`SupervisorStrategy`](../../reference/supervision.md#pykka.SupervisorStrategy)
to configure what happens when a child crashes:

```python
class Manager(pykka.Supervisor, pykka.ThreadingActor):
    children = (pykka.Child(Worker, name="worker"),)
    strategy = pykka.SupervisorStrategy(
        max_restarts=3,
        within_seconds=5,
        strategy=pykka.RestartStrategy.ONE_FOR_ONE,
    )
```

- [`RestartStrategy.ONE_FOR_ONE`](../../reference/supervision.md#pykka.RestartStrategy)
  restarts only the child that failed.
- [`RestartStrategy.ALL_FOR_ONE`](../../reference/supervision.md#pykka.RestartStrategy)
  restarts all children when any child fails.

A child that is restarted more than `max_restarts` times within
`within_seconds` seconds is stopped permanently. The window slides: failures
older than `within_seconds` do not count. Set `max_restarts=None` for an
unlimited number of restarts. A per-child strategy can be provided through
[`Child.strategy`](../../reference/supervision.md#pykka.Child).

Override
[`on_supervision_failure()`](../../reference/supervision.md#pykka.Supervisor.on_supervision_failure)
to react when a child is given up permanently.

## Business exceptions do not restart actors

An exception raised while processing a message sent through
[`ActorRef.ask()`](../reference/actors.md#pykka.ActorRef.ask) is returned to
the caller. The actor itself stays alive, so such an exception does not cause
a restart.

Only actual life-cycle failures restart an actor: an unhandled exception in a
message sent with [`ActorRef.tell()`](../reference/actors.md#pykka.ActorRef.tell),
or an exception in a life-cycle hook such as
[`on_start()`](../reference/actors.md#pykka.Actor.on_start).

## Actor references follow new incarnations

The [`ActorRef`](../reference/actors.md#pykka.ActorRef) returned for a
supervised child stays the same object across restarts and keeps the same URN.
Messages sent to it are delivered to the newest incarnation, and
`is_alive()` only becomes `False` when the child is stopped permanently.
An [`ActorProxy`](../reference/proxies.md) that was created before a restart
is bound to the old incarnation; call `actor_ref.proxy()` again to get a proxy
for the new incarnation.
