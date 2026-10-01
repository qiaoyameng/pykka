# Supervision

A supervisor is an actor that owns a static set of child actors and is
responsible for their lifecycle: it starts them when it starts, restarts them
when they fail according to a configured policy, and stops them when it stops.

Supervision is available with the threading runtime
([`ThreadingActor`][pykka.ThreadingActor]) only.

## Declaring children

Subclass [`Supervisor`][pykka.Supervisor] together with
[`ThreadingActor`][pykka.ThreadingActor] and implement
[`children()`][pykka.Supervisor.children], returning a list of
[`ChildSpec`][pykka.ChildSpec]:

```python
import pykka


class Cache(pykka.ThreadingActor):
    ...


class Worker(pykka.ThreadingActor):
    def __init__(self, config):
        super().__init__()
        self.config = config


class App(pykka.Supervisor, pykka.ThreadingActor):
    def children(self):
        return [
            pykka.ChildSpec(Cache, name="cache"),
            pykka.ChildSpec(Worker, kwargs={"config": {...}}, name="worker"),
        ]


app_ref = App.start()
cache_ref = app_ref.proxy().child_ref("cache").get()
```

Children are started in declaration order and stopped in reverse declaration
order. Child names default to the actor class name and must be unique within a
supervisor.

If a child fails to start, the supervisor aborts its own startup after stopping
every child that had already been started, so no actors are left orphaned.

## Restart strategy and intensity

- [`RestartStrategy.ONE_FOR_ONE`][pykka.RestartStrategy] (default): only the
  failing child is restarted; its siblings are left untouched.
- [`RestartStrategy.ALL_FOR_ONE`][pykka.RestartStrategy]: when one child fails,
  the whole group is restarted together, in declaration order.

[`max_restarts`][pykka.Supervisor.max_restarts] and
[`within_time_range`][pykka.Supervisor.within_time_range] define a sliding
window restart budget. If more than `max_restarts` restarts are needed within
`within_time_range` seconds, the supervisor gives up on the child: the child is
permanently stopped and unregistered, and
[`on_restart_intensity_reached()`][pykka.Supervisor.on_restart_intensity_reached]
is called. The budget slides again once failures stop for longer than the
window.

```python
class ResilientApp(pykka.Supervisor, pykka.ThreadingActor):
    restart_strategy = pykka.RestartStrategy.ONE_FOR_ONE
    max_restarts = 3
    within_time_range = 10  # seconds
```

## Business exceptions vs. actor death

Exceptions raised while handling an `ask()` message are returned to the caller
through the future and do **not** stop the actor, so they do not trigger a
restart. Only an exception that actually terminates the actor — for example one
raised while processing a `tell()` message, or in
[`on_start()`][pykka.Actor.on_start] — causes the supervisor to restart the
child.

## Stable references

The [`ActorRef`][pykka.ActorRef] of a supervised child is stable across
restarts. Messages sent to the same reference reach the replacement
incarnation once it has started, and
[`actor_urn`][pykka.ActorRef.actor_urn] stays constant.

There is a brief window between the death of an incarnation and the start of its
replacement. Messages sent in that window are delivered to the old inbox and
receive an [`ActorDeadError`][pykka.ActorDeadError]; they are not migrated to
the new incarnation. Callers that need delivery across a restart should retry
until the child answers again.

While a supervisor is stopping, it never restarts children; pending failures
are ignored and all children are stopped in reverse order.

::: pykka.Supervisor

::: pykka.ChildSpec

::: pykka.RestartStrategy
