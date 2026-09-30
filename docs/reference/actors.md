# Actors

::: pykka.Actor

::: pykka.ActorRef

## Bounded mailboxes

By default, an actor's mailbox is unbounded: if messages are produced faster
than the actor can process them, the mailbox keeps growing. To bound the
mailbox, set
[`ThreadingActor.max_queue_size`][pykka.ThreadingActor.max_queue_size] to a
positive integer:

```py
class BoundedActor(pykka.ThreadingActor):
    max_queue_size = 10
```

When the mailbox is full:

- [`ActorRef.tell()`][pykka.ActorRef.tell] raises
  [`pykka.MailboxFull`][pykka.MailboxFull] immediately by default. Pass a
  `timeout` to wait up to that many seconds for free capacity, or `None` to
  wait until capacity is available.
- [`ActorRef.ask()`][pykka.ActorRef.ask] does the same. When `block=False`,
  the [`MailboxFull`][pykka.MailboxFull] exception is set on the returned
  future instead of raised.

A message is never lost: a send either succeeds atomically, or it raises (or
sets on a future) an explicit error.

Stopping an actor uses a channel independent of the message mailbox, so a
full mailbox can never prevent an actor from being stopped.

An actor sending a message to itself never blocks: when the mailbox is full,
the self-send fails immediately with [`MailboxFull`][pykka.MailboxFull],
since the actor is the mailbox's only consumer and blocking would deadlock
it.

::: pykka.Mailbox
