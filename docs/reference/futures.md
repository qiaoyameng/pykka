# Futures

::: pykka.Future

Callbacks created with `filter()`, `join()`, `map()`, `reduce()`, and
`set_get_hook()` run with the context variables that were current when the
callback was registered. When `get()` is later called from another thread, the
waiting thread's context is restored after the callback returns.

::: pykka.get_all
