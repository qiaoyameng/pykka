from __future__ import annotations

import contextvars
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from typing import Any, TypeVar

__all__ = [
    "ContextVars",
    "ContextVarSnapshot",
    "copy_context_vars",
    "run_in_context",
    "use_context_vars",
]


T = TypeVar("T")

ContextVarSnapshot = tuple[tuple[contextvars.ContextVar[Any], Any], ...]
ContextVars = Iterable[contextvars.ContextVar[Any]]


_UNSET = object()


def copy_context_vars(
    variables: Iterable[contextvars.ContextVar[Any]],
) -> ContextVarSnapshot:
    snapshot: list[tuple[contextvars.ContextVar[Any], Any]] = []
    for variable in variables:
        value = variable.get(_UNSET)
        if value is not _UNSET:
            snapshot.append((variable, value))
    return tuple(snapshot)


@contextmanager
def use_context_vars(
    snapshot: ContextVarSnapshot,
) -> Iterator[None]:
    tokens: list[tuple[contextvars.ContextVar[Any], contextvars.Token[Any]]] = []
    try:
        for variable, value in snapshot:
            tokens.append((variable, variable.set(value)))
        yield
    finally:
        for variable, token in reversed(tokens):
            variable.reset(token)


def run_in_context(
    context: contextvars.Context,
    func: Callable[..., T],
    *args: Any,
    **kwargs: Any,
) -> T:
    def run() -> T:
        for variable, value in tuple(context.items()):
            variable.set(value)
        return func(*args, **kwargs)

    return contextvars.Context().run(run)
