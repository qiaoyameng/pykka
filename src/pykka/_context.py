from __future__ import annotations

import contextvars
from collections.abc import Iterable
from typing import Any


ContextVariable = contextvars.ContextVar[Any]
ContextVariables = Iterable[ContextVariable]


_UNSET_CONTEXT_VARIABLE = object()


def copy_selected_context(
    variables: ContextVariables,
    *,
    source: contextvars.Context | None = None,
) -> contextvars.Context:
    context = contextvars.Context()
    for variable in variables:
        if source is not None:
            value = source.get(variable, _UNSET_CONTEXT_VARIABLE)
        else:
            value = variable.get(_UNSET_CONTEXT_VARIABLE)
        if value is not _UNSET_CONTEXT_VARIABLE:
            context.run(variable.set, value)
    return context


def apply_message_context(
    baseline: contextvars.Context,
    message_context: contextvars.Context | None,
) -> contextvars.Context:
    selected_variables = (
        set()
        if message_context is None
        else {variable for variable, _value in message_context.items()}
    )
    context = contextvars.Context()
    for variable, value in baseline.items():
        if variable not in selected_variables:
            context.run(variable.set, value)
    if message_context is not None:
        for variable, value in message_context.items():
            context.run(variable.set, value)
    return context
