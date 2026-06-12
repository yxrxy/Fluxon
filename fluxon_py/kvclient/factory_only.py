"""Factory-only construction helper for KV clients.

This module provides a ``FactoryOnly`` base class that enforces
"construct via factory function only" semantics using a custom
metaclass. Classes inheriting from ``FactoryOnly`` cannot be
instantiated directly unless their ``_allow_init`` flag is
temporarily enabled by a factory.

The pattern is:

    class KvClient(FactoryOnly):
        ...  # abstract interface

    # In factory code:
    KvImpl._allow_init = True
    try:
        client = KvImpl(...)
    finally:
        KvImpl._allow_init = False

Any attempt to call ``KvImpl(...)`` when ``_allow_init`` is False
will raise a ``RuntimeError`` with a clear message.
"""

from abc import ABCMeta, ABC


class FactoryMeta(ABCMeta):
    """Metaclass that gates direct construction via ``_allow_init``.

    Subclasses start with ``_allow_init = False`` and must be
    constructed via a factory function that temporarily sets
    ``_allow_init = True`` on the target class.
    """

    def __new__(mcs, name, bases, namespace):
        cls = super().__new__(mcs, name, bases, namespace)
        cls._allow_init = False
        return cls

    def __call__(cls, *args, **kwargs):
        if not getattr(cls, "_allow_init", False):
            raise RuntimeError(f"{cls.__name__} 只能通过工厂方法创建实例")
        return super().__call__(*args, **kwargs)


class FactoryOnly(ABC, metaclass=FactoryMeta):
    """Base class for objects that must be created via factory methods."""

    pass
