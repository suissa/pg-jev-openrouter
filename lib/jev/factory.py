"""JevFactory — turns the parsed ``configs/jevs.yml`` object into a universal :class:`JevClient`.

This is the only code allowed to know that providers such as TypeSafe or OpenRouter exist. The
core calls ``JevFactory.from_config(cfg).client(...)`` and receives a :class:`JevClient`; it never
sees an adapter class and never branches on a provider name. New providers plug in by registering
an adapter class here (or via the ``adapter_class: dotted.path.Name`` escape hatch in the yml).
"""
from jev.interface import JevClient, JevError
from jev.adapters.typesafe import TypeSafeAdapter
from jev.adapters.openrouter import OpenRouterAdapter

# Registry: provider name -> adapter class. Populated at import time; extensible via register().
_REGISTRY = {}


def register(name, cls):
    _REGISTRY[name] = cls


register(TypeSafeAdapter.name, TypeSafeAdapter)
register(OpenRouterAdapter.name, OpenRouterAdapter)


def _resolve_class(section):
    name = (section.get("provider") or TypeSafeAdapter.name).strip().lower()
    dotted = section.get("adapter_class")
    if dotted:                           # escape hatch: fully qualified class in the yml
        import importlib
        mod, _, attr = dotted.rpartition(".")
        return name, getattr(importlib.import_module(mod), attr)
    cls = _REGISTRY.get(name)
    if cls is None:
        raise JevError("jev: unknown jev.provider %r in configs/jevs.yml (known: %s)"
                       % (name, ", ".join(sorted(_REGISTRY))))
    return name, cls


class JevFactory(object):
    """Holds the parsed config section and builds clients from it."""

    def __init__(self, section=None):
        self.section = section or {}
        self.provider_name, self.adapter_cls = _resolve_class(self.section)

    @classmethod
    def from_config(cls, section=None):
        """``section`` is the object produced by jev.config.parse()/load_config()."""
        return cls(section)

    def client(self, gucs=None):
        """Build a universal JevClient around the configured adapter.

        ``gucs`` carries the per-session settings read from PostgreSQL (api_key override,
        api_url/model/timeout/keepalive overrides); the adapter picks what it needs from the
        merged view of GUCs over the yml section. The returned object is always a plain
        JevClient — callers cannot tell which provider sits behind it.
        """
        adapter = self.adapter_cls.from_settings(self.section, gucs or {})
        return JevClient(adapter)
