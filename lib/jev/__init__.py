"""jev provider layer: one universal interface plus pluggable adapters.

The evaluation core (``_jev_eval`` in ``sql/jev--*.sql``) only ever talks to
:class:`jev.interface.JevClient` — a universal, provider-agnostic interface. Which adapter
actually serves the requests is decided by :class:`jev.factory.JevFactory` from the parsed
contents of ``configs/jevs.yml`` (see :mod:`jev.config`). The core never references a provider
name: it asks the factory for a client and calls ``client.evaluate(...)``.
"""
from jev.interface import JevAdapter, JevClient, JevError, JevRetryable
from jev.config import load_config, default_config_path, resolve_path, repo_root
from jev.factory import JevFactory
from jev.adapters.typesafe import TypeSafeAdapter
from jev.adapters.openrouter import OpenRouterAdapter

__all__ = [
    "JevAdapter",
    "JevClient",
    "JevError",
    "JevRetryable",
    "JevFactory",
    "TypeSafeAdapter",
    "OpenRouterAdapter",
    "load_config",
    "default_config_path",
    "resolve_path",
    "repo_root",
]
