"""Universal interface for interacting with Jev — the only contract the core may use.

Provider-agnostic by design: the core builds one shared *state* plus a dict of questions
(kind ``noul`` / ``score`` / ``choice``) and calls :meth:`JevClient.evaluate`. It gets back a
normalised answer dict shaped exactly like the TypeSafe SystemOne response::

    {"model": "...",
     "answers": {"r0": {"type": "noul", "noul": 0.93}, ...},
     "usage":   {"input_tokens": 123, "output_tokens": 5}}

Every provider plugs in as a :class:`JevAdapter` (transport + request/response translation),
and :class:`jev.factory.JevFactory` decides which adapter to build from the parsed
``configs/jevs.yml``. The core code never knows — and must never care — which provider is
behind the client.
"""


class JevAdapter(object):
    """Transport + translation for one provider. Implementations must be thread-safe."""

    name = "abstract"

    def evaluate(self, state, questions):
        """Judge ``questions`` (qid -> {type, instructions, criteria?}) over the shared ``state``.

        Returns the normalised answer dict described in the module docstring. Raises
        :class:`JevError` on a non-retryable provider error and :class:`JevRetryable` on
        transient ones; adapters may also raise plain network exceptions so the caller's
        retry loop can react to them.
        """
        raise NotImplementedError("adapter %s does not implement evaluate()" % self.name)


class JevClient(object):
    """The universal handle the core holds: delegates straight to the injected adapter.

    The core receives this object from :class:`jev.factory.JevFactory` and never inspects or
    branches on the provider behind it.
    """

    def __init__(self, adapter):
        if not isinstance(adapter, JevAdapter):
            raise TypeError("JevClient needs a JevAdapter, got %r" % type(adapter))
        self.adapter = adapter

    def evaluate(self, state, questions):
        return self.adapter.evaluate(state, questions)


class JevError(Exception):
    """Non-retryable provider failure (bad key, invalid request, quota...)."""


class JevRetryable(Exception):
    """Transient provider failure (429/408/5xx); carries an optional Retry-After delay."""

    def __init__(self, message, retry_after=None):
        Exception.__init__(self, message)
        self.retry_after = retry_after
