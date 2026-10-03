"""Native TypeSafe SystemOne adapter — the default provider for jev.

Speaks the API's own wire format (one shared state + N questions per request) and returns the
response in the normalised shape of the universal interface unchanged.
"""
import json
from urllib.parse import urlsplit

from jev.interface import JevAdapter, JevError, JevRetryable
from jev.adapters._common import HttpTransport, merged, retry_after_seconds, resolve_key, RETRYABLE_STATUS

DEFAULT_URL = "https://api.typesafe.ai/v1/systemone"


class TypeSafeAdapter(JevAdapter):
    name = "typesafe"
    ENV_KEYS = ("JEV_API_KEY", "TYPESAFE_API_KEY")   # credential sources owned by this provider

    def __init__(self, api_url, model, timeout, keepalive, concurrency, api_key):
        url = urlsplit(api_url or DEFAULT_URL)
        self.target = {"scheme": url.scheme, "host": url.hostname, "port": url.port}
        self.path = (url.path or "/") + ("?" + url.query if url.query else "")
        self.model = model or "jev-latest"
        self.api_key = api_key
        self.transport = HttpTransport(timeout=timeout, keepalive=keepalive, max_idle=concurrency)

    @classmethod
    def from_settings(cls, section, gucs):
        api_key = resolve_key(gucs, section, cls.ENV_KEYS)
        if not api_key:
            raise JevError("jev: no API key for provider 'typesafe'. SET jev.api_key = '...', "
                           "export TYPESAFE_API_KEY (or JEV_API_KEY), or point jevs.yml at another provider.")
        return cls(
            api_url=merged(section, gucs, "api_url", DEFAULT_URL),
            model=merged(section, gucs, "model", "jev-latest"),
            timeout=float(merged(section, gucs, "timeout", 30)),
            keepalive=float(merged(section, gucs, "keepalive", 600)),
            concurrency=max(1, int(merged(section, gucs, "concurrency", 16))),
            api_key=api_key,
        )

    def evaluate(self, state, questions):
        """POST {model, state, questions}; the response already matches the universal shape."""
        payload = json.dumps({"model": self.model, "state": state, "questions": questions}).encode()
        headers = {"Authorization": "Bearer " + self.api_key, "Content-Type": "application/json",
                   "User-Agent": "pg-jev/0.2.0"}
        status, getheader, raw = self.transport.post_json(self.target, self.path, payload, headers)
        if status == 200:
            data = json.loads(raw.decode())
            data["_ms"] = 0.0
            return data
        snippet = "%s %s" % (status, raw.decode(errors="replace")[:300])
        if status in RETRYABLE_STATUS:
            raise JevRetryable("jev: provider error " + snippet, retry_after=retry_after_seconds(getheader))
        raise JevError("jev: provider error " + snippet)
