"""Shared plumbing for jev adapters: keep-alive HTTP transport and settings merge.

Lives in the adapter layer (not in the core): both the native TypeSafe adapter and the
OpenRouter adapter reuse it, so the core keeps its single universal ``client.evaluate(...)`` call.
"""
import http.client
import json
import os
import socket
import ssl
import time


def merged(section, gucs, key, default=None):
    """Setting precedence: session GUC > configs/jevs.yml section > built-in default."""
    v = gucs.get(key)
    if v not in (None, ""):
        return v
    v = section.get(key)
    if v not in (None, ""):
        return v
    return default


class HttpTransport(object):
    """Pooled keep-alive connections keyed by (scheme, host, port). Thread-safe pool.

    Connections are handed out with borrow()/return_conn() around a single request; stale
    idle sockets are detected before reuse so nothing is ever sent into a dead connection.
    """

    _ssl_ctx = None

    def __init__(self, timeout=30.0, keepalive=600.0, max_idle=16):
        self.timeout = float(timeout)
        self.keepalive = float(keepalive)
        self.max_idle = int(max_idle)
        self._lock = __import__("threading").Lock()
        self._idle = {}                  # (scheme, host, port) -> [(conn, last_used)]

    @classmethod
    def _context(cls):
        if HttpTransport._ssl_ctx is None:
            HttpTransport._ssl_ctx = ssl.create_default_context()
        return HttpTransport._ssl_ctx

    @staticmethod
    def _alive(c):
        sock = c.sock
        if sock is None:
            return False
        import select
        try:
            readable, _, _ = select.select([sock], [], [], 0)
            return not readable          # readability on an idle keep-alive socket means EOF/close
        except (OSError, ValueError):
            return False

    def borrow(self, target):
        """target: dict(scheme, host, port). Returns (conn, reused)."""
        key = (target["scheme"], target["host"], target["port"])
        while True:
            with self._lock:
                entry = self._idle.setdefault(key, []).pop() if self._idle.get(key) else None
            if entry is None:
                break
            c, last_used = entry
            if time.time() - last_used < self.keepalive and self._alive(c):
                return c, True
            c.close()
        if target["scheme"] == "https":
            c = http.client.HTTPSConnection(key[1], key[2], timeout=self.timeout, context=self._context())
        else:
            c = http.client.HTTPConnection(key[1], key[2], timeout=self.timeout)
        return c, False

    def return_conn(self, target, c, reusable):
        key = (target["scheme"], target["host"], target["port"])
        if not reusable:
            c.close()
            return
        with self._lock:
            idle = self._idle.setdefault(key, [])
            if len(idle) < self.max_idle:
                idle.append((c, time.time()))
                return
        c.close()

    def post_json(self, target, path, payload, headers):
        """One POST of a JSON body; returns (status, header_getter, parsed_json_bytes).

        Network errors propagate as OSError/http.client.HTTPException so callers can retry.
        """
        body = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        c, reused = self.borrow(target)
        try:
            if not reused:
                c.connect()
                self._tcp_keepalive(c)
            c.request("POST", path, body=body, headers=headers)
            resp = c.getresponse()
            raw = resp.read()
            status, will_close, getheader = resp.status, resp.will_close, resp.getheader
        except (http.client.HTTPException, OSError):
            c.close()
            raise
        self.return_conn(target, c, not will_close)
        return status, getheader, raw

    @staticmethod
    def _tcp_keepalive(c):
        try:
            sock = c.sock
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            for name, value in (("TCP_KEEPIDLE", 30), ("TCP_KEEPINTVL", 10), ("TCP_KEEPCNT", 3)):
                if hasattr(socket, name):
                    sock.setsockopt(socket.IPPROTO_TCP, getattr(socket, name), value)
        except OSError:
            pass


def retry_after_seconds(getheader):
    v = getheader("retry-after-ms") if getheader else None
    if v and v.strip().isdigit():
        return int(v) / 1000.0
    v = getheader("retry-after") if getheader else None
    if v:
        try:
            return float(v)
        except ValueError:
            pass
    return None


RETRYABLE_STATUS = (408, 429, 500, 502, 503, 529)


def resolve_key(gucs, section, env_defaults):
    """API key precedence: jev.api_key GUC > api_key_env named in jevs.yml > env fallbacks.

    ``env_defaults`` is the ordered list of environment variables this adapter may read —
    decided by the adapter itself (the factory passes only the merged settings), keeping the
    choice of credential source inside the provider's own module.
    """
    key = gucs.get("api_key") or section.get("api_key")
    if key:
        return key
    var = section.get("api_key_env")
    if var:
        return os.environ.get(var)
    for name in env_defaults:
        v = os.environ.get(name)
        if v:
            return v
    return None
