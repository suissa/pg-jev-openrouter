-- Upgrade jev 0.1.0 -> 0.2.0. Generated from sql/jev--0.2.0.sql: every object is CREATE OR REPLACE'd in place.
-- jev 0.2.0 — natural-language predicates for PostgreSQL, powered by TypeSafe's Jev.
--
--   SELECT * FROM people WHERE jev(people, 'name is european');
--   SELECT name, jev_prob(people, 'works in healthcare') AS p FROM people;
--   SELECT name, jev_score(products, 'how luxurious is this product', ARRAY['budget','mid-range','luxury']) FROM products;
--   SELECT subject, jev_choice(support_tickets, 'which team should handle this', ARRAY['billing','technical','sales']) FROM support_tickets;
--
-- No index is needed. Rows are judged by the model, not matched by pattern.
--
-- How a statement runs: the first call for a table + question starts a read-ahead that streams the
-- table in physical order (pages of rows, constant memory, any table size), packs jev.batch_size rows
-- into one API request (one shared state, one question per row) and keeps jev.concurrency requests in
-- flight over persistent HTTPS connections. Rows are answered as their batch returns, so the executor
-- never waits for the whole table, a LIMIT stops the read-ahead early, and rows that other predicates
-- filter out before jev() is called are skipped rather than judged. Answers are cached per row content
-- for the session, so re-running a query, changing the threshold or sorting by probability is free.
--
-- Settings (SET jev.<name> = ...):
--   jev.config_file        provider config YAML, default 'configs/jevs.yml' (also $JEV_CONFIG); its
--                          'jev.provider' selects the adapter (typesafe | openrouter) via JevFactory —
--                          the core itself never knows which provider is in use
--   jev.api_key            provider API key (falls back to JEV_API_KEY, then TYPESAFE_API_KEY /
--                          OPENROUTER_API_KEY (or OPENROUTER_API), or api_key_env from jevs.yml)
--   jev.model              default 'jev-latest' (native provider; overridden by jevs.yml / the adapter's own default)
--   jev.threshold          default 0.5   probability at which jev() returns true
--   jev.batch_size         default 20    rows per API request (accuracy drops measurably above ~20-25 rows)
--   jev.concurrency        default 16    parallel API requests
--   jev.max_prefetch_rows  default 5000  how far past a cache miss the read-ahead scans to find the row, and
--                                        how many skipped rows it keeps for later (memory bound)
--   jev.notices            default 'on'  emit progress NOTICEs and a summary per table read-ahead
--   jev.api_url            native provider endpoint; default 'https://api.typesafe.ai/v1/systemone'
--                         (proxies, mocks, tests). OpenRouter uses base_url from configs/jevs.yml.
--   jev.timeout            default 30    seconds per API request (adapters may raise it: OpenRouter 60)
--   jev.keepalive          default 600   seconds a pooled API connection may sit idle before it is reconnected
--   jev.max_rows_per_statement   default 0 (off)  never send more rows than this to the API in one statement
--   jev.max_chars_per_statement  default 0 (off)  never send more characters of row data than this in one statement
--                                                 Both are spend guards for shared or public deployments.

-- \echo Use "ALTER EXTENSION jev UPDATE" to load this file. \quit

CREATE OR REPLACE FUNCTION _jev_eval(rel_type text, row_json text, query text, kind text, options text)
RETURNS jsonb
LANGUAGE plpython3u
STABLE
AS $py$
import json, os, time, hashlib, threading, random, sys
from collections import deque
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

USD_PER_INPUT_TOKEN = 0.042 / 1_000_000  # jev-1.13 list price; output tokens are free
PAGE_ROWS = 1000                          # rows read from the table per SPI query
STATE_VERSION = 3                         # 3: provider layer (interface + adapters + JevFactory)

# ---------------------------------------------------------------- session state (survives across calls)
if GD.get("jev", {}).get("version") != STATE_VERSION:
    GD["jev"] = {
        "version": STATE_VERSION,
        "cache": {},          # cache_key -> {row_hash: answer}
        "jobs": {},           # cache_key -> read-ahead state for one relation + question
        "stats": {"requests": 0, "input_tokens": 0, "output_tokens": 0, "rows_evaluated": 0,
                  "cache_hits": 0, "api_ms": 0.0, "batches": 0, "errors": 0, "retries": 0},
        "plans": {},
        "lock": threading.Lock(),
        "pool": None, "pool_size": 0,   # ThreadPoolExecutor shared by all jobs of this session
        "client_cache": {},             # settings fingerprint -> universal JevClient (from JevFactory)
        "stmt": {"ts": None, "rows": 0, "chars": 0},   # per-statement spend guard
    }
S = GD["jev"]
LOCK = S["lock"]

def plan(name, sql, types):
    p = S["plans"].get(name)
    if p is None:
        p = S["plans"][name] = plpy.prepare(sql, types)
    return p

# ---------------------------------------------------------------- settings: one SPI round trip per cache miss
CFG_SQL = """SELECT statement_timestamp()::text AS ts,
  current_setting('jev.api_key', true) AS api_key, current_setting('jev.model', true) AS model,
  current_setting('jev.batch_size', true) AS batch_size, current_setting('jev.concurrency', true) AS concurrency,
  current_setting('jev.max_prefetch_rows', true) AS max_prefetch_rows, current_setting('jev.notices', true) AS notices,
  current_setting('jev.api_url', true) AS api_url, current_setting('jev.timeout', true) AS timeout,
  current_setting('jev.keepalive', true) AS keepalive,
  current_setting('jev.config_file', true) AS config_file,
  current_setting('jev.max_rows_per_statement', true) AS max_rows, current_setting('jev.max_chars_per_statement', true) AS max_chars"""

def load_cfg():
    r = plpy.execute(plan("cfg", CFG_SQL, []))[0]
    def g(name, default):
        v = r[name]
        return default if v in (None, "") else v
    return {
        "ts": r["ts"],
        "api_key": g("api_key", None), "model": g("model", None),
        "batch_size": max(1, int(g("batch_size", "20"))), "concurrency": max(1, int(g("concurrency", "16"))),
        "max_prefetch": max(1, int(g("max_prefetch_rows", "5000"))),
        "notices": g("notices", "on").lower() in ("on", "true", "1", "yes"),
        "timeout": g("timeout", None), "keepalive": g("keepalive", None),
        "max_rows": int(g("max_rows", "0")), "max_chars": int(g("max_chars", "0")),
        "api_url": g("api_url", None), "config_file": g("config_file", None),
    }

# ---------------------------------------------------------------- provider layer: universal interface + adapters + JevFactory
# The core below only ever talks to a JevClient built by JevFactory from the parsed configs/jevs.yml.
# It never references a provider name or an adapter class; which provider serves the requests is
# entirely the factory's decision. Source of record: lib/jev/ (interface.py, config.py, factory.py,
# adapters/{_common,typesafe,openrouter}.py); the embedded fallback between the markers below is
# kept in sync with it.
# The package lives beside the installed SQL (share/jev -> ../lib/jev) or in the repo checkout.
JEV_LIB_PATHS = []
try:
    _sharedir = plpy.execute(plan("sharedir", "SELECT setting AS d FROM pg_config WHERE name = 'SHAREDIR'", []))[0]["d"]
    JEV_LIB_PATHS.append(os.path.join(_sharedir, "jev", "lib"))
except Exception:
    pass
JEV_LIB_PATHS.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "lib"))
for _p in [x for x in os.environ.get("JEV_LIB", "").split(os.pathsep) if x] + JEV_LIB_PATHS:
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from jev.interface import JevError, JevRetryable, JevAdapter, JevClient
    from jev.config import parse as _jev_parse
    from jev.factory import JevFactory
except ImportError:
    # >>> BEGIN EMBEDDED jev provider layer (mirror of lib/jev; do not edit one side without the other)

    class JevError(Exception):
        """Non-retryable provider failure (bad key, invalid request, quota...)."""

    class JevRetryable(Exception):
        """Transient provider failure (429/408/5xx); carries an optional Retry-After delay."""
        def __init__(self, message, retry_after=None):
            Exception.__init__(self, message)
            self.retry_after = retry_after

    class JevAdapter(object):
        """Transport + translation for one provider. Implementations must be thread-safe."""
        name = "abstract"
        def evaluate(self, state, questions):
            raise NotImplementedError("adapter %s does not implement evaluate()" % self.name)

    class JevClient(object):
        """Universal handle the core holds: delegates straight to the injected adapter."""
        def __init__(self, adapter):
            if not isinstance(adapter, JevAdapter):
                raise TypeError("JevClient needs a JevAdapter, got %r" % type(adapter))
            self.adapter = adapter
        def evaluate(self, state, questions):
            return self.adapter.evaluate(state, questions)

    import http.client, socket, ssl, select
    from urllib.parse import urlsplit

    def _merged(section, gucs, key, default=None):
        """Setting precedence: session GUC > configs/jevs.yml section > built-in default."""
        v = gucs.get(key)
        if v not in (None, ""):
            return v
        v = section.get(key)
        if v not in (None, ""):
            return v
        return default

    def _retry_after_seconds(getheader):
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

    _RETRYABLE_STATUS = (408, 429, 500, 502, 503, 529)

    def _resolve_key(gucs, section, env_defaults):
        """API key: jev.api_key GUC > yml api_key > api_key_env variable > adapter's own env fallbacks."""
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

    class HttpTransport(object):
        """Pooled keep-alive connections keyed by (scheme, host, port). Thread-safe pool."""
        _ssl_ctx = None
        def __init__(self, timeout=30.0, keepalive=600.0, max_idle=16):
            self.timeout = float(timeout)
            self.keepalive = float(keepalive)
            self.max_idle = int(max_idle)
            self._lock = threading.Lock()
            self._idle = {}                    # (scheme, host, port) -> [(conn, last_used)]
        @classmethod
        def _context(cls):
            if HttpTransport._ssl_ctx is None:
                HttpTransport._ssl_ctx = ssl.create_default_context()   # CA bundle once per backend
            return HttpTransport._ssl_ctx
        @staticmethod
        def _alive(c):
            """An idle keep-alive connection never has unread data: readability means EOF or TLS close."""
            sock = c.sock
            if sock is None:
                return False
            try:
                readable, _, _ = select.select([sock], [], [], 0)
                return not readable
            except (OSError, ValueError):
                return False
        def borrow(self, target):
            key = (target["scheme"], target["host"], target["port"])
            while True:
                with LOCK:
                    lst = self._idle.setdefault(key, [])
                    entry = lst.pop() if lst else None
                if entry is None:
                    break
                c, last_used = entry
                if time.time() - last_used < self.keepalive and self._alive(c):
                    return c, True
                c.close()                      # idle too long or closed by the server: never send into a dead socket
            if target["scheme"] == "https":
                c = http.client.HTTPSConnection(key[1], key[2], timeout=self.timeout, context=self._context())
            else:
                c = http.client.HTTPConnection(key[1], key[2], timeout=self.timeout)
            return c, False
        def return_conn(self, target, c, reusable):
            key = (target["scheme"], target["host"], target["port"])
            if not reusable:
                c.close(); return
            with LOCK:
                idle = self._idle.setdefault(key, [])
                if len(idle) < self.max_idle:
                    idle.append((c, time.time())); return
            c.close()
        def post_json(self, target, path, payload, headers):
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
            """Kernel notices a dropped peer in ~1 min so pooled conns fail fast instead of stalling until timeout."""
            try:
                sock = c.sock
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                for name, value in (("TCP_KEEPIDLE", 30), ("TCP_KEEPINTVL", 10), ("TCP_KEEPCNT", 3)):
                    if hasattr(socket, name):
                        sock.setsockopt(socket.IPPROTO_TCP, getattr(socket, name), value)
            except OSError:
                pass

    _TS_DEFAULT_URL = "https://api.typesafe.ai/v1/systemone"

    class TypeSafeAdapter(JevAdapter):
        """Native TypeSafe SystemOne wire format; response already matches the normalised shape."""
        name = "typesafe"
        ENV_KEYS = ("JEV_API_KEY", "TYPESAFE_API_KEY")
        def __init__(self, target, path, model, api_key, transport):
            self.target, self.path, self.model, self.api_key, self.transport = target, path, model, api_key, transport
        @classmethod
        def from_settings(cls, section, gucs):
            api_key = _resolve_key(gucs, section, cls.ENV_KEYS)
            if not api_key:
                raise JevError("jev: no API key for provider 'typesafe'. SET jev.api_key = '...', "
                               "export TYPESAFE_API_KEY (or JEV_API_KEY), or point jevs.yml at another provider.")
            url = urlsplit(_merged(section, gucs, "api_url", _TS_DEFAULT_URL))
            transport = HttpTransport(timeout=float(_merged(section, gucs, "timeout", 30)),
                                      keepalive=float(_merged(section, gucs, "keepalive", 600)),
                                      max_idle=max(1, int(_merged(section, gucs, "concurrency", 16))))
            return cls({"scheme": url.scheme, "host": url.hostname, "port": url.port},
                       (url.path or "/") + ("?" + url.query if url.query else ""),
                       _merged(section, gucs, "model", "jev-latest"), api_key, transport)
        def evaluate(self, state, questions):
            payload = json.dumps({"model": self.model, "state": state, "questions": questions}).encode()
            headers = {"Authorization": "Bearer " + self.api_key, "Content-Type": "application/json",
                       "User-Agent": "pg-jev/0.2.0"}
            status, getheader, raw = self.transport.post_json(self.target, self.path, payload, headers)
            if status == 200:
                data = json.loads(raw.decode())
                data["_ms"] = 0.0
                return data
            snippet = "%s %s" % (status, raw.decode(errors="replace")[:300])
            if status in _RETRYABLE_STATUS:
                raise JevRetryable("jev: provider error " + snippet, retry_after=_retry_after_seconds(getheader))
            raise JevError("jev: provider error " + snippet)

    _OR_DEFAULT_BASE = "https://openrouter.ai/api/v1/chat/completions"
    _OR_DEFAULT_MODEL = "deepseek/deepseek-chat-v3.1"

    def _or_render(state, questions):
        """One prompt carrying the shared state plus every question; answers keyed by question id."""
        lines = ["You are Jev, a precise evaluator. Judge each QUESTION against the RECORDS.",
                 "", "RECORDS (JSON array, referenced as rows[i]):", json.dumps(state.get("rows", []))]
        cond = state.get("condition")
        if cond:
            lines += ["", "Shared condition for every yes/no question: %r" % cond]
        lines += ["", "QUESTIONS (answer every id):"]
        schema_parts = []
        for qid, q in sorted(questions.items()):
            kind = q["type"]
            instr = q.get("instructions", "")
            if kind == "noul":
                lines.append("%s: %s -> answer the probability (0..1) that it holds" % (qid, instr))
                schema_parts.append('"%s": {"type":"noul","noul":<p>}' % qid)
            elif kind == "score":
                levels = q.get("criteria") or []
                legend = ", ".join("%d=%s" % (i, l) for i, l in enumerate(levels))
                lines.append("%s: %s -> rate on the ordered scale (%s); score = probability-weighted "
                             "index (0..%g)" % (qid, instr, legend, len(levels) - 1))
                schema_parts.append('"%s": {"type":"score","score":<x>,"probabilities":{"<idx>":<p>,...},'
                                    '"confidence":<c>}' % qid)
            elif kind == "choice":
                opts = list((q.get("criteria") or {}).keys())
                lines.append("%s: %s -> pick exactly one of %s" % (qid, instr, json.dumps(opts)))
                schema_parts.append('"%s": {"type":"choice","choice":"<option>","probabilities":{...},'
                                    '"confidence":<c>}' % qid)
            else:
                raise JevError("jev: OpenRouter adapter got unknown question kind %r" % kind)
        lines += ["", "Reply with ONLY a JSON object, nothing else:", "{ " + ", ".join(schema_parts) + " }",
                  "Probabilities must be numbers between 0 and 1."]
        return "\n".join(lines)

    class OpenRouterAdapter(JevAdapter):
        """Jev over OpenRouter chat completions; replies normalised to the universal answer shape."""
        name = "openrouter"
        ENV_KEYS = ("JEV_API_KEY", "OPENROUTER_API_KEY", "OPENROUTER_API")
        def __init__(self, target, path, model, api_key, extra_headers, transport):
            self.target, self.path, self.model = target, path, model
            self.headers = {"Authorization": "Bearer " + api_key, "Content-Type": "application/json"}
            self.headers.update(extra_headers)
            self.transport = transport
        @classmethod
        def from_settings(cls, section, gucs):
            per = {}
            adapters = section.get("adapters")
            if isinstance(adapters, dict) and isinstance(adapters.get(cls.name), dict):
                per = dict(adapters[cls.name])
            merged_sec = dict(per)
            merged_sec.update({k: v for k, v in section.items() if k != "adapters"})
            api_key = _resolve_key(gucs, merged_sec, cls.ENV_KEYS)
            if not api_key:
                raise JevError("jev: no API key for provider 'openrouter'. Export OPENROUTER_API_KEY "
                               "(or set api_key_env in configs/jevs.yml, or SET jev.api_key).")
            url = urlsplit(_merged(merged_sec, gucs, "base_url", _OR_DEFAULT_BASE))
            transport = HttpTransport(timeout=float(_merged(merged_sec, gucs, "timeout", 60)),
                                      keepalive=float(_merged(merged_sec, gucs, "keepalive", 600)),
                                      max_idle=max(1, int(_merged(merged_sec, gucs, "concurrency", 16))))
            extra = {}
            referer = _merged(merged_sec, gucs, "http_referer", None)
            title = _merged(merged_sec, gucs, "app_title", "pg-jev")
            if referer:
                extra["HTTP-Referer"] = referer
            if title:
                extra["X-Title"] = title
            return cls({"scheme": url.scheme, "host": url.hostname, "port": url.port},
                       (url.path or "/") + ("?" + url.query if url.query else ""),
                       _merged(merged_sec, gucs, "model", _OR_DEFAULT_MODEL), api_key, extra, transport)
        def evaluate(self, state, questions):
            payload = json.dumps({"model": self.model,
                                  "messages": [{"role": "user", "content": _or_render(state, questions)}],
                                  "temperature": 0, "max_tokens": 4096}).encode()
            status, getheader, raw = self.transport.post_json(self.target, self.path, payload, self.headers)
            if status == 200:
                return self._normalise(json.loads(raw.decode()), questions)
            snippet = "%s %s" % (status, raw.decode(errors="replace")[:300])
            if status in _RETRYABLE_STATUS:
                raise JevRetryable("jev: OpenRouter error " + snippet, retry_after=_retry_after_seconds(getheader))
            raise JevError("jev: OpenRouter error " + snippet)
        def _normalise(self, data, questions):
            err = data.get("error")
            if err:
                raise JevError("jev: OpenRouter returned an error: %s" % json.dumps(err)[:300])
            try:
                content = data["choices"][0]["message"]["content"]
            except (KeyError, IndexError, TypeError):
                raise JevError("jev: OpenRouter response has no message content")
            obj = self._extract_json(content)
            usage = data.get("usage") or {}
            answers = {}
            for qid, q in questions.items():
                a = obj.get(qid)
                if not isinstance(a, dict):
                    raise JevError("jev: OpenRouter answer is missing %r" % qid)
                answers[qid] = self._coerce(qid, q, a)
            out = {"model": data.get("model", self.model), "answers": answers,
                   "usage": {"input_tokens": usage.get("prompt_tokens", 0),
                             "output_tokens": usage.get("completion_tokens", 0)}}
            out["_ms"] = 0.0
            return out
        @staticmethod
        def _extract_json(text):
            start, end = text.find("{"), text.rfind("}")
            if start == -1 or end <= start:
                raise JevError("jev: OpenRouter model did not return a JSON object: %r" % text[:200])
            try:
                return json.loads(text[start:end + 1])
            except ValueError as e:
                raise JevError("jev: OpenRouter model returned invalid JSON: %s" % e)
        @staticmethod
        def _coerce(qid, q, a):
            kind = q["type"]
            if kind == "noul":
                try:
                    p = float(a.get("noul", a.get("probability")))
                except (TypeError, ValueError):
                    raise JevError("jev: OpenRouter answer %r is not a probability" % qid)
                return {"type": "noul", "noul": min(max(p, 0.0), 1.0)}
            if kind == "score":
                n = len(q.get("criteria") or [])
                try:
                    score = float(a.get("score"))
                except (TypeError, ValueError):
                    raise JevError("jev: OpenRouter answer %r has no numeric score" % qid)
                probs = {str(i): float(a.get("probabilities", {}).get(str(i), 0.0)) for i in range(n)} \
                    if isinstance(a.get("probabilities"), dict) else {}
                return {"type": "score", "score": min(max(score, 0.0), max(n - 1.0, 0.0)),
                        "legend": {str(i): l for i, l in enumerate(q["criteria"])},
                        "probabilities": probs, "confidence": float(a.get("confidence", 0.0))}
            opts = list((q.get("criteria") or {}).keys())
            choice = a.get("choice")
            if choice not in opts:
                raise JevError("jev: OpenRouter choice %r is not one of the options for %r" % (choice, qid))
            probs = a.get("probabilities") if isinstance(a.get("probabilities"), dict) else {}
            return {"type": "choice", "choice": choice, "probabilities": probs,
                    "confidence": float(a.get("confidence", 0.0))}

    _JEV_ADAPTERS = {"typesafe": TypeSafeAdapter, "openrouter": OpenRouterAdapter}

    def _jev_parse(raw_text):
        """Parse jevs.yml contents into the 'jev' section dict (YAML via PyYAML, else JSON)."""
        try:
            import yaml as _y
        except ImportError:
            _y = None
        doc = (_y.safe_load(raw_text) if _y is not None else json.loads(raw_text)) or {}
        if not isinstance(doc, dict):
            raise ValueError("jev: config root must be a mapping with a 'jev' section")
        sec = doc.get("jev") or {}
        if not isinstance(sec, dict):
            raise ValueError("jev: 'jev:' section of the config must be a mapping")
        return sec

    class JevFactory(object):
        """Turns the parsed configs/jevs.yml object into a universal JevClient. The only code that
        knows provider names exist; the core just calls factory.client(gucs).evaluate(...)."""
        def __init__(self, section=None):
            self.section = section or {}
            name = (self.section.get("provider") or "typesafe").strip().lower()
            dotted = self.section.get("adapter_class")
            if dotted:                       # escape hatch: fully qualified adapter class in the yml
                mod, _, attr = dotted.rpartition(".")
                cls = getattr(__import__(mod, fromlist=[attr]), attr)
            else:
                cls = _JEV_ADAPTERS.get(name)
                if cls is None:
                    raise JevError("jev: unknown jev.provider %r in configs/jevs.yml (known: %s)"
                                   % (name, ", ".join(sorted(_JEV_ADAPTERS))))
            self.provider_name, self.adapter_cls = name, cls
        @classmethod
        def from_config(cls, section=None):
            return cls(section)
        def client(self, gucs=None):
            return JevClient(self.adapter_cls.from_settings(self.section, gucs or {}))

    # <<< END EMBEDDED jev provider layer

# ---------------------------------------------------------------- evaluation through the universal interface
def build_payload(rows):
    """The provider-neutral request: one shared state + one question per row (the universal shape)."""
    state = {"condition": query, "rows": rows} if kind == "noul" else {"rows": rows}
    return state, {("r%d" % i): build_question(i) for i in range(len(rows))}

def call_api(client, rows):
    """Judge a batch through the JevClient the JevFactory built. Provider-agnostic: the core only
    knows the universal evaluate(state, questions) contract, never which adapter answers."""
    payload = build_payload(rows)
    delay, last = 0.5, None
    for attempt in range(7):
        t0 = time.time()
        try:
            data = client.evaluate(*payload)
        except JevRetryable as e:
            last = str(e)
            with LOCK:
                S["stats"]["retries"] += 1
            wait = getattr(e, "retry_after", None)
            time.sleep(min(wait if wait is not None else delay, 30) + random.random() * 0.25)
            delay = min(delay * 2, 8)
            continue
        except (JevError, RuntimeError, ValueError) as e:
            raise RuntimeError(str(e))           # non-retryable provider failure
        except Exception as e:                   # transport hiccup outside the adapter: retry
            last = "%s: %s" % (type(e).__name__, e)
            with LOCK:
                S["stats"]["retries"] += 1
            time.sleep(delay + random.random() * 0.25); delay = min(delay * 2, 8)
            continue
        data["_ms"] = (time.time() - t0) * 1000
        return data
    raise RuntimeError("jev: provider unreachable after retries: " + str(last))

def run_batch(client, cfg, job, bucket, pairs):
    """Worker thread: judge one batch through the universal client. Returns rows judged."""
    try:
        data = call_api(client, [json.loads(t) for _, t in pairs])
    except Exception:
        with LOCK:
            S["stats"]["errors"] += 1
        raise
    answers = data.get("answers") or {}
    if any(("r%d" % i) not in answers for i in range(len(pairs))):
        with LOCK:
            S["stats"]["errors"] += 1
        raise RuntimeError("jev: provider response is missing answers (%d of %d)" % (len(answers), len(pairs)))
    usage = data.get("usage", {})
    with LOCK:
        for i, (h, _) in enumerate(pairs):
            bucket[h] = answers["r%d" % i]
        st = S["stats"]
        st["requests"] += 1
        st["input_tokens"] += usage.get("input_tokens", 0)
        st["output_tokens"] += usage.get("output_tokens", 0)
        st["rows_evaluated"] += len(pairs)
        st["api_ms"] += data["_ms"]
        if job is not None:
            job["done_reqs"] += 1
            job["done_rows"] += len(pairs)
            job["tokens"] += usage.get("input_tokens", 0)
    return len(pairs)

def pool(cfg):
    if S["pool"] is None or S["pool_size"] != cfg["concurrency"]:
        if S["pool"] is not None:
            S["pool"].shutdown(wait=False)
        S["pool"] = ThreadPoolExecutor(max_workers=cfg["concurrency"], thread_name_prefix="jev")
        S["pool_size"] = cfg["concurrency"]
    return S["pool"]

# ---------------------------------------------------------------- spend guard
def guard(cfg, pairs):
    """Refuse to send a batch that would push this statement over the configured limits."""
    st = S["stmt"]
    if st["ts"] != cfg["ts"]:
        st["ts"], st["rows"], st["chars"] = cfg["ts"], 0, 0
    rows, chars = st["rows"] + len(pairs), st["chars"] + sum(len(t) for _, t in pairs)
    if cfg["max_rows"] and rows > cfg["max_rows"]:
        plpy.error("jev: this statement would send %d rows to the API, above jev.max_rows_per_statement = %d"
                   % (rows, cfg["max_rows"]))
    if cfg["max_chars"] and chars > cfg["max_chars"]:
        plpy.error("jev: this statement would send %d characters of row data to the API, above jev.max_chars_per_statement = %d"
                   % (chars, cfg["max_chars"]))
    st["rows"], st["chars"] = rows, chars

# ---------------------------------------------------------------- waiting that stays cancellable
def wait_for(fut):
    """Block on a request; poke SPI every 250 ms so statement_timeout and cancel requests get through."""
    while True:
        try:
            return fut.result(timeout=0.25)
        except FutureTimeout:
            plpy.execute(plan("noop", "SELECT 1", []))

# ---------------------------------------------------------------- read-ahead job: streams one relation in physical order
REL_SQL = """SELECT c.oid::regclass::text AS rel, c.relkind, c.reltuples,
  pg_relation_size(c.oid) / current_setting('block_size')::int AS nblocks
  FROM pg_class c WHERE c.oid = to_regclass($1)"""

def new_job(cfg):
    job = {"cfg": cfg, "rel": None, "mode": None, "futures": [], "inflight": {}, "fetched": deque(),
           "skipped": deque(), "skipped_map": {}, "exhausted": True, "restart_ts": cfg["ts"],
           "done_reqs": 0, "done_rows": 0, "tokens": 0, "reported_reqs": 0, "summary_done": False, "t0": time.time(),
           "est_rows": 0, "block": 0, "nblocks": 0, "rows_per_block": 50.0, "offset": 0}
    info = plpy.execute(plan("rel", REL_SQL, ["text"]), [rel_type])
    if info.nrows():
        r = info[0]
        if r["relkind"] in ("r", "m"):
            job.update(rel=r["rel"], mode="ctid", nblocks=int(r["nblocks"]), exhausted=False)
            if r["reltuples"] and r["reltuples"] > 0:
                job["est_rows"] = int(r["reltuples"])
                job["rows_per_block"] = max(1.0, r["reltuples"] / max(1, job["nblocks"]))
        elif r["relkind"] in ("v", "p", "f"):
            job.update(rel=r["rel"], mode="offset", exhausted=False)
        if job["rel"] and not job["est_rows"]:
            job["est_rows"] = plpy.execute("SELECT count(*) AS n FROM %s" % job["rel"])[0]["n"]
    return job

def restart(job, cfg):
    job.update(cfg=cfg, exhausted=False, restart_ts=cfg["ts"], block=0, offset=0, t0=time.time(),
               done_reqs=0, done_rows=0, tokens=0, reported_reqs=0, summary_done=False)
    job["fetched"].clear()

def fetch_page(job):
    """Append the next page of (row_hash, row_json) pairs to job['fetched']; sets 'exhausted' at the end."""
    if job["exhausted"]:
        return 0
    if job["mode"] == "ctid":
        if job["block"] >= job["nblocks"]:
            job["exhausted"] = True
            return 0
        blocks = max(1, min(job["nblocks"] - job["block"], int(PAGE_ROWS / job["rows_per_block"]) + 1))
        lo, hi = job["block"], job["block"] + blocks
        rows = plpy.execute(plan("page:" + job["rel"],
                                 "SELECT to_json(t)::text AS r FROM %s t WHERE ctid >= $1 AND ctid < $2" % job["rel"],
                                 ["tid", "tid"]), ["(%d,0)" % lo, "(%d,0)" % hi])
        job["block"] = hi
        if rows.nrows():
            job["rows_per_block"] = max(1.0, 0.7 * job["rows_per_block"] + 0.3 * rows.nrows() / blocks)
    else:
        rows = plpy.execute(plan("page:" + job["rel"],
                                 "SELECT to_json(t)::text AS r FROM %s t OFFSET $1 LIMIT $2" % job["rel"],
                                 ["bigint", "bigint"]), [job["offset"], PAGE_ROWS])
        job["offset"] += rows.nrows()
        if rows.nrows() < PAGE_ROWS:
            job["exhausted"] = True
    for r in rows:
        t = r["r"]
        job["fetched"].append((hashlib.sha1(t.encode()).hexdigest(), t))
    return rows.nrows()

def skip(job, pair):
    """Remember a row the executor passed over, so a later request for it can still be batched."""
    h, t = pair
    if h in job["skipped_map"]:
        return
    job["skipped_map"][h] = t
    job["skipped"].append(h)
    while len(job["skipped"]) > job["cfg"]["max_prefetch"]:
        job["skipped_map"].pop(job["skipped"].popleft(), None)

def submit(job, cfg, bucket, pairs):
    pairs = [p for p in pairs if p[0] not in bucket and p[0] not in job["inflight"]]
    if not pairs:
        return None
    guard(cfg, pairs)
    fut = pool(cfg).submit(run_batch, jev_client(cfg), cfg, job, bucket, pairs)
    fut.jev_hashes = [h for h, _ in pairs]
    job["futures"].append(fut)
    for h, _ in pairs:
        job["inflight"][h] = fut
        job["skipped_map"].pop(h, None)
    return fut

def top_up(job, cfg, bucket):
    """Keep up to 2 x concurrency requests in flight, in physical order, without exceeding the page buffer."""
    cap = 2 * cfg["concurrency"]
    while len(job["futures"]) < cap:
        while len(job["fetched"]) < cfg["batch_size"] and fetch_page(job):
            pass                                    # full batches across page boundaries
        if not job["fetched"]:
            return
        batch = [job["fetched"].popleft() for _ in range(min(cfg["batch_size"], len(job["fetched"])))]
        submit(job, cfg, bucket, batch)

def locate(job, cfg, h):
    """Scan forward from the read-ahead position until row h is at the head of job['fetched'].
    Rows passed over are kept in 'skipped'. Returns False when h is not within cfg['max_prefetch'] rows."""
    scanned = 0
    while True:
        while job["fetched"]:
            if job["fetched"][0][0] == h:
                return True
            skip(job, job["fetched"].popleft())
            scanned += 1
        if scanned >= cfg["max_prefetch"] or not fetch_page(job):
            return False

def sweep(job, cfg):
    """Main thread bookkeeping for finished requests: progress notices and the end-of-table summary."""
    still = []
    for fut in job["futures"]:
        if fut.done():
            for h in fut.jev_hashes:           # answered rows are in the cache; failed rows get retried on request
                job["inflight"].pop(h, None)
        else:
            still.append(fut)
    job["futures"] = still
    if not cfg["notices"] or job["rel"] is None:
        return
    if job["done_reqs"] > job["reported_reqs"]:
        job["reported_reqs"] = job["done_reqs"]
        total_reqs = max(job["done_reqs"], -(-job["est_rows"] // cfg["batch_size"]))
        plpy.notice("jev: progress %d/%d requests, %d/%d rows"
                    % (job["done_reqs"], total_reqs, job["done_rows"], max(job["done_rows"], job["est_rows"])))
    if job["exhausted"] and not job["fetched"] and not job["futures"] and not job["summary_done"] and job["done_rows"]:
        job["summary_done"] = True
        n, r, tok = job["done_rows"], job["done_reqs"], job["tokens"]
        plpy.notice("jev: %s → judged %d row%s of %s in %d request%s, %d input tokens (≈$%.4f), %.0f ms"
                    % (kind, n, "" if n == 1 else "s", job["rel"], r, "" if r == 1 else "s",
                       tok, tok * USD_PER_INPUT_TOKEN, (time.time() - job["t0"]) * 1000))

def answer(bucket, h, fut):
    try:
        wait_for(fut)
    except RuntimeError as e:
        plpy.error(str(e))
    if h not in bucket:
        plpy.error("jev: the API returned no answer for a row")
    return json.dumps(bucket[h])

# ---------------------------------------------------------------- main
if kind not in ("noul", "score", "choice"):
    plpy.error("jev: unknown kind %r" % kind)
if kind != "noul" and not isinstance(opts, list):
    plpy.error("jev: %s needs a text[] of %s" % (kind, "levels" if kind == "score" else "options"))
cache_key = json.dumps([rel_type, query, kind, opts], sort_keys=True)
bucket = S["cache"].setdefault(cache_key, {})
row_hash = hashlib.sha1(row_json.encode()).hexdigest()
job = S["jobs"].get(cache_key)

if row_hash in bucket:
    S["stats"]["cache_hits"] += 1
    if job is not None and job["futures"]:
        sweep(job, job["cfg"])
        if len(job["futures"]) < job["cfg"]["concurrency"] and (job["fetched"] or not job["exhausted"]):
            cfg = load_cfg()
            if cfg["ts"] == job["cfg"]["ts"]:       # same statement: keep the pipeline full while the executor consumes hits
                top_up(job, cfg, bucket)
    return json.dumps(bucket[row_hash])

cfg = load_cfg()
if job is None:
    job = S["jobs"][cache_key] = new_job(cfg)
else:
    job["cfg"] = cfg
    sweep(job, cfg)

fut = job["inflight"].get(row_hash)
if fut is None and job["rel"] is not None:
    if row_hash in job["skipped_map"]:
        # The executor came back for a row the read-ahead passed over (index scan, backward scan, join order):
        # judge it together with the most recently skipped rows, which are its likely neighbours.
        pairs = [(row_hash, job["skipped_map"].pop(row_hash))]
        while job["skipped"] and len(pairs) < cfg["batch_size"]:
            h = job["skipped"].pop()
            if h in job["skipped_map"]:
                pairs.append((h, job["skipped_map"].pop(h)))
        fut = submit(job, cfg, bucket, pairs)
    else:
        found = locate(job, cfg, row_hash)
        if not found and job["exhausted"] and job["restart_ts"] != cfg["ts"]:
            restart(job, cfg)                       # a new statement, and the row is not where we left off: rescan once
            found = locate(job, cfg, row_hash)
        if found:
            top_up(job, cfg, bucket)
            fut = job["inflight"].get(row_hash)
if fut is None:
    fut = submit(job, cfg, bucket, [(row_hash, row_json)])   # anonymous record, or a row the read-ahead cannot reach
    if fut is None:                                           # answered by a request that finished meanwhile
        fut = job["inflight"].get(row_hash)
        if fut is None:
            return json.dumps(bucket[row_hash])
result = answer(bucket, row_hash, fut)
sweep(job, cfg)
return result
$py$;

COMMENT ON FUNCTION _jev_eval(text, text, text, text, text) IS
  'Internal: evaluates one row (batched with its table) against a TypeSafe question. Returns the raw answer JSON.';

-- ------------------------------------------------------------------ public API

-- Full answer JSON for a row: {"type":"noul","noul":0.93} / score / choice answers.
CREATE OR REPLACE FUNCTION jev_eval(rec anyelement, query text, kind text DEFAULT 'noul', options text[] DEFAULT NULL)
RETURNS jsonb LANGUAGE sql STABLE AS $$
  SELECT _jev_eval(pg_typeof($1)::text, to_json($1)::text, $2, $3, to_json($4)::text)
$$;

-- Probability (0..1) that the row satisfies the natural-language condition.
CREATE OR REPLACE FUNCTION jev_prob(rec anyelement, query text)
RETURNS float8 LANGUAGE sql STABLE AS $$
  SELECT (_jev_eval(pg_typeof($1)::text, to_json($1)::text, $2, 'noul', NULL)->>'noul')::float8
$$;

-- Boolean predicate for WHERE clauses. Threshold: argument > jev.threshold setting > 0.5.
CREATE OR REPLACE FUNCTION jev(rec anyelement, query text, threshold float8 DEFAULT NULL)
RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT jev_prob($1, $2) >= coalesce($3, nullif(current_setting('jev.threshold', true), '')::float8, 0.5)
$$;

-- Graded rating along ordered levels; returns the probability-weighted level index (0 .. n-1).
CREATE OR REPLACE FUNCTION jev_score(rec anyelement, query text, levels text[])
RETURNS float8 LANGUAGE sql STABLE AS $$
  SELECT (_jev_eval(pg_typeof($1)::text, to_json($1)::text, $2, 'score', to_json($3)::text)->>'score')::float8
$$;

-- Same, normalised to 0..1 so it is comparable across rubrics.
CREATE OR REPLACE FUNCTION jev_score_norm(rec anyelement, query text, levels text[])
RETURNS float8 LANGUAGE sql STABLE AS $$
  SELECT jev_score($1, $2, $3) / greatest(array_length($3, 1) - 1, 1)
$$;

-- Classify each row into one option.
CREATE OR REPLACE FUNCTION jev_choice(rec anyelement, query text, options text[])
RETURNS text LANGUAGE sql STABLE AS $$
  SELECT _jev_eval(pg_typeof($1)::text, to_json($1)::text, $2, 'choice', to_json($3)::text)->>'choice'
$$;

-- Confidence (0..1) of the choice / score answer.
CREATE OR REPLACE FUNCTION jev_confidence(rec anyelement, query text, kind text, options text[])
RETURNS float8 LANGUAGE sql STABLE AS $$
  SELECT (_jev_eval(pg_typeof($1)::text, to_json($1)::text, $2, $3, to_json($4)::text)->>'confidence')::float8
$$;

-- Session statistics: requests, tokens, estimated cost, cache hits.
CREATE OR REPLACE FUNCTION jev_stats()
RETURNS jsonb LANGUAGE plpython3u STABLE AS $py$
import json
s = {"requests": 0, "input_tokens": 0, "output_tokens": 0, "rows_evaluated": 0,
     "cache_hits": 0, "api_ms": 0.0, "batches": 0, "errors": 0, "retries": 0}
jev = GD.get("jev", {})
s.update(jev.get("stats", {}))
s["estimated_cost_usd"] = round(s.get("input_tokens", 0) * 0.042 / 1_000_000, 6)
s["cached_answers"] = sum(len(b) for b in jev.get("cache", {}).values())
s["in_flight"] = sum(1 for j in jev.get("jobs", {}).values() for f in j["futures"] if not f.done())
client = next(iter(jev.get("client_cache", {}).values()), None)   # the universal client, whichever adapter backs it
conns = getattr(getattr(client, "adapter", None), "transport", None)
s["connections"] = sum(len(v) for v in getattr(conns, "_idle", {}).values()) if conns else 0  # idle, pooled
return json.dumps(s)
$py$;

-- Forget all cached judgments for this session.
CREATE OR REPLACE FUNCTION jev_cache_clear()
RETURNS void LANGUAGE plpython3u VOLATILE AS $py$
if "jev" in GD:
    GD["jev"]["cache"].clear()
    GD["jev"]["jobs"].clear()
$py$;

CREATE OR REPLACE FUNCTION jev_version() RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT '0.2.0' $$;

COMMENT ON FUNCTION jev(anyelement, text, float8) IS 'True when the row satisfies the natural-language condition (Jev via the provider chosen in configs/jevs.yml). Usage: WHERE jev(tbl, ''condition'')';
COMMENT ON FUNCTION jev_prob(anyelement, text) IS 'Probability that the row satisfies the natural-language condition.';
COMMENT ON FUNCTION jev_score(anyelement, text, text[]) IS 'Probability-weighted rating of the row along ordered levels.';
COMMENT ON FUNCTION jev_choice(anyelement, text, text[]) IS 'Classifies the row into one of the given options.';

-- ---------------------------------------------------------------- provider configuration
-- The adapter is selected by 'jev.provider' inside configs/jevs.yml and built by JevFactory;
-- these GUCs only feed settings to it. Define them here so fresh installs get defaults without
-- postgresql.conf edits (ALTER EXTENSION ... UPDATE keeps any values users already set).
DO $do$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_settings WHERE name = 'jev.config_file') THEN
    CREATE FUNCTION public.jev_set_config_file(text) RETURNS void LANGUAGE plpython3u VOLATILE AS $py$
import os
p = $1 or ""
if not p.startswith("/"):
    p = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "configs", p)
os.environ["JEV_CONFIG"] = p
$py$;
    COMMENT ON FUNCTION public.jev_set_config_file(text) IS
      'Point this session at another provider config (configs/jevs.yml shape); relative paths resolve against the extension install dir.';
    EXECUTE pg_catalog.set_config('jev.config_file', 'configs/jevs.yml', false);
  END IF;
END
$do$;

DROP FUNCTION IF EXISTS public.jev_set_config_file(text);
