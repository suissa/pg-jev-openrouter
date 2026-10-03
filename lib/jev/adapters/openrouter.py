"""OpenRouter adapter — judge rows through any OpenRouter chat model.

Implements the universal :class:`jev.interface.JevAdapter` contract on top of OpenRouter's
chat-completions endpoint: the shared state and the batch of questions are rendered into one
prompt, the model must answer with a strict JSON object, and the reply is parsed back into the
normalised answer shape (``{"answers": {qid: {...}}, "usage": {...}}``) that every jev caller
sees — so the core behaves identically no matter which provider the factory chose.

Credentials: the key comes from ``JEV_API_KEY``, from the variable named by ``api_key_env`` in
configs/jevs.yml (e.g. ``OPENROUTER_API_KEY``), or from ``OPENROUTER_API_KEY`` /
``OPENROUTER_API`` as this provider's own fallbacks. Which provider is used at all is decided
by ``jev.provider`` in configs/jevs.yml — never by branching inside the core.
"""
import json
from urllib.parse import urlsplit

from jev.interface import JevAdapter, JevError, JevRetryable
from jev.adapters._common import HttpTransport, merged, retry_after_seconds, resolve_key, RETRYABLE_STATUS

DEFAULT_BASE_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_MODEL = "deepseek/deepseek-chat-v3.1"


def _render(state, questions):
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
    name = "openrouter"
    ENV_KEYS = ("JEV_API_KEY", "OPENROUTER_API_KEY", "OPENROUTER_API")

    def __init__(self, base_url, model, timeout, keepalive, concurrency, api_key, referer=None, title=None):
        url = urlsplit(base_url or DEFAULT_BASE_URL)
        self.target = {"scheme": url.scheme, "host": url.hostname, "port": url.port}
        self.path = (url.path or "/") + ("?" + url.query if url.query else "")
        self.model = model or DEFAULT_MODEL
        self.api_key = api_key
        self.headers = {"Authorization": "Bearer " + api_key, "Content-Type": "application/json"}
        if referer:
            self.headers["HTTP-Referer"] = referer
        if title:
            self.headers["X-Title"] = title
        self.transport = HttpTransport(timeout=timeout, keepalive=keepalive, max_idle=concurrency)

    @classmethod
    def from_settings(cls, section, gucs):
        per = dict(section.get("adapters", {}).get(cls.name, {})) if isinstance(section.get("adapters"), dict) else {}
        merged_section = dict(per); merged_section.update({k: v for k, v in section.items() if k != "adapters"})
        api_key = resolve_key(gucs, merged_section, cls.ENV_KEYS)
        if not api_key:
            raise JevError("jev: no API key for provider 'openrouter'. Export OPENROUTER_API_KEY "
                           "(or set api_key_env in configs/jevs.yml, or SET jev.api_key).")
        return cls(
            base_url=merged(merged_section, gucs, "base_url", DEFAULT_BASE_URL),
            model=merged(merged_section, gucs, "model", DEFAULT_MODEL),
            timeout=float(merged(merged_section, gucs, "timeout", 60)),
            keepalive=float(merged(merged_section, gucs, "keepalive", 600)),
            concurrency=max(1, int(merged(merged_section, gucs, "concurrency", 16))),
            api_key=api_key,
            referer=merged(merged_section, gucs, "http_referer", None),
            title=merged(merged_section, gucs, "app_title", "pg-jev"),
        )

    def evaluate(self, state, questions):
        payload = json.dumps({
            "model": self.model,
            "messages": [{"role": "user", "content": _render(state, questions)}],
            "temperature": 0,
            "max_tokens": 4096,
        }).encode()
        status, getheader, raw = self.transport.post_json(self.target, self.path, payload, self.headers)
        if status == 200:
            return self._normalise(json.loads(raw.decode()), questions)
        snippet = "%s %s" % (status, raw.decode(errors="replace")[:300])
        if status in RETRYABLE_STATUS:
            raise JevRetryable("jev: OpenRouter error " + snippet, retry_after=retry_after_seconds(getheader))
        raise JevError("jev: OpenRouter error " + snippet)

    def _normalise(self, data, questions):
        """Chat completion -> the normalised answer shape of the universal interface."""
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
        # choice
        opts = list((q.get("criteria") or {}).keys())
        choice = a.get("choice")
        if choice not in opts:
            raise JevError("jev: OpenRouter choice %r is not one of the options for %r" % (choice, qid))
        probs = a.get("probabilities") if isinstance(a.get("probabilities"), dict) else {}
        return {"type": "choice", "choice": choice, "probabilities": probs,
                "confidence": float(a.get("confidence", 0.0))}
