"""Parser for ``configs/jevs.yml`` — the single place that decides which adapter is used.

Shape of the file::

    jev:
      provider: typesafe            # optional; default 'typesafe' (the native Jev API)
      api_url: https://api.typesafe.ai/v1/systemone
      model: jev-latest
      timeout: 30
      keepalive: 600
      adapters:                     # optional per-adapter overrides
        openrouter:
          base_url: https://openrouter.ai/api/v1/chat/completions
          model: deepseek/deepseek-chat-v3.1

Provider credentials come from the **environment** and are looked up by the factory through the
generic key ``JEV_API_KEY`` plus the user-specified ``api_key_env`` — never by hardcoding one
provider's variable (e.g. OPENROUTER_API_KEY) into the core. The environment *is* the place where
"only create the OpenRouter client" is expressed: set ``provider: openrouter`` in this file and
export the key; nothing else builds an OpenRouter client.
"""
import json
import os

try:
    import yaml as _yaml
except ImportError:                      # PyYAML missing (minimal server images): JSON is accepted too,
    _yaml = None                         # since YAML is a superset of JSON.


def repo_root():
    """Repo (or install) root: the directory that contains lib/ and configs/.

    From lib/jev/config.py three levels up is the root; when the package is installed deeper
    (site-packages-style layouts) the nearest ancestor holding a configs/jevs.yml wins.
    """
    here = os.path.abspath(os.path.dirname(__file__))
    candidates = [os.path.dirname(os.path.dirname(here)),          # <root>/lib/jev -> <root>
                  os.path.dirname(os.path.dirname(os.path.dirname(here)))]  # .../share/jev/lib/jev
    for c in candidates:
        if os.path.isfile(os.path.join(c, "configs", "jevs.yml")):
            return c
    d = os.path.dirname(os.path.dirname(here))
    while True:                                                    # walk up looking for configs/jevs.yml
        if os.path.isfile(os.path.join(d, "configs", "jevs.yml")):
            return d
        parent = os.path.dirname(d)
        if parent == d:
            return candidates[0]
        d = parent


def resolve_path(path):
    """Absolute paths pass through; relative ones resolve against the repo/install root."""
    if not path or os.path.isabs(path):
        return path
    return os.path.join(repo_root(), path)


def default_config_path():
    return os.environ.get("JEV_CONFIG",
                          os.path.join(repo_root(), "configs", "jevs.yml"))


def parse(raw_text):
    """Parse the contents of jevs.yml into the provider section dict ('jev' root)."""
    if _yaml is not None:
        doc = _yaml.safe_load(raw_text)
    else:
        doc = json.loads(raw_text)
    if doc is None:
        doc = {}
    if not isinstance(doc, dict):
        raise ValueError("jev: config root must be a mapping with a 'jev' section")
    sec = doc.get("jev") or {}
    if not isinstance(sec, dict):
        raise ValueError("jev: 'jev:' section of the config must be a mapping")
    return sec


def load_config(path=None):
    """Read and parse ``configs/jevs.yml`` (or $JEV_CONFIG, or an explicit path).

    Relative paths resolve against the repo/install root, so callers can pass the same value
    as the ``jev.config_file`` GUC ('configs/jevs.yml'). No file -> defaults (native provider).
    """
    if path is None:
        path = default_config_path()
    else:
        path = resolve_path(path)
    try:
        with open(path) as f:
            return parse(f.read())
    except IOError:
        return {}                        # absent config: fall back to the built-in defaults


def env_key(section, name, default=None):
    """Environment variable named by ``api_key_env`` in the config (default JEV_API_KEY)."""
    var = (section.get(name) or default or "JEV_API_KEY") if section else (default or "JEV_API_KEY")
    return os.environ.get(var), var
