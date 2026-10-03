#!/usr/bin/env python3
"""Demo: Postgres em Docker + seed + perguntas naturais respondidas pelo jev via OpenRouter.

O que este script faz, passo a passo:

  1. Sobe um PostgreSQL com plpython3u e a extensao jev instaladas, num container Docker
     (usa o test/Dockerfile da repo, que ja faz `make install` da extensao).
  2. Gera um arquivo no formato configs/jevs.yml com `provider: openrouter`. O adapter a ser
     usado e decidido APENAS pela leitura desse yaml: o JevFactory recebe o objeto resultado do
     parser (jev.config.load_config) e devolve um JevClient universal — nenhum codigo do core
     sabe (ou precisa saber) qual provider esta atras dele.
  3. Executa um SEED: cria a tabela `support_tickets` e insere tickets realistas.
  4. Seleciona diversos dados com SQL normal e depois faz perguntas pro jev via OpenRouter,
     usando as 4 funcoes da interface universal:
       - jev(t, 'condicao')               -> filtro booleano em linguagem natural
       - jev_prob(t, 'condicao')          -> probabilidade calibrada por linha
       - jev_score(t, q, ARRAY[niveis])   -> classificacao ordinal ponderada
       - jev_choice(t, q, ARRAY[opcoes])  -> escolha de uma categoria
  5. Imprime os resultados e as estatisticas da sessao (jev_stats()).

Uso:
  export OPENROUTER_API_KEY=sk-or-...      # sua chave real da OpenRouter
  python3 scripts/demo_openrouter.py

  # Sem Docker? Aponte para um Postgres que ja tenha o jev instalado:
  python3 scripts/demo_openrouter.py --dsn "host=localhost port=5432 user=postgres password=pw"

  # Dry-run offline sem custo: --mock sobe um fake OpenRouter local (mesmo wire format).
  python3 scripts/demo_openrouter.py --mock

Variaveis:
  OPENROUTER_API_KEY (ou OPENROUTER_API)   chave lida pelo adapter (nunca pelo core; nunca no yml)
  JEV_DEMO_IMAGE                           imagem docker (padrao: pg-jev-demo:latest, build local)
  JEV_DEMO_PORT                            porta publicada no host (padrao: 5543)
"""
import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, "lib"))

DEFAULT_IMAGE = os.environ.get("JEV_DEMO_IMAGE", "pg-jev-demo:latest")
DEFAULT_PORT = int(os.environ.get("JEV_DEMO_PORT", "5543"))
CONTAINER = "jev-openrouter-demo"

# ---------------------------------------------------------------- seed data
SEED_SQL = """
DROP TABLE IF EXISTS support_tickets;
CREATE TABLE support_tickets (
  id         serial PRIMARY KEY,
  subject    text NOT NULL,
  body       text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO support_tickets (subject, body) VALUES
  ('Cannot login to my account',
   'I have been locked out since this morning. I reset my password three times and it still says invalid credentials. This is urgent, I have a payroll run tonight!'),
  ('Double charged for Pro plan',
   'Your system billed me twice on the 3rd of March, 49 dollars each. I want my money back NOW. This is the third time this happened and I am done being polite.'),
  ('Feature request: dark mode',
   'It would be lovely to have a dark theme for the dashboard. No rush at all, just a small wish from a happy customer.'),
  ('API returns 500 on /export',
   'Every call to POST /v2/export fails with a 500 after ~30s. Logs attached. Our nightly ETL is blocked because of this.'),
  ('How do I add a teammate?',
   'New here. I want to invite my colleague Maria to the workspace but cannot find the button. Thanks in advance!'),
  ('Service is completely down!!!',
   'NOTHING works. The whole platform has been unreachable for two hours. We are losing customers every minute. FIX THIS IMMEDIATELY.'),
  ('Invoice PDF has wrong VAT number',
   'The invoice for February shows our old VAT id. Could you reissue it? Not urgent, but accounting will complain eventually.'),
  ('Slow dashboard after upgrade',
   'Since moving to the Enterprise plan the main dashboard takes 15 seconds to load. It used to be instant. A bit frustrating.'),
  ('Thank you!',
   'Just wanted to say the support team handled my issue wonderfully yesterday. Keep up the great work, you made my week.'),
  ('Data export stuck at 99%',
   'My CSV export has been sitting at 99% for six hours. Cancelled twice, same result. Mildly annoying but I can wait a day.'),
  ('Billing portal shows empty page',
   'When I open Billing > History the page is blank white. Chrome and Firefox both. I need to download invoices for taxes this week, please help.'),
  ('Question about SSO / SAML',
   'Does the Pro plan support Okta SSO? Our IT department requires SAML before we roll out to 500 users next month.'),
  ('Refund request for annual plan',
   'I bought the annual plan last week but the product does not do what the landing page promised. I expect a full refund under your 30-day policy.'),
  ('Mobile app crashes on startup',
   'After the latest update the Android app closes immediately on launch. Reinstalled twice. Phone is a Pixel 7, Android 14.');
ANALYZE support_tickets;
"""

# As perguntas ao jev (via OpenRouter). Cada uma roda como SQL puro: o core chama
# client.evaluate(...) do JevFactory — o provider atras da interface universal e invisivel.
QUESTIONS = [
    ("Filtro booleano — jev()",
     "SELECT count(*) AS angry FROM support_tickets "
     "WHERE jev(support_tickets, 'the customer is angry or threatening to leave')"),
    ("Filtro booleano — jev()",
     "SELECT id, subject FROM support_tickets "
     "WHERE jev(support_tickets, 'this is a billing or payment problem') ORDER BY id"),
    ("Probabilidade por linha — jev_prob()",
     "SELECT id, subject, round(jev_prob(support_tickets, "
     "'the issue is urgent and blocks business operations')::numeric, 2) AS p "
     "FROM support_tickets ORDER BY p DESC LIMIT 5"),
    ("Classificacao ordinal — jev_score()",
     "SELECT id, subject, jev_score(support_tickets, 'how unhappy is this customer', "
     "ARRAY['delighted','neutral','frustrated','furious']) AS unhappiness "
     "FROM support_tickets ORDER BY unhappiness DESC LIMIT 5"),
    ("Roteamento por categoria — jev_choice()",
     "SELECT jev_choice(support_tickets, 'which team should handle this ticket', "
     "ARRAY['billing','technical','customer-success']) AS team, count(*) "
     "FROM support_tickets GROUP BY 1 ORDER BY 2 DESC"),
]

# ---------------------------------------------------------------- helpers
def sh(cmd, check=True, quiet=False, **kw):
    if not quiet:
        print("$ " + " ".join(cmd))
    r = subprocess.run(cmd, capture_output=True, text=True, **kw)
    if check and r.returncode != 0:
        sys.exit("command failed: %s\n%s\n%s" % (" ".join(cmd), r.stdout, r.stderr))
    return r


def docker_available():
    return sh(["docker", "version"], check=False, quiet=True).returncode == 0


def wait_port(host, port, timeout=90.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.5)
    return False


def start_container(port):
    if not docker_available():
        sys.exit("docker nao esta disponivel. Use --dsn para apontar para um Postgres existente.")
    names = sh(["docker", "ps", "-a", "--format", "{{.Names}}"], check=False, quiet=True).stdout.split()
    if CONTAINER in names:
        sh(["docker", "rm", "-f", CONTAINER], quiet=True)
    images = sh(["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"], check=False, quiet=True).stdout
    if DEFAULT_IMAGE not in images:
        print("building image %s (test/Dockerfile: postgres + plpython3u + make install)..." % DEFAULT_IMAGE)
        build = sh(["docker", "build", "-q", "-t", DEFAULT_IMAGE, "-f",
                    os.path.join(REPO_ROOT, "test", "Dockerfile"), REPO_ROOT], check=False, quiet=True)
        if build.returncode != 0:
            sys.exit("docker build falhou:\n%s\n%s" % (build.stdout, build.stderr))
    run = sh(["docker", "run", "-d", "--name", CONTAINER,
              "-e", "POSTGRES_HOST_AUTH_METHOD=trust",
              "-p", "%d:5432" % port, DEFAULT_IMAGE], check=False, quiet=True)
    if run.returncode != 0:
        sys.exit("docker run falhou:\n%s%s" % (run.stdout, run.stderr))
    print("waiting for postgres on localhost:%d ..." % port)
    if not wait_port("127.0.0.1", port):
        logs = sh(["docker", "logs", CONTAINER], check=False, quiet=True)
        sys.exit("postgres did not come up:\n%s" % logs.stdout[-3000:])
    time.sleep(2)   # let the entrypoint finish initdb/upgrade
    return "postgresql://postgres@127.0.0.1:%d/postgres" % port


# ---------------------------------------------------------------- mock OpenRouter (offline demo)
def start_mock_openrouter():
    """Fake chat-completions endpoint: deterministico, responde no wire format da OpenRouter
    ({choices[0].message.content} com um objeto JSON de respostas) para que o OpenRouterAdapter
    normalise tudo exatamente como faria contra a API real."""
    import http.server
    import re

    def answer_for(instr, kind, crit):
        low = (instr or "").lower()
        if kind == "noul":
            return {"type": "noul",
                    "noul": 0.86 if ("angry" in low or "billing" in low or "urgent" in low) else 0.2}
        if kind == "score":
            k = max(0, min(len(crit) - 1, 3))
            return {"type": "score", "score": float(k),
                    "probabilities": {str(i): (1.0 if i == k else 0.0) for i in range(len(crit))},
                    "confidence": 0.9}
        opts = list(crit) or ["a", "b"]
        c = opts[0]
        return {"type": "choice", "choice": c,
                "probabilities": {o: (1.0 if o == c else 0.0) for o in opts}, "confidence": 0.9}

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
            content = body["messages"][0]["content"]
            answers = {}
            # linha de pergunta renderizada pelo adapter: "rN: <instrucao> -> <sufixo por tipo>"
            for line in content.splitlines():
                m = re.match(r"^(r\d+): (.*)$", line)
                if not m or " -> " not in line:
                    continue
                qid, rest = m.group(1), m.group(2)
                instr, tail = rest.split(" -> ", 1)
                if tail.startswith("answer the probability"):
                    kind, crit = "noul", []
                elif tail.startswith("rate on the ordered scale"):
                    inner = tail.split("(", 1)[1].split(")", 1)[0] if "(" in tail else ""
                    crit = [c.split("=", 1)[-1].strip() for c in inner.split(",")]
                    kind = "score"
                else:
                    mm = re.search(r"pick exactly one of \[(.+)\]", tail)
                    crit = [c.strip().strip('"\'') for c in mm.group(1).split(",")] if mm else []
                    kind = "choice"
                answers[qid] = answer_for(instr, kind, crit)
            reply = json.dumps({"model": "mock-chat",
                                "choices": [{"message": {"role": "assistant",
                                                         "content": json.dumps(answers)}}],
                                "usage": {"prompt_tokens": len(content) // 4,
                                          "completion_tokens": len(answers)}}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(reply)))
            self.end_headers()
            self.wfile.write(reply)

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    port = srv.server_address[1]
    print("mock OpenRouter listening on http://127.0.0.1:%d/api/v1/chat/completions" % port)
    return port


# ---------------------------------------------------------------- config file (jevs.yml shape)
def write_config(provider, base_url=None, model=None):
    """Escreve um arquivo no formato configs/jevs.yml. A CHAVE NUNCA vai no arquivo: o adapter
    le OPENROUTER_API_KEY / OPENROUTER_API do ambiente (ou api_key_env definido aqui)."""
    lines = ["jev:",
             "  provider: %s" % provider,
             "  timeout: 120",
             "  concurrency: 4",
             "  adapters:",
             "    openrouter:"]
    if base_url:
        lines.append("      base_url: %s" % base_url)
    if model:
        lines.append("      model: %s" % model)
    fd, path = tempfile.mkstemp(prefix="jevs-demo-", suffix=".yml")
    with os.fdopen(fd, "w") as f:
        f.write("\n".join(lines) + "\n")
    return path


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description="Postgres in Docker + seed + jev questions via OpenRouter")
    ap.add_argument("--dsn", default=None, help="Postgres DSN (default: spin up a Docker container)")
    ap.add_argument("--port", type=int, default=DEFAULT_PORT, help="host port for the Docker container")
    ap.add_argument("--mock", action="store_true", help="use a local fake OpenRouter (no key/cost needed)")
    ap.add_argument("--keep", action="store_true", help="leave the container running after the demo")
    args = ap.parse_args()

    dsn = args.dsn
    container_up = False
    if not dsn:
        dsn = start_container(args.port)
        container_up = True

    cfg_path = None
    try:
        import psycopg2
        conn = None
        deadline = time.time() + 60
        while conn is None:
            try:
                conn = psycopg2.connect(dsn, connect_timeout=5)
            except Exception as e:
                if time.time() > deadline:
                    raise
                print("  ... waiting for postgres (%s)" % str(e).strip()[:80])
                time.sleep(2)
        conn.autocommit = True
        cur = conn.cursor()

        # 1. extension
        cur.execute("CREATE EXTENSION IF NOT EXISTS plpython3u")
        cur.execute("CREATE EXTENSION IF NOT EXISTS jev CASCADE")
        cur.execute("SELECT jev_version()")
        print("jev version in database: %s" % cur.fetchone()[0])

        # 2. seed
        print("\n== SEED: creating support_tickets ==")
        cur.execute(SEED_SQL)
        cur.execute("SELECT count(*) FROM support_tickets")
        print("seeded %d tickets" % cur.fetchone()[0])

        # 3. provider selection happens ONLY through configs/jevs.yml -> parser -> JevFactory.
        if args.mock:
            mock_port = start_mock_openrouter()
            os.environ.setdefault("OPENROUTER_API_KEY", "demo-key-not-secret")
            cfg_path = write_config("openrouter",
                                    base_url="http://127.0.0.1:%d/api/v1/chat/completions" % mock_port,
                                    model="mock-chat")
        else:
            key = os.environ.get("OPENROUTER_API_KEY") or os.environ.get("OPENROUTER_API")
            if not key:
                sys.exit("set OPENROUTER_API_KEY (ou use --mock para um dry-run offline)")
            cfg_path = write_config("openrouter")   # API real: base_url/model vem do yml/default

        # Prova do contrato antes de tocar no SQL: parser object -> JevFactory -> client universal.
        from jev.config import load_config
        from jev.factory import JevFactory
        section = load_config(cfg_path)              # o objeto resultado do parser do yaml
        factory = JevFactory.from_config(section)    # a factory decide o adapter; ninguem mais decide
        client = factory.client({})                  # JevClient universal — provider invisivel ao caller
        assert client.adapter.name == "openrouter"
        print("\nJevFactory built provider=%r from %s (core only sees JevClient.evaluate)"
              % (factory.provider_name, os.path.basename(cfg_path)))

        # 4. point the session at that config; keys stay in the environment.
        cur.execute("SET jev.notices = 'on'")
        cur.execute("SET statement_timeout = '10min'")
        cur.execute("SELECT public.jev_set_config_file(%s)", (cfg_path,))

        # 5. selecoes simples primeiro, depois as perguntas em linguagem natural.
        print("\n== SELECT simples (dados da tabela) ==")
        cur.execute("SELECT id, subject FROM support_tickets ORDER BY id LIMIT 5")
        for row in cur.fetchall():
            print("  #%s %s" % row)

        print("\n== Perguntas ao jev via OpenRouter (interface universal) ==")
        for title, sql in QUESTIONS:
            print("\n-- %s" % title)
            print("   SQL: %s" % (sql if len(sql) < 160 else sql[:157] + "..."))
            t0 = time.time()
            try:
                cur.execute(sql)
                rows = cur.fetchall()
                cols = [d.name for d in cur.description]
                dt = time.time() - t0
                print("   [%s]" % " | ".join(cols))
                for row in rows:
                    print("   %s" % " | ".join(str(v) for v in row))
                print("   (%d rows, %.1fs)" % (len(rows), dt))
            except Exception as e:
                conn.rollback()
                conn.autocommit = True
                print("   ERROR: %s" % str(e).strip().splitlines()[0])

        # 6. estatisticas da sessao — provam que os pedidos passaram pelo adapter escolhido.
        cur.execute("SELECT jev_stats()")
        stats = json.loads(cur.fetchone()[0])
        print("\n== jev_stats() ==")
        print(json.dumps(stats, indent=2))
        if stats.get("requests"):
            print("OK: %d requests julgados atraves do JevClient universal "
                  "(adapter escolhido so pelo configs/jevs.yml)" % stats["requests"])
    finally:
        if container_up and not args.keep:
            sh(["docker", "rm", "-f", CONTAINER], check=False, quiet=True)
        elif container_up:
            print("\ncontainer %s left running; stop it with: docker rm -f %s" % (CONTAINER, CONTAINER))
        if cfg_path:
            os.unlink(cfg_path)


if __name__ == "__main__":
    main()
