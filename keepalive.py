# -*- coding: utf-8 -*-
"""
Keep-alive do Supabase — evita a pausa por inatividade do plano gratuito.

Por que existe: o agendador embutido no `financeiro.py` roda DENTRO do
processo do Streamlit. Ele só começa quando alguém abre a página, e morre
junto com o contêiner. Este script não depende de nada disso.

Por que bate em DUAS portas:

    1. Conexão Postgres (SELECT numa tabela real)
    2. Requisição HTTP à API REST do projeto

A documentação da Supabase diz apenas que o projeto é pausado sem
"atividade suficiente do banco", sem esclarecer se conexão direta ao
Postgres conta. Na prática, um keep-alive que só abria conexão Postgres
não impediu a pausa. A requisição à API REST conta sob qualquer
interpretação, então o script faz as duas coisas.

Uso:
    python keepalive.py

Variáveis:
    SUPABASE_HOST, SUPABASE_USER, SUPABASE_PASSWORD   obrigatórias
    SUPABASE_PORT (5432), SUPABASE_DBNAME (postgres)  opcionais
    SUPABASE_ANON_KEY                                 opcional, ver abaixo
    SUPABASE_REF                                      opcional, ver abaixo

Sobre a SUPABASE_ANON_KEY: é a chave pública do projeto (a mesma que iria
num app de navegador). Sem ela o script ainda alcança a API, mas leva 401 —
o que provavelmente já conta como atividade, embora seja menos garantido.
Com ela, a consulta é legítima e não há dúvida.

O identificador do projeto é deduzido do SUPABASE_USER (`postgres.<ref>`)
ou do SUPABASE_HOST. Defina SUPABASE_REF apenas se a dedução falhar.

Sai com 0 se pelo menos a API respondeu; 1 se nada respondeu.
"""

from __future__ import annotations

import os
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import psycopg2

try:
    from zoneinfo import ZoneInfo

    FUSO = ZoneInfo("America/Sao_Paulo")
except Exception:  # pragma: no cover
    FUSO = timezone(timedelta(hours=-3))

TEMPO_LIMITE = 20


def _agora() -> str:
    return datetime.now(FUSO).strftime("%d/%m/%Y %H:%M:%S")


def _log(mensagem: str) -> None:
    print(f"[keep-alive] {_agora()} — {mensagem}", flush=True)


def _obrigatorio(nome: str, padrao: str | None = None) -> str:
    valor = os.environ.get(nome) or padrao
    if not valor:
        _log(f"ERRO: variável de ambiente {nome} não definida.")
        sys.exit(1)
    return valor


def identificar_projeto() -> str | None:
    """Descobre o identificador do projeto a partir do que já está configurado.

    O usuário do pooler tem a forma `postgres.<ref>`, e o host da conexão
    direta é `db.<ref>.supabase.co`. Qualquer um dos dois serve.
    """
    explicito = os.environ.get("SUPABASE_REF")
    if explicito:
        return explicito.strip()

    usuario = os.environ.get("SUPABASE_USER", "")
    if "." in usuario:
        candidato = usuario.split(".", 1)[1].strip()
        if re.fullmatch(r"[a-z0-9]{16,32}", candidato):
            return candidato

    host = os.environ.get("SUPABASE_HOST", "")
    achado = re.search(r"\bdb\.([a-z0-9]{16,32})\.supabase\.co\b", host)
    return achado.group(1) if achado else None


def pingar_api(ref: str) -> bool:
    """Requisição HTTP à API REST — é isto que conta como atividade.

    Um 401 (sem chave) também significa que a requisição chegou ao projeto.
    O que importa é distinguir "o projeto respondeu" de "o projeto está fora".
    """
    chave = os.environ.get("SUPABASE_ANON_KEY", "").strip()
    url = f"https://{ref}.supabase.co/rest/v1/"
    pedido = urllib.request.Request(url, method="GET")
    pedido.add_header("User-Agent", "honorarios-keepalive/1.0")
    if chave:
        pedido.add_header("apikey", chave)
        pedido.add_header("Authorization", f"Bearer {chave}")

    try:
        with urllib.request.urlopen(pedido, timeout=TEMPO_LIMITE) as resposta:
            _log(f"API REST respondeu {resposta.status} — atividade registrada.")
            return True
    except urllib.error.HTTPError as erro:
        if erro.code in (401, 403) and not chave:
            _log(
                f"API REST respondeu {erro.code} (sem SUPABASE_ANON_KEY). A requisição "
                "chegou ao projeto, mas defina a chave para não depender disso."
            )
        else:
            _log(f"API REST respondeu {erro.code} — a requisição chegou ao projeto.")
        return True
    except Exception as erro:
        _log(f"API REST inacessível: {erro}")
        return False


def pingar_banco(parametros: dict) -> bool:
    """Consulta uma tabela real. Confirma que o banco responde de fato."""
    conexao = None
    try:
        conexao = psycopg2.connect(**parametros)
        with conexao.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM contratos")
            (total,) = cur.fetchone()
        _log(f"Postgres OK — {total} contrato(s) no banco.")
        return True
    except psycopg2.errors.UndefinedTable:
        _log("Postgres OK (tabela `contratos` ainda não existe).")
        return True
    except Exception as erro:
        _log(f"Postgres falhou: {str(erro).strip()[:200]}")
        return False
    finally:
        if conexao is not None:
            try:
                conexao.close()
            except Exception:
                pass


def main() -> int:
    parametros = {
        "host": _obrigatorio("SUPABASE_HOST"),
        "port": int(_obrigatorio("SUPABASE_PORT", "5432")),
        "dbname": _obrigatorio("SUPABASE_DBNAME", "postgres"),
        "user": _obrigatorio("SUPABASE_USER"),
        "password": _obrigatorio("SUPABASE_PASSWORD"),
        "sslmode": "require",
        "connect_timeout": TEMPO_LIMITE,
        "application_name": "honorarios-keepalive",
    }

    ref = identificar_projeto()
    if ref:
        api_ok = pingar_api(ref)
    else:
        api_ok = False
        _log(
            "Não consegui deduzir o identificador do projeto. Defina SUPABASE_REF "
            "(o código que aparece no usuário `postgres.<ref>`)."
        )

    banco_ok = pingar_banco(parametros)

    if api_ok and banco_ok:
        _log("Tudo certo.")
        return 0
    if api_ok:
        _log("API respondeu, mas o banco não. O projeto segue ativo; verifique a conexão.")
        return 1
    if banco_ok:
        _log("Banco respondeu, mas a API não — é a API que conta contra a pausa.")
        return 1
    _log("Nem API nem banco responderam. O projeto pode estar pausado.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
