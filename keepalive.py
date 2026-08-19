# -*- coding: utf-8 -*-
"""
Keep-alive independente do Supabase.

Por que existe: o agendador embutido no `financeiro.py` roda DENTRO do processo
do Streamlit. Ele só começa quando alguém abre a página, e morre junto com o
contêiner. Se o Railway reiniciar (deploy, queda, manutenção) e ninguém abrir o
site, nada mais pinga o banco — e o projeto pode ser pausado por inatividade.

Este script não depende de nada disso: roda sozinho, faz uma consulta real e
sai. Use como Cron Job do Railway ou via GitHub Actions (veja README-keepalive).

Uso:
    python keepalive.py

Sai com código 0 se conseguiu consultar, 1 se falhou (o agendador marca a
execução como falha e você vê no log).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

import psycopg2

try:
    from zoneinfo import ZoneInfo

    FUSO = ZoneInfo("America/Sao_Paulo")
except Exception:  # pragma: no cover
    FUSO = timezone(timedelta(hours=-3))


def _obrigatorio(nome: str, padrao: str | None = None) -> str:
    valor = os.environ.get(nome) or padrao
    if not valor:
        print(f"[keep-alive] ERRO: variável de ambiente {nome} não definida.", flush=True)
        sys.exit(1)
    return valor


def main() -> int:
    momento = datetime.now(FUSO).strftime("%d/%m/%Y %H:%M:%S")
    parametros = {
        "host": _obrigatorio("SUPABASE_HOST"),
        "port": int(_obrigatorio("SUPABASE_PORT", "5432")),
        "dbname": _obrigatorio("SUPABASE_DBNAME", "postgres"),
        "user": _obrigatorio("SUPABASE_USER"),
        "password": _obrigatorio("SUPABASE_PASSWORD"),
        "sslmode": "require",
        "connect_timeout": 15,
        "application_name": "honorarios-keepalive",
    }

    conexao = None
    try:
        conexao = psycopg2.connect(**parametros)
        with conexao.cursor() as cur:
            # Consulta numa tabela real de propósito: é atividade de usuário
            # sobre dados de usuário, que é o que o Supabase contabiliza.
            cur.execute("SELECT COUNT(*) FROM contratos")
            (total,) = cur.fetchone()
        print(f"[keep-alive] {momento} — OK, {total} contrato(s) no banco.", flush=True)
        return 0
    except psycopg2.errors.UndefinedTable:
        # Banco novo, tabelas ainda não criadas: a conexão em si já conta
        # como atividade, então não é motivo para falhar.
        print(f"[keep-alive] {momento} — OK (tabela `contratos` ainda não existe).", flush=True)
        return 0
    except Exception as erro:
        print(f"[keep-alive] {momento} — FALHOU: {erro}", flush=True)
        return 1
    finally:
        if conexao is not None:
            try:
                conexao.close()
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())
