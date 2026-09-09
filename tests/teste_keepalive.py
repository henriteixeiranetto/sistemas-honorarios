# -*- coding: utf-8 -*-
"""
Testa o keep-alive sem rede: dedução do identificador do projeto e a decisão
de sucesso/falha.

Rodar:  python tests/teste_keepalive.py
"""

from __future__ import annotations

import importlib
import os
import pathlib
import sys

from carregar import RAIZ, Resultado

sys.path.insert(0, str(RAIZ))


def _recarregar(**ambiente):
    for chave in ("SUPABASE_REF", "SUPABASE_USER", "SUPABASE_HOST", "SUPABASE_ANON_KEY"):
        os.environ.pop(chave, None)
    os.environ.update(ambiente)
    return importlib.reload(importlib.import_module("keepalive"))


r = Resultado("Keep-alive")


r.secao("[1] Identificar o projeto a partir do que já está configurado")
ka = _recarregar(
    SUPABASE_USER="postgres.eygwrggknlblfcywfzgx",
    SUPABASE_HOST="aws-1-sa-east-1.pooler.supabase.com",
)
r.checar("usuário do pooler (postgres.<ref>)", ka.identificar_projeto(), "eygwrggknlblfcywfzgx")

ka = _recarregar(SUPABASE_USER="postgres", SUPABASE_HOST="db.eygwrggknlblfcywfzgx.supabase.co")
r.checar("host da conexão direta", ka.identificar_projeto(), "eygwrggknlblfcywfzgx")

ka = _recarregar(SUPABASE_REF="abcdefghijklmnop", SUPABASE_USER="postgres", SUPABASE_HOST="x")
r.checar("SUPABASE_REF tem prioridade", ka.identificar_projeto(), "abcdefghijklmnop")

ka = _recarregar(SUPABASE_USER="postgres", SUPABASE_HOST="localhost")
r.checar("sem como deduzir devolve None", ka.identificar_projeto(), None)

# Regressão: um usuário com ponto mas sufixo curto não é um identificador.
ka = _recarregar(SUPABASE_USER="postgres.admin", SUPABASE_HOST="localhost")
r.checar("sufixo curto não é confundido com ref", ka.identificar_projeto(), None)


r.secao("[2] O script existe e expõe as duas checagens")
ka = _recarregar(SUPABASE_USER="postgres.eygwrggknlblfcywfzgx", SUPABASE_HOST="x")
for funcao in ("pingar_api", "pingar_banco", "identificar_projeto", "main"):
    r.verdadeiro(f"{funcao}() definida", callable(getattr(ka, funcao, None)))

fonte = pathlib.Path(RAIZ / "keepalive.py").read_text(encoding="utf-8")
# Regressão: a versão anterior só abria conexão Postgres, e o projeto pausava
# mesmo assim. A requisição HTTP à API é o que conta contra a pausa.
r.verdadeiro("faz requisição HTTP à API REST", "urllib.request" in fonte)
r.verdadeiro("monta a URL do projeto", "supabase.co/rest/v1" in fonte)
r.verdadeiro("continua consultando o Postgres", "SELECT COUNT(*) FROM contratos" in fonte)


sys.exit(r.encerrar())
