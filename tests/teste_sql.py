# -*- coding: utf-8 -*-
"""
Extrai todo o SQL do `financeiro.py` e valida contra a gramática real do
PostgreSQL, usando o pglast (binding do libpg_query, o mesmo parser do
servidor). Pega erro de sintaxe sem precisar de banco.

Requer:  pip install pglast     (está no requirements-dev.txt)
Rodar:   python tests/teste_sql.py
"""

from __future__ import annotations

import ast
import pathlib
import re
import sys

from carregar import ORIGEM

try:
    import pglast
except ImportError:
    print("pglast não instalado — pulando a validação de SQL.")
    print("Para rodar: pip install -r requirements-dev.txt")
    sys.exit(0)

INICIOS = ("select", "insert", "update", "delete", "create", "alter", "with", "drop")

fonte = pathlib.Path(ORIGEM).read_text(encoding="utf-8")
arvore = ast.parse(fonte)

# Constantes dentro de f-string são pedaços, não comandos completos.
pedacos = {
    id(valor)
    for no in ast.walk(arvore)
    if isinstance(no, ast.JoinedStr)
    for valor in no.values
    if isinstance(valor, ast.Constant)
}

comandos: list[tuple[int, str]] = []
for no in ast.walk(arvore):
    if isinstance(no, ast.Constant) and isinstance(no.value, str) and id(no) not in pedacos:
        texto = no.value.strip()
        if texto.lower().startswith(INICIOS) and len(texto) > 15:
            comandos.append((no.lineno, texto))
    elif isinstance(no, ast.JoinedStr):
        montado = "".join(
            str(v.value) if isinstance(v, ast.Constant) else "__CAMPO__" for v in no.values
        ).strip()
        if montado.lower().startswith(INICIOS):
            comandos.append((no.lineno, montado))


def preparar(sql: str) -> str:
    """Troca os placeholders por algo que o parser aceite."""
    sql = re.sub(r"VALUES\s+%s", "VALUES (1,2,3,4)", sql, flags=re.IGNORECASE)
    sql = sql.replace("__CAMPO__ __CAMPO__", "coluna_exemplo TEXT").replace("__CAMPO__", "coluna_exemplo")
    contador = [0]

    def numerar(_m):
        contador[0] += 1
        return f"${contador[0]}"

    return re.sub(r"%s", numerar, sql)


print(f"Comandos SQL encontrados em {pathlib.Path(ORIGEM).name}: {len(comandos)}\n")

falhas = 0
for linha, sql in comandos:
    resumo = " ".join(sql.split())[:70]
    try:
        pglast.parse_sql(preparar(sql))
        print(f"  ok    L{linha:<5} {resumo}")
    except Exception as erro:
        falhas += 1
        print(f"  ERRO  L{linha:<5} {resumo}")
        print(f"        {erro}")

# O migracoes.sql também precisa ser válido.
migracoes = pathlib.Path(ORIGEM).parent / "migracoes.sql"
if migracoes.exists():
    try:
        blocos = pglast.parse_sql(migracoes.read_text(encoding="utf-8"))
        print(f"\n  ok    migracoes.sql — {len(blocos)} comandos")
    except Exception as erro:
        falhas += 1
        print(f"\n  ERRO  migracoes.sql: {erro}")

print("\n" + "=" * 62)
if falhas:
    print(f"SQL: {falhas} comando(s) inválido(s)")
    sys.exit(1)
print(f"SQL: todos os {len(comandos)} comandos são válidos no PostgreSQL")
sys.exit(0)
