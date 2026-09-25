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

# O console do Windows usa cp1252 e levanta excecao ao imprimir emoji. Um
# teste nao pode falhar por causa do terminal.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(errors="replace")

try:
    import pglast
except ImportError:
    print("pglast não instalado — pulando a validação de SQL.")
    print("Para rodar: pip install -r requirements-dev.txt")
    sys.exit(0)

INICIOS = ("select", "insert", "update", "delete", "create", "alter", "with", "drop")

# O comando tem de COMECAR com a palavra-chave seguida de espaco. Sem a
# fronteira, a docstring "Selectbox da lista..." era lida como um SELECT e o
# parser reclamava de um SQL que nunca existiu.
COMANDO = re.compile(r"^(?:" + "|".join(INICIOS) + r")\s", re.IGNORECASE)


def sem_comentario(sql: str) -> str:
    """Tira os comentários do topo, para reconhecer o comando que vem abaixo.

    Sem isso, toda constante que começa explicando a si mesma era classificada
    como "não é SQL" e saía da validação sem avisar ninguém.
    """
    linhas = sql.strip().splitlines()
    while linhas and (not linhas[0].strip() or linhas[0].lstrip().startswith("--")):
        linhas.pop(0)
    return "\n".join(linhas).strip()


def parece_sql(texto: str) -> bool:
    return bool(COMANDO.match(sem_comentario(texto)))


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
        if parece_sql(texto) and len(texto) > 15:
            comandos.append((no.lineno, texto))
    elif isinstance(no, ast.JoinedStr):
        montado = "".join(
            str(v.value) if isinstance(v, ast.Constant) else "__CAMPO__" for v in no.values
        ).strip()
        if parece_sql(montado):
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

# =============================================================================
# Sinal de porcentagem solto
# =============================================================================
# O pglast aprova, o PostgreSQL aprovaria, e mesmo assim a consulta explode
# antes de sair do Python: quando recebe parâmetros, o psycopg2 varre a string
# e trata QUALQUER "%" como um espaço para argumento — inclusive um que esteja
# dentro de um comentário. Um a mais consome o parâmetro do WHERE e o erro que
# chega na tela é "tuple index out of range", que não aponta para lugar nenhum.
#
# Para escrever o símbolo de porcentagem em SQL, dobre: "%%".

SOLTO = re.compile(r"%(?![s%]|\(\w+\)s)")

print("\nSinal de porcentagem solto:")
achados = 0
for linha, sql in comandos:
    for m in SOLTO.finditer(sql):
        achados += 1
        falhas += 1
        i = sql[: m.start()].count("\n")
        print(f"  ERRO  L{linha:<5} {sql.splitlines()[i].strip()}")
        print('        "%" solto — dobre para "%%" ou reescreva sem ele')
if not achados:
    print(f"  ok    nenhum em {len(comandos)} comandos")


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
