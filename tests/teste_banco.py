# -*- coding: utf-8 -*-
"""
Testa a camada de banco com conexão simulada: transação, rollback,
atomicidade, descarte de conexão morta, retry de leitura e cache.

Não precisa de banco nenhum — os dublês abaixo substituem o psycopg2.

Rodar:  python tests/teste_banco.py
"""

from __future__ import annotations

import sys

import psycopg2

from carregar import Resultado, carregar_financeiro

fin = carregar_financeiro()
r = Resultado("Camada de banco")


# ----------------------------------------------------------------- dublês --
class CursorFalso:
    def __init__(self, conexao):
        self.conexao = conexao
        self.rowcount = 1
        self.description = None
        self._linhas = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, query, params=None):
        self.conexao.registro.append(("execute", " ".join(query.split())[:60]))
        if self.conexao.falhar_em and self.conexao.falhar_em in query:
            raise psycopg2.OperationalError("conexão caiu")
        if "boom" in query:
            raise ValueError("erro de negócio")
        self.description = [("col",)]
        self._linhas = [{"col": 42}]

    def fetchone(self):
        return (42,)

    def fetchall(self):
        return list(self._linhas or [])

    def close(self):
        pass


class ConexaoFalsa:
    def __init__(self, falhar_em=None):
        self.closed = 0
        self.registro = []
        self.falhar_em = falhar_em

    def cursor(self, cursor_factory=None):
        return CursorFalso(self)

    def commit(self):
        self.registro.append(("commit", ""))

    def rollback(self):
        self.registro.append(("rollback", ""))

    def close(self):
        self.closed = 1


class PoolFalso:
    def __init__(self, conexoes):
        self.conexoes = list(conexoes)
        self.fechadas = []

    def getconn(self):
        return self.conexoes.pop(0) if self.conexoes else ConexaoFalsa()

    def putconn(self, conexao, close=False):
        if close:
            self.fechadas.append(conexao)
            conexao.close()


def instalar(pool):
    fin._pool = lambda: pool
    return pool


# ------------------------------------------------------------------ testes --
r.secao("[1] transacao() confirma e invalida o cache")
conexao = ConexaoFalsa()
instalar(PoolFalso([conexao, ConexaoFalsa()]))
versao = fin._estado_cache()["versao"]
with fin.transacao() as cur:
    cur.execute("UPDATE contratos SET cliente = %s", ("x",))
acoes = [a for a, _ in conexao.registro]
r.verdadeiro("houve commit", "commit" in acoes)
r.verdadeiro("cache foi invalidado", fin._estado_cache()["versao"] > versao)


r.secao("[2] transacao() desfaz e NÃO invalida o cache quando falha")
conexao = ConexaoFalsa()
instalar(PoolFalso([conexao, ConexaoFalsa()]))
versao = fin._estado_cache()["versao"]
try:
    with fin.transacao() as cur:
        cur.execute("UPDATE contratos SET boom = 1")
    propagou = False
except ValueError:
    propagou = True
r.verdadeiro("a exceção propaga", propagou)
r.verdadeiro("houve rollback", ("rollback", "") in conexao.registro)
r.verdadeiro("não houve commit", ("commit", "") not in conexao.registro)
r.checar("cache intacto", fin._estado_cache()["versao"], versao)


r.secao("[3] Atomicidade: falha nas parcelas desfaz o contrato")
conexao = ConexaoFalsa()
instalar(PoolFalso([conexao, ConexaoFalsa()]))
try:
    with fin.transacao() as cur:
        cur.execute("INSERT INTO contratos (cliente) VALUES (%s) RETURNING id", ("Ana",))
        cur.fetchone()
        cur.execute("INSERT INTO parcelas boom")
except ValueError:
    pass
r.verdadeiro("rollback desfez tudo", ("rollback", "") in conexao.registro)
r.verdadeiro("nada foi confirmado", ("commit", "") not in conexao.registro)


r.secao("[4] exec_db levanta ErroBanco em vez de engolir a falha")
instalar(PoolFalso([ConexaoFalsa(), ConexaoFalsa()]))
try:
    fin.exec_db("UPDATE contratos SET boom = 1")
    virou = "nenhuma exceção"
except fin.ErroBanco:
    virou = "ErroBanco"
except Exception as erro:
    virou = type(erro).__name__
r.checar("tipo da exceção", virou, "ErroBanco")


r.secao("[5] Conexão morta é descartada e trocada")
morta = ConexaoFalsa(falhar_em="SELECT 1")
viva = ConexaoFalsa()
pool = instalar(PoolFalso([morta, viva]))
with fin._conexao(verificar=True) as obtida:
    r.verdadeiro("entregou a conexão viva", obtida is viva)
r.verdadeiro("a morta foi fechada", morta in pool.fechadas)


r.secao("[6] Leitura se recupera de queda de conexão")
instalar(PoolFalso([ConexaoFalsa(falhar_em="FROM contratos"), ConexaoFalsa()]))
df = fin._select("SELECT * FROM contratos", ())
r.checar("linhas devolvidas", len(df), 1)
r.checar("valor lido", int(df.iloc[0]["col"]), 42)


r.secao("[7] Cache de consultas e invalidação por escrita")
contador = {"n": 0}
original = fin._select


def contando(query, params):
    contador["n"] += 1
    return original(query, params)


fin._select = contando
fin._consulta_cacheada.clear()
instalar(PoolFalso([ConexaoFalsa() for _ in range(20)]))

fin.select_db("SELECT * FROM contratos ORDER BY cliente")
base = contador["n"]
fin.select_db("SELECT * FROM contratos ORDER BY cliente")
fin.select_db("SELECT * FROM contratos ORDER BY cliente")
r.checar("3 chamadas = 1 ida ao banco", contador["n"], base)

fin.select_db("SELECT * FROM contratos ORDER BY cliente", cache=False)
r.checar("cache=False sempre consulta", contador["n"], base + 1)

fin._invalidar_cache()
fin.select_db("SELECT * FROM contratos ORDER BY cliente")
r.checar("após escrita, consulta de novo", contador["n"], base + 2)


r.secao("[8] select_db devolve cópia, não o objeto do cache")
primeira = fin.select_db("SELECT * FROM contratos ORDER BY cliente")
primeira["col"] = 999
segunda = fin.select_db("SELECT * FROM contratos ORDER BY cliente")
r.checar("cache não foi contaminado", int(segunda.iloc[0]["col"]), 42)

fin._select = original


r.secao("[9] Proteção das tabelas (RLS) sem trancar o sistema fora")
# O Supabase avisou: tabela em `public` sem Row-Level Security fica aberta na
# API REST para qualquer um com o endereço do projeto. A correção liga o RLS —
# mas, sem nenhuma política, o RLS nega tudo a quem não o ignora. Ligar numa
# tabela onde o sistema NÃO é dono deixaria o site no ar e vazio.

p = fin.pode_ativar_rls
r.checar("dono, sem forçar: pode", p(True, False, False), True)
r.checar("dono com BYPASSRLS: pode", p(True, False, True), True)
r.checar("dono com RLS forçado e BYPASSRLS: pode", p(True, True, True), True)
# O RLS forçado vale até para o dono. Sem BYPASSRLS, ele perderia o acesso.
r.checar("dono com RLS forçado, sem BYPASSRLS: NÃO pode", p(True, True, False), False)
# Só o dono consegue ligar o RLS; tentar daria erro de permissão.
r.checar("não é dono: NÃO pode", p(False, False, True), False)
r.checar("nulo vindo do banco não vira permissão", p(None, None, None), False)

from contextlib import contextmanager  # noqa: E402


def _texto(consulta) -> str:
    """O SQL montado com psycopg2.sql, sem precisar de conexão para renderizar."""
    if isinstance(consulta, str):
        return consulta
    partes = []
    for parte in getattr(consulta, "seq", [consulta]):
        if hasattr(parte, "strings"):
            partes.append(".".join(parte.strings))
        elif hasattr(parte, "string"):
            partes.append(parte.string)
    return "".join(partes)


class BancoRLS:
    """Um schema `public` de mentira, com transação de verdade.

    `tabelas` = {nome: (linhas, perde_acesso)}. `perde_acesso` simula uma
    tabela onde o dono não é o sistema: ligar o RLS zera o que ele enxerga.
    """

    def __init__(self, pendentes, tabelas):
        self.pendentes = pendentes
        self.tabelas = tabelas
        self.com_rls: set[str] = set()

    @contextmanager
    def transacao(self):
        banco = self
        provisorio: set[str] = set()

        class Cursor:
            def execute(self, consulta, params=None):
                texto = _texto(consulta)
                self._resposta = None
                if "relrowsecurity" in texto:
                    self._todas = list(banco.pendentes)
                    return
                nome = texto.rsplit(".", 1)[-1].strip()
                if texto.startswith("ALTER TABLE"):
                    nome = texto.split(".", 1)[1].split(" ", 1)[0]
                    provisorio.add(nome)
                elif texto.startswith("SELECT COUNT"):
                    linhas, perde = banco.tabelas[nome]
                    ligado = nome in banco.com_rls or nome in provisorio
                    self._resposta = (0 if (ligado and perde) else linhas,)

            def fetchone(self):
                return self._resposta

            def fetchall(self):
                return self._todas

        yield Cursor()
        # Só chega aqui se nada levantou: é o commit.
        banco.com_rls |= provisorio


# Três tabelas: duas do próprio sistema, uma que ele não controla.
pendentes = [
    ("contratos", True, False, False),
    ("parcelas_exito", True, False, False),
    ("tabela_alheia", False, False, False),
]
banco = BancoRLS(pendentes, {
    "contratos": (42, False),
    "parcelas_exito": (0, False),
    "tabela_alheia": (7, True),
})
_orig = fin.transacao
fin.transacao = banco.transacao
try:
    avisos = fin._proteger_tabelas()
finally:
    fin.transacao = _orig

r.verdadeiro("liga o RLS nas tabelas do sistema", {"contratos", "parcelas_exito"} <= banco.com_rls)
r.verdadeiro("tabela vazia também é protegida", "parcelas_exito" in banco.com_rls)
r.verdadeiro("não mexe na tabela que não é dele", "tabela_alheia" not in banco.com_rls)
r.checar("e avisa sobre ela, uma vez", sum("tabela_alheia" in a for a in avisos), 1)
r.checar("sem outros avisos", len(avisos), 1)

# A rede de segurança: o dono aparenta poder, mas ligar o RLS zera a leitura.
# Tem de desfazer o ALTER e avisar — nunca deixar o site vazio.
armadilha = BancoRLS(
    [("contratos", True, False, False)],
    {"contratos": (42, True)},
)
fin.transacao = armadilha.transacao
try:
    avisos = fin._proteger_tabelas()
finally:
    fin.transacao = _orig

r.verdadeiro("se o sistema perderia as linhas, desfaz", "contratos" not in armadilha.com_rls)
r.verdadeiro("e explica o motivo", any("42" in a for a in avisos))

# Falha ao consultar o catálogo não pode derrubar a inicialização.
@contextmanager
def _quebrada():
    raise psycopg2.OperationalError("sem conexão")
    yield  # noqa: unreachable

fin.transacao = _quebrada
try:
    avisos = fin._proteger_tabelas()
finally:
    fin.transacao = _orig
r.checar("banco fora do ar vira aviso, não exceção", len(avisos), 1)

# Toda tabela nova nasce sem RLS no Supabase — foi assim com parcelas_exito e
# opcoes. A varredura precisa rodar DEPOIS de todos os CREATE TABLE.
from carregar import ORIGEM  # noqa: E402

_init = ORIGEM.read_text(encoding="utf-8")
_init = _init[_init.index("def inicializar_banco"):]
_init = _init[: _init.index("\ndef ", 1)]
r.verdadeiro("inicialização protege as tabelas", "_proteger_tabelas()" in _init)
r.verdadeiro(
    "e só depois de criar todas",
    _init.rindex("DDL_") < _init.index("_proteger_tabelas()"),
)


sys.exit(r.encerrar())
