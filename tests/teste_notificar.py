# -*- coding: utf-8 -*-
"""
Testa o aviso de vencimentos sem banco e sem rede: montagem da mensagem,
deduplicação e escolha de canal.

Rodar:  python tests/teste_notificar.py
"""

from __future__ import annotations

import datetime as dt
import os
import sys

from carregar import RAIZ, Resultado

sys.path.insert(0, str(RAIZ))

import notificar as nt  # noqa: E402

r = Resultado("Aviso de vencimentos")


def _item(cliente, tipo, nr, valor, vencimento, contrato_id=1):
    return {
        "tipo": tipo, "contrato_id": contrato_id, "cliente": cliente,
        "telefone": "", "nr_parcela": nr, "valor": valor, "vencimento": vencimento,
    }


HOJE = dt.date(2026, 9, 21)
ONTEM = (HOJE - dt.timedelta(days=5)).isoformat()


r.secao("[1] Formatação brasileira")
r.checar("moeda", nt.moeda(4000), "R$ 4.000,00")
r.checar("moeda com milhão", nt.moeda(1234567.89), "R$ 1.234.567,89")
r.checar("moeda inválida não quebra", nt.moeda("abc"), "R$ 0,00")
r.checar("data", nt.data_br("2026-09-21"), "21/09/2026")
r.checar("data com hora", nt.data_br("2026-09-21 10:00:00"), "21/09/2026")


r.secao("[2] Mensagem")
itens = [
    _item("José Caverna", "Honorários Iniciais", 2, 3775.0, HOJE.isoformat()),
    _item("Construtora Ação Ltda", "Redução da Liminar", 3, 4000.0, HOJE.isoformat(), 2),
]
assunto, corpo = nt.montar_mensagem(itens, HOJE)
r.verdadeiro("assunto traz a data", "21/09/2026" in assunto)
r.verdadeiro("assunto traz a quantidade", "2 parcela" in assunto)
r.verdadeiro("corpo lista o primeiro cliente", "José Caverna" in corpo)
r.verdadeiro("corpo lista o segundo cliente", "Construtora Ação Ltda" in corpo)
r.verdadeiro("corpo traz o valor em pt-BR", "R$ 3.775,00" in corpo)
r.verdadeiro("corpo soma o total", "R$ 7.775,00" in corpo)
r.verdadeiro("sem seção de atraso quando não há", "Em atraso" not in corpo)

# Regressão: atrasada e vencendo hoje têm de aparecer separadas, senão a
# pessoa não sabe o que é urgente e o que é de hoje.
com_atraso = itens + [_item("Maria Silva", "Honorários Iniciais", 1, 500.0, ONTEM, 3)]
_, corpo2 = nt.montar_mensagem(com_atraso, HOJE)
r.verdadeiro("separa a seção de atraso", "Em atraso" in corpo2)
r.verdadeiro("mostra os dias de atraso", "5 dia(s)" in corpo2)
r.verdadeiro("mantém a seção de hoje", "Vence hoje" in corpo2)
r.verdadeiro("total soma tudo", "R$ 8.275,00" in corpo2)


r.secao("[3] Canal não configurado devolve None (e não falha)")
for chave in list(os.environ):
    if chave.startswith(("NOTIFICAR_", "SMTP_", "WHATSAPP_", "TELEGRAM_")):
        del os.environ[chave]
r.checar("e-mail sem destinatário", nt.enviar_email("a", "b"), None)
r.checar("whatsapp sem token", nt.enviar_whatsapp("b", itens, HOJE), None)
r.checar("telegram sem token", nt.enviar_telegram("b"), None)


r.secao("[4] Deduplicação — não avisar duas vezes")


class _CursorFalso:
    def __init__(self, ja_avisados):
        self.ja_avisados = ja_avisados
        self.inseridos = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self._ultimo = (sql, params)
        if sql.strip().upper().startswith("INSERT"):
            self.inseridos.append(params)

    def fetchall(self):
        return list(self.ja_avisados)

    def close(self):
        pass


class _ConexaoFalsa:
    def __init__(self, ja_avisados=()):
        self.cur = _CursorFalso(ja_avisados)

    def cursor(self, cursor_factory=None):
        return self.cur


DIA = HOJE.isoformat()

# Nada avisado ainda: os dois passam.
r.checar("nenhum avisado ainda", len(nt.filtrar_nao_avisadas(_ConexaoFalsa(), itens, DIA)), 2)

# O primeiro já foi avisado hoje: sobra um.
avisado = [("Honorários Iniciais", 1, 2, DIA)]
restantes = nt.filtrar_nao_avisadas(_ConexaoFalsa(avisado), itens, DIA)
r.checar("um já avisado, sobra um", len(restantes), 1)
r.checar("sobrou o certo", restantes[0]["cliente"], "Construtora Ação Ltda")

# Todos avisados hoje: não sobra nada.
todos = [("Honorários Iniciais", 1, 2, DIA), ("Redução da Liminar", 2, 3, DIA)]
r.checar("todos avisados hoje", len(nt.filtrar_nao_avisadas(_ConexaoFalsa(todos), itens, DIA)), 0)
r.checar("lista vazia não consulta o banco", nt.filtrar_nao_avisadas(None, [], DIA), [])

# Regressão: a chave é o DIA do aviso, não o vencimento. Uma parcela atrasada
# avisada ontem tem de voltar a aparecer hoje — senão a cobrança some.
ontem = (HOJE - dt.timedelta(days=1)).isoformat()
avisado_ontem = [("Honorários Iniciais", 1, 2, ontem), ("Redução da Liminar", 2, 3, ontem)]
r.checar(
    "avisado ontem volta a aparecer hoje",
    len(nt.filtrar_nao_avisadas(_ConexaoFalsa(avisado_ontem), itens, DIA)),
    2,
)


r.secao("[5] Registro do envio")
conexao = _ConexaoFalsa()
nt.registrar_envio(conexao, itens, HOJE.isoformat(), "e-mail")
r.checar("grava uma linha por parcela", len(conexao.cur.inseridos), 2)
r.checar("grava o canal usado", conexao.cur.inseridos[0][-1], "e-mail")
r.checar("grava o dia do aviso", conexao.cur.inseridos[0][-2], HOJE.isoformat())


sys.exit(r.encerrar())
