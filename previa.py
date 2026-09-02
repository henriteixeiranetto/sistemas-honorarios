# -*- coding: utf-8 -*-
"""
Prévia do sistema com dados falsos — não toca no banco de verdade.

Serve para clicar por todas as telas sem risco: nenhum contrato real é lido,
nenhuma alteração é gravada. É a forma segura de conferir mudanças de layout,
formatação e navegação antes de mandar para produção.

    streamlit run previa.py

Sem credencial nenhuma: a camada de banco é substituída por dados de exemplo.

Para conferir se o painel aguenta uma consulta quebrada (uma tela não pode
derrubar as outras):

    SIMULA_FALHA=1 streamlit run previa.py        # Linux/Mac
    $env:SIMULA_FALHA=1; streamlit run previa.py  # PowerShell
"""

import os
import pathlib
import re
import types

import pandas as pd
import streamlit as st

RAIZ = pathlib.Path(__file__).resolve().parent
ORIGEM = RAIZ / "financeiro.py"

# Carrega o financeiro.py sem executar a aplicação (a última linha é main()).
_codigo = ORIGEM.read_text(encoding="utf-8").rstrip()
if not _codigo.endswith("main()"):
    raise RuntimeError("Esperava que financeiro.py terminasse com main().")

fin = types.ModuleType("financeiro")
fin.__file__ = str(ORIGEM)
exec(compile(_codigo[: -len("main()")], str(ORIGEM), "exec"), fin.__dict__)

SIMULA_FALHA = os.environ.get("SIMULA_FALHA") == "1"
HOJE = fin.hoje()


def _d(dias: int) -> str:
    return (pd.Timestamp(HOJE) + pd.Timedelta(days=dias)).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Dados de exemplo. Valores escolhidos para exercitar os casos difíceis:
# milhão (separador de milhar), contrato quitado, parcela atrasada, cliente
# sem telefone, observação com acento e travessão.
# ---------------------------------------------------------------------------
CONTRATOS = pd.DataFrame([
    {"id": 9, "cliente": "José Caverna", "cpf_cnpj": "739.181.690-65",
     "telefone": "(81) 99812-4477", "valor_total": 15100.0, "saldo_devedor": 8000.0,
     "data_contrato": _d(-160), "observacoes": "Cliente pediu prazo — retornar 2ª feira",
     "tutela": "Deferido", "hon_inicial_ativo": "Sim", "hon_inicial_valor": 15100.0,
     "hon_inicial_parcelado": "Sim", "hon_inicial_parcelas": 4,
     "hon_inicial_vlr_parcela": 3775.0, "hon_liminar_fixo": 2000.0,
     "hon_liminar_reducao_vlr": 48000.0, "hon_liminar_reducao_prc": 12,
     "hon_exito_percentual": 20.0, "hon_exito_fixo": 0.0,
     "nr_processo": "0801234-55.2026.8.17.0001", "nr_vara": "3ª Vara Cível",
     "nome_juiz": "Dra. Helena Vasconcelos", "comarca": "Recife/PE",
     "exito_pago": 0, "exito_data_pagamento": "", "exito_valor_recebido": 0.0,
     "quitado_em": ""},
    {"id": 11, "cliente": "Marcelo Soares de Albuquerque", "cpf_cnpj": "897.208.790-41",
     "telefone": "", "valor_total": 1234567.89, "saldo_devedor": 1226567.89,
     "data_contrato": _d(-95), "observacoes": "", "tutela": "Pendente",
     "hon_inicial_ativo": "Sim", "hon_inicial_valor": 1234567.89,
     "hon_inicial_parcelado": "Sim", "hon_inicial_parcelas": 4,
     "hon_inicial_vlr_parcela": 308641.97, "hon_liminar_fixo": 0.0,
     "hon_liminar_reducao_vlr": 0.0, "hon_liminar_reducao_prc": 0,
     "hon_exito_percentual": 0.0, "hon_exito_fixo": 45000.0,
     "nr_processo": "0809876-12.2026.8.17.0001", "nr_vara": "1ª Vara da Fazenda",
     "nome_juiz": "", "comarca": "Recife/PE",
     "exito_pago": 0, "exito_data_pagamento": "", "exito_valor_recebido": 0.0,
     "quitado_em": ""},
    {"id": 14, "cliente": "Construtora Ação Ltda", "cpf_cnpj": "11.222.333/0001-81",
     "telefone": "(81) 3333-4444", "valor_total": 6500.0, "saldo_devedor": 0.0,
     "data_contrato": _d(-320), "observacoes": "Indicação do Dr. Guilherme",
     "tutela": "Parcial", "hon_inicial_ativo": "Sim", "hon_inicial_valor": 6500.0,
     "hon_inicial_parcelado": "Não", "hon_inicial_parcelas": 1,
     "hon_inicial_vlr_parcela": 6500.0, "hon_liminar_fixo": 0.0,
     "hon_liminar_reducao_vlr": 0.0, "hon_liminar_reducao_prc": 0,
     "hon_exito_percentual": 0.0, "hon_exito_fixo": 0.0,
     "nr_processo": "", "nr_vara": "", "nome_juiz": "", "comarca": "",
     "exito_pago": 1, "exito_data_pagamento": _d(-30), "exito_valor_recebido": 12000.0,
     "quitado_em": _d(-30)},
])

PARCELAS = pd.DataFrame([
    {"id": 1, "contrato_id": 9, "nr_parcela": 1, "valor_parcela": 3775.0,
     "data_vencimento": _d(-120), "data_pagamento": _d(-118) + " 10:22:00",
     "pago": 1, "forma_pagamento": "Pix"},
    {"id": 2, "contrato_id": 9, "nr_parcela": 2, "valor_parcela": 3775.0,
     "data_vencimento": _d(-30), "data_pagamento": "", "pago": 0, "forma_pagamento": ""},
    {"id": 3, "contrato_id": 9, "nr_parcela": 3, "valor_parcela": 3775.0,
     "data_vencimento": _d(5), "data_pagamento": "", "pago": 0, "forma_pagamento": ""},
    {"id": 4, "contrato_id": 9, "nr_parcela": 4, "valor_parcela": 3775.0,
     "data_vencimento": _d(40), "data_pagamento": "", "pago": 0, "forma_pagamento": ""},
])

PARCELAS_LIMINAR = pd.DataFrame([
    {"id": 1, "contrato_id": 9, "nr_parcela": 1, "valor_parcela": 4000.0,
     "data_prevista": _d(-60), "data_pagamento": _d(-58), "pago": 1},
    {"id": 2, "contrato_id": 9, "nr_parcela": 2, "valor_parcela": 4000.0,
     "data_prevista": _d(-12), "data_pagamento": "", "pago": 0},
    {"id": 3, "contrato_id": 9, "nr_parcela": 3, "valor_parcela": 4000.0,
     "data_prevista": _d(18), "data_pagamento": "", "pago": 0},
])

# Reproduz a mistura de tipos que existe no banco real — é dela que vieram
# os erros de UNION e de COALESCE.
ESTRUTURA = pd.DataFrame([
    {"table_name": "parcelas", "column_name": "data_vencimento",
     "data_type": "date", "is_nullable": "NO", "column_default": None},
    {"table_name": "parcelas", "column_name": "data_pagamento",
     "data_type": "timestamp without time zone", "is_nullable": "YES", "column_default": None},
    {"table_name": "parcelas_liminar", "column_name": "data_prevista",
     "data_type": "text", "is_nullable": "NO", "column_default": None},
    {"table_name": "contratos", "column_name": "quitado_em",
     "data_type": "text", "is_nullable": "YES", "column_default": None},
])


def _consulta_falsa(query: str, params=(), cache: bool = True) -> pd.DataFrame:
    q = " ".join(query.split()).lower()

    if "information_schema" in q:
        return ESTRUTURA.copy()
    if "count(*)" in q:
        return pd.DataFrame([{"count": len(CONTRATOS)}])
    if "version()" in q:
        return pd.DataFrame([{"version": "PostgreSQL 16 (prévia local, sem banco)"}])
    if "sum(case when pago = 1" in q:
        return pd.DataFrame([{"recebido": 4000.0, "pendente": 8000.0}])
    if "left(data_pagamento" in q:
        base = pd.Timestamp(HOJE)
        return pd.DataFrame({
            "mes": [(base - pd.DateOffset(months=i)).strftime("%Y-%m") for i in range(5, -1, -1)],
            "total": [8200.0, 12400.0, 6500.0, 15300.0, 9800.0, 18750.0],
        })

    if "union all" in q and "data_vencimento <" in q:          # inadimplência
        if SIMULA_FALHA:
            raise fin.ErroBanco(
                "UNION types date and text cannot be matched LINE 11: pl.data_prevista"
            )
        return pd.DataFrame([
            {"cliente": "José Caverna", "telefone": "(81) 99812-4477",
             "saldo_devedor": 8000.0, "tipo": "Honorários Iniciais",
             "nr_parcela": 2, "valor_parcela": 3775.0, "vencimento": _d(-30)},
            {"cliente": "José Caverna", "telefone": "(81) 99812-4477",
             "saldo_devedor": 8000.0, "tipo": "Liminar / Redução",
             "nr_parcela": 2, "valor_parcela": 4000.0, "vencimento": _d(-12)},
        ])
    if "union all" in q:                                        # próximos vencimentos
        return pd.DataFrame([
            {"cliente": "José Caverna", "telefone": "(81) 99812-4477",
             "tipo": "Honorários Iniciais", "nr_parcela": 3,
             "valor_parcela": 3775.0, "vencimento": _d(5)},
        ])

    # Tabela principal = o primeiro FROM, ignorando o que estiver em subconsulta.
    achado = re.search(r"from\s+([a-z_]+)", q)
    principal = achado.group(1) if achado else ""

    if "data_quitacao" in q:
        df = CONTRATOS[CONTRATOS["saldo_devedor"] <= 0].copy()
        df["data_quitacao"] = df["quitado_em"]
        return df
    if principal == "contratos":
        df = CONTRATOS.copy()
        if "saldo_devedor > 0" in q and "exists" not in q:
            df = df[df["saldo_devedor"] > 0]
        return df
    if principal == "parcelas_liminar":
        df = PARCELAS_LIMINAR.copy()
        if "pago = 0" in q:
            df = df[df["pago"] == 0]
        return df[["pago"]] if "select pago" in q else df
    if principal == "parcelas":
        df = PARCELAS.copy()
        if "pago = 0" in q:
            df = df[df["pago"] == 0]
        return df

    return pd.DataFrame()


def _escrita_falsa(*_args, **_kwargs) -> None:
    st.toast("Prévia: a gravação foi ignorada (nenhum banco conectado).", icon="🧪")


# Troca a camada de banco por dados de exemplo.
fin.select_db = _consulta_falsa
fin.escalar = lambda q, p=(), padrao=0: (
    len(CONTRATOS) if "count" in q.lower() else "PostgreSQL 16 (prévia local)"
)
fin.exec_db = _escrita_falsa
fin.exec_retorna = lambda *a, **k: 999
fin.inicializar_banco = lambda: {"ok": True, "avisos": []}
fin.iniciar_keepalive = lambda: None
fin._estado_cache = lambda: {"versao": 1}
fin._parametros_conexao = lambda: {
    "host": "prévia-local", "port": 5432, "dbname": "sem-banco",
}

# Entra direto, sem tela de login.
st.session_state.setdefault("autenticado", True)
st.session_state.setdefault("usuario", "prévia")

fin.main()

st.sidebar.warning(
    "Modo prévia: dados de exemplo, nenhuma gravação real.", icon="🧪"
)
