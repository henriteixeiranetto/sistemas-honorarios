# -*- coding: utf-8 -*-
"""
Testa as funções puras: validação, formatação, divisão de parcelas, datas,
geração de Excel e PDF.

Rodar:  python tests/teste_funcoes.py
"""

from __future__ import annotations

import datetime as dt
import io
import sys

import pandas as pd

from carregar import Resultado, carregar_financeiro

fin = carregar_financeiro()
r = Resultado("Funções puras")


r.secao("[1] Validação de CPF/CNPJ")
r.checar("CPF válido", fin.validar_cpf("52998224725"), True)
r.checar("CPF com dígitos repetidos", fin.validar_cpf("11111111111"), False)
r.checar("CPF curto demais", fin.validar_cpf("123"), False)
r.checar("CNPJ válido", fin.validar_cnpj("11222333000181"), True)
r.checar("CNPJ com dígito errado", fin.validar_cnpj("11222333000182"), False)
r.checar("documento formatado aceito", fin.validar_documento("529.982.247-25"), None)
r.checar(
    "documento de tamanho inválido",
    fin.validar_documento("123"),
    "O documento deve ter 11 dígitos (CPF) ou 14 dígitos (CNPJ).",
)


r.secao("[2] Formatação")
r.checar("CPF", fin.formatar_cpf_cnpj("52998224725"), "529.982.247-25")
r.checar("CPF já formatado não duplica", fin.formatar_cpf_cnpj("529.982.247-25"), "529.982.247-25")
r.checar("CNPJ", fin.formatar_cpf_cnpj("11222333000181"), "11.222.333/0001-81")
r.checar("documento vazio", fin.formatar_cpf_cnpj(""), "-")
r.checar("celular", fin.formatar_telefone("11999998888"), "(11) 99999-8888")
r.checar("fixo", fin.formatar_telefone("1133334444"), "(11) 3333-4444")
r.checar("telefone já formatado", fin.formatar_telefone("(11) 99999-8888"), "(11) 99999-8888")
r.checar("data", fin.formatar_data("2026-08-18"), "18/08/2026")
r.checar("data com hora", fin.formatar_data("2026-08-18 14:30:00"), "18/08/2026 14:30")
r.checar("data vazia", fin.formatar_data(""), "-")
r.checar("data NaT", fin.formatar_data("NaT"), "-")
r.checar("data inválida", fin.formatar_data("xx"), "-")
r.checar("nulo reconhece NaT", fin.nulo("NaT"), True)
r.checar("nulo não confunde texto", fin.nulo("Pago"), False)

# Regressão: o sistema exibia "R$ 15,100.00" (formato americano). Ponto no
# milhar e vírgula no decimal — é o que um escritório brasileiro espera ler.
r.checar("moeda: milhar", fin.moeda(15100), "R$ 15.100,00")
r.checar("moeda: milhão", fin.moeda(1234567.89), "R$ 1.234.567,89")
r.checar("moeda: centavos", fin.moeda(1625.5), "R$ 1.625,50")
r.checar("moeda: zero", fin.moeda(0), "R$ 0,00")
r.checar("moeda: negativo", fin.moeda(-300.25), "R$ -300,25")
r.checar("moeda: texto inválido", fin.moeda("abc"), "R$ 0,00")
r.checar("moeda: None", fin.moeda(None), "R$ 0,00")
# Regressão: o markdown do Streamlit trata $...$ como LaTeX. Duas ocorrências
# de "R$" na mesma frase faziam o texto entre elas virar fórmula, em fonte
# monoespaçada, com os ** do negrito aparecendo crus.
r.checar("moeda para markdown escapa o cifrão", fin.moeda_md(48000), r"R\$ 48.000,00")
r.verdadeiro("moeda comum NÃO escapa (usada em HTML, PDF e recibo)", "\\" not in fin.moeda(48000))
r.checar("número sem moeda", fin.numero_br(1234567.891), "1.234.567,89")
r.checar("percentual pt-BR", fin.porcentagem(0.47), "47,0%")
r.checar("percentual cheio", fin.porcentagem(1.0), "100,0%")
r.verdadeiro("data exibida com o dia primeiro", fin.FORMATO_DATA.startswith("%d"))
r.verdadeiro("calendário com o dia primeiro", fin.FORMATO_DATA_WIDGET.startswith("DD"))
r.checar("telefone vazio vai NULL ao banco", fin.telefone_para_banco(""), None)
r.checar("telefone '-' vai NULL ao banco", fin.telefone_para_banco("-"), None)


r.secao("[3] Divisão de parcelas — a soma tem que fechar ao centavo")
for total, quantidade in [(1000.0, 3), (100.0, 7), (2500.55, 12), (999.99, 2), (5000.0, 1), (0.03, 4)]:
    valores = fin.dividir_parcelas(total, quantidade)
    r.checar(f"soma de {total} em {quantidade}x", round(sum(valores), 2), round(total, 2))
    r.checar(f"quantidade de {total} em {quantidade}x", len(valores), quantidade)
r.checar("zero parcelas", fin.dividir_parcelas(100.0, 0), [])


r.secao("[4] Vencimentos mensais")
vencimentos = fin.gerar_vencimentos(dt.date(2026, 1, 31), 4)
r.checar("primeiro vencimento é a data inicial", vencimentos[0], dt.date(2026, 1, 31))
r.checar("fevereiro é limitado ao dia 28", vencimentos[1], dt.date(2026, 2, 28))
r.checar("março volta ao dia 31", vencimentos[2], dt.date(2026, 3, 31))
r.checar("quantidade gerada", len(vencimentos), 4)


r.secao("[5] Status da parcela")
r.checar("paga", fin.obter_status_parcela(1, "2020-01-01"), "🟢 Pago")
r.checar("pendente no futuro", fin.obter_status_parcela(0, "2999-01-01"), "🟡 Pendente")
r.verdadeiro("atrasada", fin.obter_status_parcela(0, "2020-01-01").startswith("🔴 Atrasado"))
r.checar("data inválida não quebra", fin.obter_status_parcela(0, ""), "🟡 Pendente")


r.secao("[6] Link do WhatsApp — regressão do 55 duplicado")
r.verdadeiro(
    "sem DDI ganha o 55",
    fin.link_whatsapp("(11) 99999-8888", "oi").startswith("https://wa.me/5511999998888"),
)
r.verdadeiro(
    "com DDI não duplica",
    fin.link_whatsapp("5511999998888", "oi").startswith("https://wa.me/5511999998888"),
)
r.checar("sem telefone não gera link", fin.link_whatsapp("", "oi"), None)


r.secao("[7] Carimbo de data/hora do pagamento")
r.checar("recebimento de hoje guarda a hora", len(fin.carimbo(fin.hoje())), 19)
r.checar("lançamento retroativo guarda só a data", fin.carimbo(dt.date(2026, 1, 5)), "2026-01-05")
r.checar("retroativo continua legível", fin.formatar_data(fin.carimbo(dt.date(2026, 1, 5))), "05/01/2026")


r.secao("[8] Recibo")
recibo = fin.montar_recibo(
    titulo="RECIBO DE HONORÁRIOS",
    cliente="José da Silva",
    documento="52998224725",
    itens=["💰 Honorários Iniciais — Parcela 1: R$ 500,00"],
    total=500.0,
    data=dt.date(2026, 8, 18),
    metodo="Pix",
    saldo_restante=1500.0,
)
r.verdadeiro("traz o cliente", "José da Silva" in recibo)
r.verdadeiro("formata o documento", "529.982.247-25" in recibo)
r.verdadeiro("mostra o saldo restante", "Saldo Devedor Restante" in recibo)
r.verdadeiro("mostra o método", "Pix" in recibo)
sem_saldo = fin.montar_recibo(
    titulo="X", cliente="A", documento="", itens=[], total=1.0, data=dt.date(2026, 1, 1)
)
r.verdadeiro("omite o saldo quando não se aplica", "Saldo Devedor" not in sem_saldo)


r.secao("[9] PDF — regressão do crash com emoji e acento")
tabela = pd.DataFrame(
    {
        "Cliente": ["José Antônio Gonçalves de Araújo", "Ação Ltda"],
        "Status": ["🟢 Pago", "🔴 Atrasado (12 dias)"],
        "Observações": ["Cliente pediu prazo — ligar 2ª feira ✅", "-"],
        "Valor": [1234.56, 99.9],
    }
)
pdf = fin.gerar_pdf(tabela, "Relatório de Contratos Ativos")
r.checar("gera PDF com emoji sem estourar", pdf[:4], b"%PDF")
r.verdadeiro("PDF tem conteúdo", len(pdf) > 800)
r.checar("PDF de tabela vazia", fin.gerar_pdf(pd.DataFrame(), "Vazio")[:4], b"%PDF")
grande = pd.DataFrame(
    {"Cliente": [f"Cliente ção {i}" for i in range(120)], "Valor": [float(i) for i in range(120)]}
)
r.checar("PDF com quebra de página", fin.gerar_pdf(grande, "Relatório Grande")[:4], b"%PDF")
r.checar("recibo em PDF", fin.gerar_pdf_recibo(recibo)[:4], b"%PDF")


r.secao("[10] Excel")
xlsx = fin.gerar_excel(tabela)
r.checar("arquivo xlsx", xlsx[:2], b"PK")
import openpyxl  # noqa: E402  (só é necessário para conferir o resultado)

aba = openpyxl.load_workbook(io.BytesIO(xlsx))["Relatorio"]
r.checar("cabeçalho em negrito", aba["A1"].font.bold, True)
r.checar("primeira linha congelada", aba.freeze_panes, "A2")
r.checar("linhas escritas", aba.max_row, 3)
r.checar("coluna de valor formatada como moeda", aba["D2"].number_format, "R$ #,##0.00")


r.secao("[11] Fuso horário")
r.checar("fuso do escritório", str(fin.FUSO), "America/Sao_Paulo")
r.verdadeiro("agora() tem fuso", fin.agora().tzinfo is not None)
r.verdadeiro("hoje() devolve date", isinstance(fin.hoje(), dt.date))


r.secao("[12] Conversão numérica das consultas")
bruto = pd.DataFrame({"valor_total": ["100.5", ""], "saldo_devedor": ["", "3"]})
convertido = fin.numerico(bruto.copy(), "valor_total", "saldo_devedor")
r.checar("soma dos valores", float(convertido["valor_total"].sum()), 100.5)
r.checar("vazio vira zero", float(convertido["saldo_devedor"].sum()), 3.0)
r.verdadeiro("coluna inexistente não quebra", "x" not in fin.numerico(bruto.copy(), "x").columns)


r.secao("[13] Rótulos das listas de contrato")
# O escritório pediu para tirar o "(Contrato #12)" do lado do nome. O rótulo
# é a chave do selectbox: sem desempate, dois contratos do mesmo cliente
# colidiriam e um sumiria da lista.
_df = pd.DataFrame([
    {"id": 9, "cliente": "Construtora Ação Ltda", "nr_processo": "0801234-55", "data_contrato": "2026-03-18"},
    {"id": 11, "cliente": "José Caverna", "nr_processo": "", "data_contrato": "2026-03-09"},
])
_mapa = fin._mapa_contratos(_df)
r.checar("nome limpo, sem número do contrato", sorted(_mapa), ["Construtora Ação Ltda", "José Caverna"])

_repetido = pd.DataFrame([
    {"id": 9, "cliente": "Maria Silva", "nr_processo": "0801234-55", "data_contrato": "2026-03-18"},
    {"id": 11, "cliente": "Maria Silva", "nr_processo": "0809876-12", "data_contrato": "2026-05-02"},
])
_mapa = fin._mapa_contratos(_repetido)
r.checar("nome repetido não perde contrato", len(_mapa), 2)
r.checar("ids preservados", sorted(_mapa.values()), [9, 11])
r.verdadeiro("desempate usa o processo", any("0801234-55" in k for k in _mapa))

_sem_processo = pd.DataFrame([
    {"id": 9, "cliente": "Maria Silva", "nr_processo": "", "data_contrato": "2026-03-18"},
    {"id": 11, "cliente": "Maria Silva", "nr_processo": "", "data_contrato": "2026-05-02"},
])
_mapa = fin._mapa_contratos(_sem_processo)
r.checar("sem processo, desempata pela data", len(_mapa), 2)
r.verdadeiro("data no rótulo", any("18/03/2026" in k for k in _mapa))

_iguais = pd.DataFrame([
    {"id": 9, "cliente": "Maria Silva", "nr_processo": "", "data_contrato": ""},
    {"id": 11, "cliente": "Maria Silva", "nr_processo": "", "data_contrato": ""},
])
r.checar("idênticos em tudo ainda coexistem", len(fin._mapa_contratos(_iguais)), 2)


r.secao("[14] Resumo financeiro do contrato inteiro")
# Regressão relatada pelo escritório: num contrato que vive da redução da
# liminar (sem honorários iniciais), o cabeçalho de Pagamentos mostrava
# "Valor Total R$ 0,00" e a barra ficava em 0%, mesmo com parcelas recebidas.
# O cálculo olhava só para os honorários iniciais.


def _resumo(**campos):
    base = {
        "inicial_total": 0, "inicial_recebido": 0,
        "liminar_total": 0, "liminar_recebido": 0,
        "exito_fixo": 0, "exito_recebido": 0,
        "exito_pago": 0, "exito_percentual": 0,
    }
    base.update(campos)
    original = fin.select_db
    fin.select_db = lambda *a, **k: pd.DataFrame([base])
    try:
        return fin.resumo_financeiro(1)
    finally:
        fin.select_db = original


# O caso exato do print: 3 parcelas de R$ 3.000, duas pagas, sem iniciais.
so_liminar = _resumo(liminar_total=9000, liminar_recebido=6000)
r.checar("total soma a liminar", so_liminar["total"], 9000.0)
r.checar("recebido conta as parcelas pagas", so_liminar["recebido"], 6000.0)
r.checar("falta o restante", so_liminar["falta"], 3000.0)
r.verdadeiro("barra sai de zero", abs(so_liminar["proporcao"] - 2 / 3) < 0.001)

so_inicial = _resumo(inicial_total=10000, inicial_recebido=5000)
r.checar("contrato só de iniciais continua certo", so_inicial["proporcao"], 0.5)

misto = _resumo(inicial_total=4000, inicial_recebido=4000, liminar_total=6000, liminar_recebido=1500)
r.checar("soma iniciais e liminar", misto["total"], 10000.0)
r.checar("soma os recebimentos dos dois", misto["recebido"], 5500.0)

# Êxito fixo acordado entra no total mesmo antes de ser recebido.
exito_pendente = _resumo(liminar_total=9000, liminar_recebido=9000, exito_fixo=5000)
r.checar("êxito fixo pendente entra no total", exito_pendente["total"], 14000.0)
r.checar("mas não como recebido", exito_pendente["recebido"], 9000.0)

exito_quitado = _resumo(liminar_total=9000, liminar_recebido=9000,
                        exito_fixo=5000, exito_recebido=5000, exito_pago=1)
r.checar("contrato quitado fecha em 100%", exito_quitado["proporcao"], 1.0)

# Êxito por percentual não tem valor conhecido antes do resultado da causa.
# Se entrasse no total como incógnita, a barra nunca fecharia.
percentual = _resumo(liminar_total=9000, liminar_recebido=9000, exito_percentual=20)
r.checar("êxito percentual fica fora do total", percentual["total"], 9000.0)
r.checar("e permite fechar em 100%", percentual["proporcao"], 1.0)

vazio = _resumo()
r.checar("contrato sem valores não quebra", vazio["proporcao"], 0.0)
r.checar("nem divide por zero", vazio["total"], 0.0)


r.secao("[15] Pendências — quando um contrato pode ser arquivado")
# O escritório pediu um botão para arquivar quem já quitou tudo. A régua tem
# de ser a mesma do painel, senão o contrato some de um lugar e fica no outro.


def _pend(**campos):
    base = {
        "saldo_inicial": 0, "liminar_abertas": 0,
        "reducao_sem_parcelas": 0, "exito_em_aberto": 0,
    }
    base.update(campos)
    return fin.pendencias_contrato(base)


r.checar("contrato quitado não tem pendência", _pend(), [])

saldo = _pend(saldo_inicial=3500)
r.checar("saldo inicial vira uma pendência", len(saldo), 1)
r.verdadeiro("e diz quanto falta", "R$ 3.500,00" in saldo[0])

r.verdadeiro("parcela da redução em aberto", "1 parcela" in _pend(liminar_abertas=1)[0])
r.verdadeiro("singular sem 's'", "não recebida" in _pend(liminar_abertas=1)[0])
r.verdadeiro("plural com 's'", "3 parcelas" in _pend(liminar_abertas=3)[0])
r.verdadeiro("redução sem cronograma", "cronograma" in _pend(reducao_sem_parcelas=1)[0])
r.verdadeiro("êxito em aberto", "êxito" in _pend(exito_em_aberto=1)[0])

r.checar(
    "várias pendências aparecem juntas",
    len(_pend(saldo_inicial=100, liminar_abertas=2, exito_em_aberto=1)),
    3,
)

# Valor nulo vindo do banco não pode virar exceção na hora de abrir a tela.
r.checar("campo nulo não quebra", fin.pendencias_contrato({"saldo_inicial": None}), [])
r.checar("campo ausente não quebra", fin.pendencias_contrato({}), [])
r.checar("texto inválido não quebra", fin.pendencias_contrato({"saldo_inicial": "abc"}), [])

# Regressão: saldo negativo acontece quando se recebe a mais. Não é pendência.
r.checar("saldo negativo não é pendência", _pend(saldo_inicial=-50), [])


r.secao("[16] Painel usa o mesmo cálculo das outras telas")
# Regressão relatada: a tabela do painel mostrava "Valor Total R$ 0,00" e
# "Saldo Pendente R$ 0,00" para contratos que em Meus Contratos apareciam com
# valor. As duas telas faziam contas diferentes.

LINHAS_FALSAS = [
    # Só redução, 2 de 3 parcelas pagas — o caso da TF Perfumes.
    {"id": 1, "inicial_total": 0, "inicial_recebido": 0,
     "liminar_total": 9000, "liminar_recebido": 6000,
     "exito_fixo": 0, "exito_recebido": 0, "exito_pago": 0},
    # Iniciais + redução, para conferir que os dois somam.
    {"id": 2, "inicial_total": 15100, "inicial_recebido": 7100,
     "liminar_total": 12000, "liminar_recebido": 4000,
     "exito_fixo": 0, "exito_recebido": 0, "exito_pago": 0},
    # Contrato encerrado, êxito recebido.
    {"id": 3, "inicial_total": 6500, "inicial_recebido": 6500,
     "liminar_total": 0, "liminar_recebido": 0,
     "exito_fixo": 0, "exito_recebido": 12000, "exito_pago": 1},
]

_original = fin.select_db
fin.select_db = lambda *a, **k: pd.DataFrame(LINHAS_FALSAS)
try:
    painel = fin.resumo_por_contrato()
finally:
    fin.select_db = _original

r.checar("uma linha por contrato", len(painel), 3)
r.checar("colunas esperadas", list(painel.columns), ["id", "total", "recebido", "falta"])

linha1 = painel[painel["id"] == 1].iloc[0]
r.checar("contrato só de redução não zera", linha1["total"], 9000.0)
r.checar("e mostra o que falta", linha1["falta"], 3000.0)

linha2 = painel[painel["id"] == 2].iloc[0]
r.checar("iniciais + redução somam", linha2["total"], 27100.0)
r.checar("recebimentos somam", linha2["recebido"], 11100.0)
r.checar("pendente é a diferença", linha2["falta"], 16000.0)

# A soma das colunas do topo sai daqui: o painel mostra o total contratado e
# o total a receber de todos os contratos não arquivados.
r.checar("total contratado do painel", float(painel["total"].sum()), 54600.0)
r.checar("total a receber do painel", float(painel["falta"].sum()), 19000.0)

linha3 = painel[painel["id"] == 3].iloc[0]
r.checar("contrato quitado não tem pendente", linha3["falta"], 0.0)

# A régua tem de ser literalmente a mesma do cabeçalho de Pagamentos.
r.checar(
    "painel e Pagamentos batem",
    fin.totais_contrato(LINHAS_FALSAS[1])["total"],
    float(linha2["total"]),
)

fin.select_db = lambda *a, **k: pd.DataFrame()
try:
    r.checar("banco vazio não quebra o painel", len(fin.resumo_por_contrato()), 0)
finally:
    fin.select_db = _original


r.secao("[17] Contrato pago que continua na lista por causa do êxito")
# Relatado pelo escritório: contrato com "Pago: 100,0%" e "A Receber R$ 0,00"
# aparecendo entre os pendentes. Está certo — falta o êxito, que não tem valor
# conhecido — mas sem explicação parece defeito do sistema.

QUITADO = {"total": 4248.44, "recebido": 4248.44, "falta": 0.0, "proporcao": 1.0}
DEVENDO = {"total": 9000.0, "recebido": 6000.0, "falta": 3000.0, "proporcao": 2 / 3}


def _aviso(contrato, resumo=None):
    return fin.falta_so_exito(contrato, resumo or QUITADO)


so_exito = {"exito_pago": 0, "hon_exito_percentual": 20, "hon_exito_fixo": 0}
texto = _aviso(so_exito)
r.verdadeiro("avisa quando só falta o êxito", bool(texto))
r.verdadeiro("diz o percentual combinado", "20,0%" in texto)
r.verdadeiro("aponta a aba de êxito", "Êxito" in texto)
r.verdadeiro("oferece arquivar", "Arquivar" in texto)

# Ainda há parcela a receber: o contrato está na lista pelo motivo óbvio.
r.checar("não avisa quando ainda falta dinheiro", _aviso(so_exito, DEVENDO), "")

r.checar(
    "não avisa com êxito já recebido",
    _aviso({"exito_pago": 1, "hon_exito_percentual": 20, "hon_exito_fixo": 0}),
    "",
)
r.checar(
    "não avisa quando não há êxito combinado",
    _aviso({"exito_pago": 0, "hon_exito_percentual": 0, "hon_exito_fixo": 0}),
    "",
)

# Êxito de valor fixo: mostra o valor, não um percentual.
fixo = _aviso({"exito_pago": 0, "hon_exito_percentual": 0, "hon_exito_fixo": 45000})
r.verdadeiro("êxito fixo aparece como valor", "45.000,00" in fixo)
# O markdown do Streamlit leria "R$ ... $" como fórmula LaTeX.
r.verdadeiro("cifrão escapado para o markdown", r"R\$" in fixo)

r.checar("campos ausentes não quebram", _aviso({}), "")
r.checar(
    "valor nulo vindo do banco não quebra",
    _aviso({"exito_pago": None, "hon_exito_percentual": None, "hon_exito_fixo": None}),
    "",
)


sys.exit(r.encerrar())
