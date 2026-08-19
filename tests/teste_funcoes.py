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


sys.exit(r.encerrar())
