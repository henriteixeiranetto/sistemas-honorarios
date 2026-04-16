import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, date
import time
import io
import urllib.parse
import re
from fpdf import FPDF
 
# =============================================================================
# 1. CONFIGURAÇÕES E ESTILO
# =============================================================================
st.set_page_config(page_title="Sistema de Honorários", layout="wide", page_icon="⚖️")
 
st.markdown("""
    <style>
        .stMetric { background-color: #f8f9fa; padding: 15px; border-radius: 10px; border: 1px solid #dee2e6; }
        button[kind="primary"] { width: 100%; height: 3em; font-weight: bold; }
        .observacao-box { background-color: #fff3cd; padding: 10px; border-radius: 5px; border-left: 5px solid #ffca2c; margin-bottom: 20px; }
        .info-cliente { background-color: #f0f2f6; padding: 15px; border-radius: 8px; margin-bottom: 10px; border-left: 5px solid #007bff; }
        .secao-form { background-color: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 4px solid #007bff; margin-bottom: 10px; }
    </style>
""", unsafe_allow_html=True)
 
# =============================================================================
# 2. CONEXÃO COM SUPABASE (PostgreSQL)
# =============================================================================
def criar_conexao():
    import os
    # Railway: variáveis de ambiente
    # Streamlit Cloud: st.secrets
    host     = os.environ.get("SUPABASE_HOST")     or st.secrets["supabase"]["host"]
    port     = os.environ.get("SUPABASE_PORT")     or st.secrets["supabase"]["port"]
    dbname   = os.environ.get("SUPABASE_DBNAME")   or st.secrets["supabase"]["dbname"]
    user     = os.environ.get("SUPABASE_USER")     or st.secrets["supabase"]["user"]
    password = os.environ.get("SUPABASE_PASSWORD") or st.secrets["supabase"]["password"]
    return psycopg2.connect(
        host            = host,
        port            = port,
        dbname          = dbname,
        user            = user,
        password        = password,
        sslmode         = "require",
        connect_timeout = 10,
        keepalives      = 1,
        keepalives_idle = 30,
    )
 
@st.cache_resource(show_spinner=False)
def get_conn():
    return {"conn": criar_conexao()}
 
def _conn():
    cache = get_conn()
    try:
        cache["conn"].isolation_level
    except Exception:
        cache["conn"] = criar_conexao()
    return cache["conn"]
 
def exec_db(query, params=()):
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            conn.commit()
    except Exception as e:
        conn.rollback()
        st.error(f"Erro no banco: {e}")
 
def exec_retorna(query, params=()):
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            conn.commit()
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as e:
        conn.rollback()
        st.error(f"Erro no banco: {e}")
        return None
 
def select_db(query, params=()):
    conn = _conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame([dict(r) for r in rows])
            return df.where(pd.notnull(df), "")
    except Exception as e:
        conn.rollback()
        st.error(f"Erro no banco: {e}")
        return pd.DataFrame()
 
# =============================================================================
# 3. BANCO DE DADOS — INICIALIZAÇÃO
# =============================================================================
def inicializar_banco():
    # Tabela principal de contratos
    exec_db("""
        CREATE TABLE IF NOT EXISTS contratos (
            id                      SERIAL PRIMARY KEY,
            cliente                 TEXT NOT NULL,
            cpf_cnpj                TEXT,
            telefone                TEXT,
            valor_total             REAL NOT NULL,
            saldo_devedor           REAL NOT NULL,
            data_contrato           TEXT NOT NULL,
            observacoes             TEXT,
 
            -- Honorários Iniciais
            hon_inicial_ativo       TEXT,
            hon_inicial_valor       REAL,
            hon_inicial_parcelado   TEXT,
            hon_inicial_parcelas    INTEGER,
            hon_inicial_vlr_parcela REAL,
 
            -- Honorários da Liminar
            hon_liminar_fixo        REAL,
            hon_liminar_reducao_vlr REAL,
            hon_liminar_reducao_prc INTEGER,
            tutela                  TEXT,
 
            -- Honorários de Êxito
            hon_exito_percentual    REAL,
            hon_exito_fixo          REAL,
 
            -- Dados do Processo
            nr_processo             TEXT,
            nr_vara                 TEXT,
            nome_juiz               TEXT,
            comarca                 TEXT
        )
    """)
 
    # Adiciona colunas novas em tabelas que já existem no Supabase
    # (sem erro se já existirem)
    novas_colunas = [
        ("hon_inicial_ativo",       "TEXT"),
        ("hon_inicial_valor",       "REAL"),
        ("hon_inicial_parcelado",   "TEXT"),
        ("hon_inicial_parcelas",    "INTEGER"),
        ("hon_inicial_vlr_parcela", "REAL"),
        ("hon_liminar_fixo",        "REAL"),
        ("hon_liminar_reducao_vlr", "REAL"),
        ("hon_liminar_reducao_prc", "INTEGER"),
        ("hon_exito_percentual",    "REAL"),
        ("hon_exito_fixo",          "REAL"),
        ("nr_processo",             "TEXT"),
        ("nr_vara",                 "TEXT"),
        ("nome_juiz",               "TEXT"),
        ("comarca",                 "TEXT"),
        ("exito_pago",              "INTEGER"),
        ("exito_data_pagamento",    "TEXT"),
        ("exito_valor_recebido",    "REAL"),
    ]
    conn = _conn()
    for col, tipo in novas_colunas:
        try:
            with conn.cursor() as cur:
                cur.execute(f"ALTER TABLE contratos ADD COLUMN IF NOT EXISTS {col} {tipo}")
                conn.commit()
        except Exception:
            conn.rollback()
 
    # Tabela de parcelas (honorários iniciais)
    exec_db("""
        CREATE TABLE IF NOT EXISTS parcelas (
            id              SERIAL PRIMARY KEY,
            contrato_id     INTEGER NOT NULL
                                REFERENCES contratos(id) ON DELETE CASCADE,
            nr_parcela      INTEGER NOT NULL,
            valor_parcela   REAL NOT NULL,
            data_vencimento TEXT NOT NULL,
            data_pagamento  TEXT,
            pago            INTEGER DEFAULT 0,
            forma_pagamento TEXT
        )
    """)

    # Tabela de parcelas da redução da liminar (independente das parcelas de honorários)
    exec_db("""
        CREATE TABLE IF NOT EXISTS parcelas_liminar (
            id             SERIAL PRIMARY KEY,
            contrato_id    INTEGER NOT NULL
                               REFERENCES contratos(id) ON DELETE CASCADE,
            nr_parcela     INTEGER NOT NULL,
            valor_parcela  REAL NOT NULL,
            data_prevista  TEXT NOT NULL,
            data_pagamento TEXT,
            pago           INTEGER DEFAULT 0
        )
    """)

    # Garante coluna tutela na tabela contratos (segurança para BDs antigos)
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE contratos ADD COLUMN IF NOT EXISTS tutela TEXT")
            conn.commit()
    except Exception:
        conn.rollback()
 
if 'banco_ok' not in st.session_state:
    inicializar_banco()
    st.session_state['banco_ok'] = True
 
# =============================================================================
# 4. FUNÇÕES DE VALIDAÇÃO, FORMATAÇÃO E EXPORTAÇÃO
# =============================================================================
def validar_cpf(cpf):
    cpf = re.sub(r'\D', '', str(cpf))
    if len(cpf) != 11 or len(set(cpf)) == 1: return False
    for i in range(9, 11):
        soma = sum(int(cpf[num]) * ((i + 1) - num) for num in range(0, i))
        if (soma * 10 % 11) % 10 != int(cpf[i]): return False
    return True
 
def validar_cnpj(cnpj):
    cnpj = re.sub(r'\D', '', str(cnpj))
    if len(cnpj) != 14 or len(set(cnpj)) == 1: return False
    def calcular_digito(n):
        pesos = [5,4,3,2,9,8,7,6,5,4,3,2] if n == 12 else [6,5,4,3,2,9,8,7,6,5,4,3,2]
        soma  = sum(int(cnpj[i]) * pesos[i] for i in range(n)) % 11
        return 0 if soma < 2 else 11 - soma
    return calcular_digito(12) == int(cnpj[12]) and calcular_digito(13) == int(cnpj[13])
 
def nulo(v):
    return not v or str(v).strip() in ("", "None", "nan", "NaT")
 
def formatar_cpf_cnpj(valor):
    if nulo(valor): return "-"
    num = re.sub(r'\D', '', str(valor))
    if len(num) == 11: return f"{num[:3]}.{num[3:6]}.{num[6:9]}-{num[9:]}"
    if len(num) == 14: return f"{num[:2]}.{num[2:5]}.{num[5:8]}/{num[8:12]}-{num[12:]}"
    return str(valor)
 
def formatar_telefone(valor):
    if nulo(valor): return "-"
    num = re.sub(r'\D', '', str(valor))
    if len(num) == 11: return f"({num[:2]}) {num[2:7]}-{num[7:]}"
    if len(num) == 10: return f"({num[:2]}) {num[2:6]}-{num[6:]}"
    return str(valor)
 
def formatar_data(data):
    if nulo(data): return "-"
    try:
        s = str(data).strip()
        if len(s) > 10:
            return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y %H:%M")
        return datetime.strptime(s[:10], "%Y-%m-%d").strftime("%d/%m/%Y")
    except:
        return "-"
 
def obter_status_parcela(pago, data_vencimento):
    if int(pago) == 1: return "🟢 Pago"
    try:
        venc = datetime.strptime(str(data_vencimento)[:10], "%Y-%m-%d").date()
        if venc < date.today():
            return f"🔴 Atrasado ({(date.today() - venc).days} dias)"
    except:
        pass
    return "🟡 Pendente"
 
def gerar_excel(df):
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Relatorio')
    return buffer.getvalue()
 
def gerar_pdf(df, titulo):
    pdf = FPDF(orientation="L")
    pdf.set_auto_page_break(auto=True, margin=12)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, titulo, align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 6, f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}",
             align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)
    if df.empty:
        pdf.set_font("Helvetica", "I", 10)
        pdf.cell(0, 10, "Nenhum registro encontrado.", align="C", new_x="LMARGIN", new_y="NEXT")
        return bytes(pdf.output())
    colunas      = list(df.columns)
    largura_util = pdf.w - pdf.l_margin - pdf.r_margin
    col_w        = largura_util / len(colunas)
    row_h        = 7
    pdf.set_fill_color(30, 77, 216)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 8)
    for col in colunas:
        pdf.cell(col_w, row_h, str(col), border=1, align="C", fill=True)
    pdf.ln()
    pdf.set_text_color(30, 30, 30)
    pdf.set_font("Helvetica", "", 8)
    fill = False
    for _, row in df.iterrows():
        pdf.set_fill_color(240, 244, 255) if fill else pdf.set_fill_color(255, 255, 255)
        for item in row:
            texto = "-" if nulo(item) else (f"R$ {item:,.2f}" if isinstance(item, float) else str(item))
            while pdf.get_string_width(texto) > col_w - 2 and len(texto) > 3:
                texto = texto[:-4] + "..."
            pdf.cell(col_w, row_h, texto, border=1, align="C", fill=True)
        pdf.ln()
        fill = not fill
    return bytes(pdf.output())
 
# =============================================================================
# 5. LOGIN E NAVEGAÇÃO
# =============================================================================
if 'autenticado' not in st.session_state:
    st.session_state['autenticado'] = False
 
if not st.session_state['autenticado']:
    col_l, col_c, col_r = st.columns([1, 2, 1])
    with col_c:
        st.markdown("<h2 style='text-align: center;'>⚖️ Sistema de Honorários</h2>", unsafe_allow_html=True)
        u = st.text_input("Usuário")
        s = st.text_input("Senha", type="password")
        if st.button("Entrar", type="primary"):
            import os
            cred_usuario = os.environ.get("CRED_USUARIO") or st.secrets["credenciais"]["usuario"]
            cred_senha   = os.environ.get("CRED_SENHA")   or st.secrets["credenciais"]["senha"]
            if u == cred_usuario and s == cred_senha:
                st.session_state['autenticado'] = True
                st.rerun()
            else:
                st.error("Credenciais inválidas")
    st.stop()
 
opcoes_menu = ["📊 Dashboard", "➕ Novo Contrato", "💰 Pagamentos", "📂 Meus Contratos", "📁 Arquivados", "⚙️ Gestão"]
if 'rad_nav' not in st.session_state:
    st.session_state['rad_nav'] = "📊 Dashboard"
aba = st.sidebar.radio("Navegação", opcoes_menu, key="rad_nav")
 
# =============================================================================
# 6. INTERFACE PRINCIPAL
# =============================================================================
 
# --- DASHBOARD ---
if aba == "📊 Dashboard":
    st.header("Resumo Financeiro")
    df_c = select_db("SELECT * FROM contratos ORDER BY cliente ASC")
 
    if not df_c.empty:
        df_c['valor_total']   = pd.to_numeric(df_c['valor_total'],   errors='coerce').fillna(0)
        df_c['saldo_devedor'] = pd.to_numeric(df_c['saldo_devedor'], errors='coerce').fillna(0)
        df_ativos = df_c[df_c['saldo_devedor'] > 0].copy()
 
        m1, m2, m3 = st.columns(3)
        m1.metric("Contratado Geral",    f"R$ {df_c['valor_total'].sum():,.2f}")
        m2.metric("Saldo Devedor Total", f"R$ {df_c['saldo_devedor'].sum():,.2f}")
        m3.metric("Contratos Ativos",    len(df_ativos))
        st.divider()
 
        df_alertas = select_db("""
            SELECT c.cliente, c.telefone, c.saldo_devedor,
                   p.nr_parcela, p.valor_parcela, p.data_vencimento
            FROM parcelas p
            JOIN contratos c ON p.contrato_id = c.id
            WHERE p.pago = 0
        """)
        if not df_alertas.empty:
            hoje = pd.to_datetime(date.today())
            df_alertas['venc_date'] = pd.to_datetime(df_alertas['data_vencimento'], errors='coerce')
            df_vencidos = df_alertas[df_alertas['venc_date'] < hoje].copy()
            if not df_vencidos.empty:
                df_vencidos['Dias Atraso']   = (hoje - df_vencidos['venc_date']).dt.days
                df_vencidos['saldo_devedor'] = pd.to_numeric(df_vencidos['saldo_devedor'], errors='coerce')
                df_vencidos['valor_parcela'] = pd.to_numeric(df_vencidos['valor_parcela'], errors='coerce')
                df_resumo = df_vencidos.groupby(['cliente','telefone','saldo_devedor']).agg(
                    qtd_parcelas    = ('nr_parcela',    'count'),
                    valor_atrasado  = ('valor_parcela', 'sum'),
                    dias_max_atraso = ('Dias Atraso',   'max'),
                ).reset_index()
                st.error(f"🚨 Atenção: {len(df_resumo)} cliente(s) em inadimplência! "
                         f"Total atrasado: **R$ {df_resumo['valor_atrasado'].sum():,.2f}**")
                df_resumo['telefone'] = df_resumo['telefone'].apply(formatar_telefone)
                df_resumo.columns = ['Cliente','Telefone','Falta Pagar (Total)',
                                     'Parcelas Atrasadas','Valor Atrasado','Dias do Pior Atraso']
                st.dataframe(df_resumo, use_container_width=True, hide_index=True,
                             column_config={
                                 "Falta Pagar (Total)": st.column_config.NumberColumn(format="R$ %.2f"),
                                 "Valor Atrasado":      st.column_config.NumberColumn(format="R$ %.2f"),
                             })
                st.divider()
 
        if not df_ativos.empty:
            col_t, col_a = st.columns([2, 1])
            col_t.subheader("Contratos Ativos (Em Aberto)")
            with col_a:
                st.markdown("⚡ **Atalho Rápido:**")
                cliente_map_dash    = {f"{r['cliente']} (Contrato #{r['id']})": r['id']
                                       for _, r in df_ativos.iterrows()}
                cliente_selecionado = st.selectbox("Selecione o cliente:",
                                                   options=list(cliente_map_dash.keys()),
                                                   label_visibility="collapsed")
                def ir_para_pagamentos():
                    st.session_state['cliente_foco'] = cliente_map_dash[cliente_selecionado]
                    st.session_state['rad_nav'] = "💰 Pagamentos"
                st.button("Ir para Pagamento ➡", type="primary", on_click=ir_para_pagamentos)
 
            df_ativos['data_contrato'] = df_ativos['data_contrato'].apply(formatar_data)
            df_ativos['cpf_cnpj']      = df_ativos['cpf_cnpj'].apply(formatar_cpf_cnpj)
            df_ativos['telefone']      = df_ativos['telefone'].apply(formatar_telefone)
            df_ativos['observacoes']   = df_ativos['observacoes'].apply(lambda x: "-" if nulo(x) else str(x))
            df_view = df_ativos[['cliente','cpf_cnpj','telefone','data_contrato',
                                 'valor_total','saldo_devedor','observacoes']].copy()
            df_view.columns = ['Cliente','CPF/CNPJ','Telefone','Data Contrato',
                               'Valor Total','Saldo Pendente','Observações']
            st.dataframe(df_view, use_container_width=True, hide_index=True,
                         column_config={
                             "Valor Total":    st.column_config.NumberColumn("Total",    format="R$ %.2f"),
                             "Saldo Pendente": st.column_config.NumberColumn("Pendente", format="R$ %.2f"),
                         })
            st.divider()
            col_exp1, col_exp2 = st.columns(2)
            with col_exp1:
                st.download_button("📥 Exportar Ativos para Excel",
                    data=gerar_excel(df_view),
                    file_name=f"contratos_ativos_{date.today()}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            with col_exp2:
                st.download_button("📄 Exportar Ativos para PDF",
                    data=gerar_pdf(df_view, "Relatório de Contratos Ativos"),
                    file_name=f"contratos_ativos_{date.today()}.pdf",
                    mime="application/pdf")
        else:
            st.success("Todos os clientes estão com as contas em dia!")
    else:
        st.info("Nenhum contrato registrado.")
 
# --- NOVO CONTRATO ---
elif aba == "➕ Novo Contrato":
    st.header("Cadastrar Novo Contrato")
 
    # ------------------------------------------------------------------
    # INFORMAÇÕES BÁSICAS
    # ------------------------------------------------------------------
    st.subheader("📋 Informações Básicas")
    col1, col2 = st.columns(2)
    nome    = col1.text_input("Nome do Cliente")
    cpf_raw = col2.text_input("CPF ou CNPJ (Somente números)", placeholder="Ex: 00000000000")
    tel_raw = col1.text_input("Telefone (Somente números)", placeholder="Ex: 11999998888")
    data_c  = col2.date_input("Data do Contrato", value=date.today())
 
    st.divider()
 
    # ------------------------------------------------------------------
    # HONORÁRIOS INICIAIS
    # ------------------------------------------------------------------
    st.subheader("💰 Honorários Iniciais")
    col1, col2 = st.columns(2)
    hon_ini_ativo     = col1.selectbox("Há cobrança inicial?", ["Não", "Sim"], key="hon_ini_ativo")
    hon_ini_valor     = col2.number_input("Valor Total (R$)", min_value=0.0, step=100.0,
                                          format="%.2f", key="hon_ini_valor")
    hon_ini_parcelado = col1.selectbox("Pagamento parcelado?", ["Não", "Sim"], key="hon_ini_parc")
    hon_ini_parcelas  = 1
    hon_ini_vlr_parc  = 0.0
 
    if hon_ini_parcelado == "Sim" and hon_ini_valor > 0:
        col3, col4 = st.columns(2)
        hon_ini_parcelas = col3.number_input("Quantidade de Parcelas", min_value=1,
                                              max_value=60, value=1, step=1, key="hon_ini_qtd")
        hon_ini_vlr_parc = round(hon_ini_valor / hon_ini_parcelas, 2) if hon_ini_parcelas > 0 else 0.0
        col4.metric("Valor de Cada Parcela", f"R$ {hon_ini_vlr_parc:,.2f}")
 
    # Valor total usado no restante do sistema = honorários iniciais
    valor = hon_ini_valor
 
    st.divider()
 
    # ------------------------------------------------------------------
    # HONORÁRIOS DA LIMINAR
    # ------------------------------------------------------------------
    st.subheader("⚖️ Honorários da Liminar")
    col1, col2 = st.columns(2)
    tutela             = col1.selectbox("Status da Tutela",
                                        ["Pendente", "Deferido", "Indeferido", "Parcial"])
    hon_lim_fixo       = col2.number_input("Honorários Fixos da Liminar (R$)",
                                            min_value=0.0, step=100.0, format="%.2f", key="hon_lim_fixo")
    col3, col4 = st.columns(2)
    hon_lim_red_vlr    = col3.number_input("Valor Efetivo da Redução Obtida (R$)",
                                            min_value=0.0, step=100.0, format="%.2f", key="hon_lim_red_vlr")
    hon_lim_red_prc    = col4.number_input("Nº de Parcelas da Redução",
                                            min_value=0, max_value=360, value=0, step=1, key="hon_lim_red_prc")
 
    st.divider()
 
    # ------------------------------------------------------------------
    # HONORÁRIOS DE ÊXITO
    # ------------------------------------------------------------------
    st.subheader("🏆 Honorários de Êxito")
    col1, col2 = st.columns(2)
    hon_exito_pct  = col1.number_input("Percentual de Êxito (%)", min_value=0.0,
                                        max_value=100.0, step=0.5, format="%.2f", key="hon_ex_pct")
    hon_exito_fixo = col2.number_input("Valor Fixo de Êxito (R$)", min_value=0.0,
                                        step=100.0, format="%.2f", key="hon_ex_fixo")
 
    st.divider()
 
    # ------------------------------------------------------------------
    # DADOS DO PROCESSO
    # ------------------------------------------------------------------
    st.subheader("📁 Dados do Processo")
    col1, col2 = st.columns(2)
    nr_processo = col1.text_input("Número do Processo", placeholder="Ex: 0000000-00.0000.0.00.0000")
    nr_vara     = col2.text_input("Número da Vara", placeholder="Ex: 3ª Vara Cível")
    col3, col4 = st.columns(2)
    nome_juiz   = col3.text_input("Nome do Juiz")
    comarca     = col4.text_input("Comarca")
    obs         = st.text_area("Observações (anotações extras)")
 
    st.divider()
 
    # ------------------------------------------------------------------
    # PARCELAMENTO DOS HONORÁRIOS INICIAIS (para controle de pagamentos)
    # ------------------------------------------------------------------
    if valor > 0:
        st.subheader("📅 Parcelamento para Controle de Pagamentos")
        opcoes  = ([f"À vista: R$ {valor:,.2f}"] +
                   [f"R$ {valor:,.2f} ou {i}x de R$ {valor/i:,.2f} sem juros" for i in range(2, 11)])
        selecao = st.selectbox("Como deseja controlar as parcelas no sistema?", opcoes)
        n_p     = 1 if "À vista" in selecao else int(selecao.split(" ou ")[1].split("x")[0])
 
    if st.button("Salvar Contrato", type="primary"):
        doc_limpo = re.sub(r'\D', '', cpf_raw)
        if not nome:
            st.error("Nome obrigatório.")
        elif len(doc_limpo) == 11 and not validar_cpf(doc_limpo):
            st.error("CPF inválido! Por favor, insira um documento real.")
        elif len(doc_limpo) == 14 and not validar_cnpj(doc_limpo):
            st.error("CNPJ inválido! Por favor, insira um documento real.")
        elif len(doc_limpo) not in [11, 14]:
            st.error("O documento deve ter 11 dígitos (CPF) ou 14 dígitos (CNPJ).")
        else:
            cpf_fmt   = formatar_cpf_cnpj(doc_limpo)
            tel_fmt   = formatar_telefone(tel_raw)
            obs_val   = obs.strip() or None
            valor_sal = valor if valor > 0 else 0.0
            n_p_sal   = n_p if valor > 0 else 1
 
            c_id = exec_retorna(
                """INSERT INTO contratos
                   (cliente, cpf_cnpj, telefone, valor_total, saldo_devedor, data_contrato,
                    observacoes, tutela,
                    hon_inicial_ativo, hon_inicial_valor, hon_inicial_parcelado,
                    hon_inicial_parcelas, hon_inicial_vlr_parcela,
                    hon_liminar_fixo, hon_liminar_reducao_vlr, hon_liminar_reducao_prc,
                    hon_exito_percentual, hon_exito_fixo,
                    nr_processo, nr_vara, nome_juiz, comarca)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   RETURNING id""",
                (nome, cpf_fmt, tel_fmt, valor_sal, valor_sal,
                 data_c.strftime("%Y-%m-%d"), obs_val, tutela,
                 hon_ini_ativo, hon_ini_valor if hon_ini_ativo == "Sim" else None,
                 hon_ini_parcelado,
                 hon_ini_parcelas if hon_ini_parcelado == "Sim" else None,
                 hon_ini_vlr_parc if hon_ini_parcelado == "Sim" else None,
                 hon_lim_fixo or None, hon_lim_red_vlr or None, hon_lim_red_prc or None,
                 hon_exito_pct or None, hon_exito_fixo or None,
                 nr_processo.strip() or None, nr_vara.strip() or None,
                 nome_juiz.strip() or None, comarca.strip() or None)
            )
 
            if valor_sal > 0:
                v_base = round(valor_sal / n_p_sal, 2)
                for i in range(1, n_p_sal + 1):
                    v_f  = round(valor_sal - (v_base * (n_p_sal - 1)), 2) if i == n_p_sal else v_base
                    venc = data_c + pd.DateOffset(months=i - 1)
                    exec_db(
                        "INSERT INTO parcelas (contrato_id, nr_parcela, valor_parcela, data_vencimento) VALUES (%s,%s,%s,%s)",
                        (c_id, i, v_f, venc.strftime("%Y-%m-%d"))
                    )
 
            st.success("Contrato cadastrado com sucesso!")
            time.sleep(1)
            st.rerun()
 
# --- PAGAMENTOS ---
elif aba == "💰 Pagamentos":
    st.header("Registrar Recebimento")

    if 'ultimo_recibo' in st.session_state:
        with st.container(border=True):
            st.subheader("📄 Recibo Gerado")
            st.code(st.session_state['ultimo_recibo'], language="text")
            tel_limpo = "".join(filter(str.isdigit, st.session_state['tel_cliente']))
            texto_url = urllib.parse.quote(st.session_state['ultimo_recibo'])
            st.link_button("📲 Enviar Recibo por WhatsApp",
                           f"https://wa.me/55{tel_limpo}?text={texto_url}", type="primary")
            if st.button("Limpar Tela"):
                del st.session_state['ultimo_recibo']
                del st.session_state['tel_cliente']
                st.rerun()
        st.divider()

    df_contratos = select_db("SELECT * FROM contratos WHERE saldo_devedor > 0 ORDER BY cliente ASC")
    if df_contratos.empty:
        st.info("Não há contratos pendentes.")
    else:
        df_contratos['valor_total']   = pd.to_numeric(df_contratos['valor_total'],   errors='coerce').fillna(0)
        df_contratos['saldo_devedor'] = pd.to_numeric(df_contratos['saldo_devedor'], errors='coerce').fillna(0)

        cliente_map     = {f"{r['cliente']} (Contrato #{r['id']})": r['id']
                           for _, r in df_contratos.iterrows()}
        opcoes_dropdown = list(cliente_map.keys())

        if 'cliente_foco' in st.session_state:
            foco_id = st.session_state.pop('cliente_foco')
            for k in opcoes_dropdown:
                if cliente_map[k] == foco_id:
                    st.session_state['select_cliente'] = k
                    break

        nome_sel = st.selectbox("Selecione o Cliente", options=opcoes_dropdown, key='select_cliente')
        id_sel   = cliente_map[nome_sel]
        resumo   = df_contratos[df_contratos['id'] == id_sel].iloc[0]

        st.markdown(f"""
            <div class='info-cliente'>
                <b>Dados do Cliente:</b><br>
                👤 Nome: {resumo['cliente']} | 💳 Doc: {formatar_cpf_cnpj(resumo['cpf_cnpj'])}<br>
                📞 Tel: {formatar_telefone(resumo['telefone'])}
            </div>
        """, unsafe_allow_html=True)

        infos_proc = []
        if not nulo(resumo.get('nr_processo',  '')): infos_proc.append(f"📄 Processo: {resumo['nr_processo']}")
        if not nulo(resumo.get('nr_vara',       '')): infos_proc.append(f"🏛️ Vara: {resumo['nr_vara']}")
        if not nulo(resumo.get('nome_juiz',     '')): infos_proc.append(f"👨‍⚖️ Juiz: {resumo['nome_juiz']}")
        if not nulo(resumo.get('comarca',       '')): infos_proc.append(f"📍 Comarca: {resumo['comarca']}")
        if infos_proc:
            st.markdown(" &nbsp;|&nbsp; ".join(infos_proc))

        vt = float(resumo['valor_total'])
        sd = float(resumo['saldo_devedor'])
        col_r1, col_r2 = st.columns(2)
        col_r1.metric("Valor Total",    f"R$ {vt:,.2f}")
        col_r2.metric("Saldo Restante", f"R$ {sd:,.2f}")
        pct = float(max(0.0, min(1.0, (vt - sd) / vt))) if vt > 0 else 0.0
        st.progress(pct, text=f"Progresso de Pagamento: {pct:.1%}")

        if not nulo(resumo['observacoes']):
            st.markdown(f"<div class='observacao-box'><b>Notas:</b> {resumo['observacoes']}</div>",
                        unsafe_allow_html=True)

        tab_ini, tab_lim, tab_exit, tab_comb = st.tabs([
            "💰 Honorários Iniciais", "⚖️ Liminar / Redução", "🏆 Êxito", "📋 Combinado"
        ])

        # ── TAB 1: HONORÁRIOS INICIAIS ────────────────────────────────────
        with tab_ini:
            df_parc = select_db(
                "SELECT * FROM parcelas WHERE contrato_id = %s ORDER BY nr_parcela", (int(id_sel),))
            if df_parc.empty:
                st.info("Nenhuma parcela de honorários iniciais cadastrada.")
            else:
                df_parc['pago']          = pd.to_numeric(df_parc['pago'], errors='coerce').fillna(0).astype(int)
                df_parc['valor_parcela'] = pd.to_numeric(df_parc['valor_parcela'], errors='coerce')

                df_view                    = df_parc.copy()
                df_view['Status']          = df_view.apply(
                    lambda r: obter_status_parcela(r['pago'], r['data_vencimento']), axis=1)
                df_view['Vencimento']      = df_view['data_vencimento'].apply(formatar_data)
                df_view['forma_pagamento'] = df_view['forma_pagamento'].apply(
                    lambda x: "-" if nulo(x) else str(x))

                st.dataframe(df_view, use_container_width=True, hide_index=True,
                             column_order=['nr_parcela','valor_parcela','Vencimento','Status','forma_pagamento'],
                             column_config={
                                 "valor_parcela":   st.column_config.NumberColumn("Valor",   format="R$ %.2f"),
                                 "nr_parcela":      "Parcela",
                                 "forma_pagamento": "Método",
                             })

                pendentes = df_parc[df_parc['pago'] == 0]
                if not pendentes.empty:
                    col_p, col_v = st.columns(2)
                    opcoes_parc  = {f"Parc {r.nr_parcela} (Venc: {formatar_data(r.data_vencimento)})": r.nr_parcela
                                    for _, r in pendentes.iterrows()}
                    parc_label   = col_p.selectbox("Qual parcela pagar?", options=list(opcoes_parc.keys()),
                                                   key=f"ini_parc_{id_sel}")
                    n_p_f        = opcoes_parc[parc_label]
                    valor_pago   = col_v.number_input("Valor Recebido (R$)",
                        value=float(pendentes[pendentes['nr_parcela'] == n_p_f]['valor_parcela'].values[0]),
                        format="%.2f", key=f"ini_vlr_{id_sel}")
                    forma_p = st.selectbox("Método de Recebimento",
                                           ["Pix","Dinheiro","Transferência","Cartão","Boleto"],
                                           key=f"ini_forma_{id_sel}")

                    if st.button("Confirmar Pagamento", type="primary", key=f"ini_btn_{id_sel}"):
                        novo_saldo = round(sd - valor_pago, 2)
                        exec_db(
                            "UPDATE parcelas SET pago=1, data_pagamento=%s, forma_pagamento=%s WHERE contrato_id=%s AND nr_parcela=%s",
                            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), forma_p, id_sel, n_p_f)
                        )
                        exec_db("UPDATE contratos SET saldo_devedor=%s WHERE id=%s", (novo_saldo, id_sel))
                        if novo_saldo <= 0:
                            exec_db("UPDATE contratos SET observacoes='Pago' WHERE id=%s", (int(id_sel),))
                            st.balloons()
                        st.session_state['ultimo_recibo'] = (
                            f"⚖️ *RECIBO DE HONORÁRIOS*\n"
                            f"---------------------------------------\n"
                            f"*Cliente:* {resumo['cliente']}\n"
                            f"*CPF/CNPJ:* {formatar_cpf_cnpj(resumo['cpf_cnpj'])}\n"
                            f"*Referente:* Parcela {n_p_f} — Honorários Iniciais\n"
                            f"*Valor Pago:* R$ {valor_pago:,.2f}\n"
                            f"*Data/Hora:* {datetime.now().strftime('%d/%m/%Y %H:%M')}\n"
                            f"*Método:* {forma_p}\n"
                            f"---------------------------------------\n"
                            f"✅ *Situação Atual:*\n"
                            f"*Saldo Devedor Restante:* R$ {max(novo_saldo, 0):,.2f}\n"
                            f"---------------------------------------\n"
                            f"Obrigado pela confiança!"
                        )
                        st.session_state['tel_cliente'] = str(resumo['telefone'])
                        st.rerun()
                else:
                    st.success("🎉 Todas as parcelas iniciais já foram pagas!")

        # ── TAB 2: LIMINAR / REDUÇÃO ──────────────────────────────────────
        with tab_lim:
            df_plim_pag = select_db(
                "SELECT * FROM parcelas_liminar WHERE contrato_id = %s ORDER BY nr_parcela", (int(id_sel),))
            tutela_res = str(resumo.get('tutela', '') or 'Pendente')
            if df_plim_pag.empty:
                if tutela_res in ("Deferido", "Parcial"):
                    st.info("Tutela deferida, mas as parcelas da redução ainda não foram cadastradas. "
                            "Acesse **📂 Meus Contratos** para criar as parcelas.")
                else:
                    st.info(f"Status da tutela: **{tutela_res}**. "
                            "Quando a tutela for deferida, cadastre as parcelas em **📂 Meus Contratos**.")
            else:
                df_plim_pag['pago']          = pd.to_numeric(df_plim_pag['pago'], errors='coerce').fillna(0).astype(int)
                df_plim_pag['valor_parcela'] = pd.to_numeric(df_plim_pag['valor_parcela'], errors='coerce')

                df_plim_view                = df_plim_pag.copy()
                df_plim_view['Status']      = df_plim_view.apply(
                    lambda r: "🟢 Recebido" if r['pago'] == 1 else obter_status_parcela(r['pago'], r['data_prevista']),
                    axis=1)
                df_plim_view['Previsão']    = df_plim_view['data_prevista'].apply(formatar_data)
                df_plim_view['Recebido em'] = df_plim_view['data_pagamento'].apply(formatar_data)

                st.dataframe(df_plim_view, use_container_width=True, hide_index=True,
                             column_order=['nr_parcela','valor_parcela','Previsão','Recebido em','Status'],
                             column_config={
                                 "nr_parcela":    "Parcela",
                                 "valor_parcela": st.column_config.NumberColumn("Valor", format="R$ %.2f"),
                             })

                pend_lim = df_plim_pag[df_plim_pag['pago'] == 0]
                if not pend_lim.empty:
                    col_lp, col_lv = st.columns(2)
                    opcoes_lim_pag = {
                        f"Parcela {r.nr_parcela} — Prev. {formatar_data(r.data_prevista)} — R$ {r.valor_parcela:,.2f}": r.nr_parcela
                        for _, r in pend_lim.iterrows()
                    }
                    lim_pag_label = col_lp.selectbox("Qual parcela recebeu?",
                                                     options=list(opcoes_lim_pag.keys()),
                                                     key=f"lim_pag_sel_{id_sel}")
                    n_lim_pag  = opcoes_lim_pag[lim_pag_label]
                    vlr_lim_pag = float(pend_lim[pend_lim['nr_parcela'] == n_lim_pag]['valor_parcela'].values[0])
                    vlr_lim_rec = col_lv.number_input("Valor Recebido (R$)", value=vlr_lim_pag,
                                                      format="%.2f", key=f"lim_pag_vlr_{id_sel}")
                    data_lim_pag = st.date_input("Data do Recebimento", value=date.today(),
                                                 key=f"lim_pag_data_{id_sel}")

                    if st.button("✅ Confirmar Recebimento", type="primary", key=f"lim_pag_btn_{id_sel}"):
                        exec_db(
                            "UPDATE parcelas_liminar SET pago=1, data_pagamento=%s WHERE contrato_id=%s AND nr_parcela=%s",
                            (data_lim_pag.strftime("%Y-%m-%d"), int(id_sel), n_lim_pag)
                        )
                        st.session_state['ultimo_recibo'] = (
                            f"⚖️ *RECIBO — LIMINAR / REDUÇÃO*\n"
                            f"---------------------------------------\n"
                            f"*Cliente:* {resumo['cliente']}\n"
                            f"*CPF/CNPJ:* {formatar_cpf_cnpj(resumo['cpf_cnpj'])}\n"
                            f"*Referente:* Parcela {n_lim_pag} da Redução da Liminar\n"
                            f"*Valor Recebido:* R$ {vlr_lim_rec:,.2f}\n"
                            f"*Data:* {data_lim_pag.strftime('%d/%m/%Y')}\n"
                            f"---------------------------------------\n"
                            f"Obrigado pela confiança!"
                        )
                        st.session_state['tel_cliente'] = str(resumo['telefone'])
                        st.rerun()
                else:
                    st.success("🎉 Todas as parcelas da redução já foram recebidas!")

        # ── TAB 3: ÊXITO ─────────────────────────────────────────────────
        with tab_exit:
            exito_pago_atual = int(resumo.get('exito_pago') or 0)
            hon_exito_pct    = float(resumo.get('hon_exito_percentual') or 0)
            hon_exito_fix    = float(resumo.get('hon_exito_fixo')       or 0)

            if hon_exito_pct == 0 and hon_exito_fix == 0:
                st.info("Nenhum honorário de êxito configurado para este contrato. "
                        "Configure em **📂 Meus Contratos → Editar Contrato**.")
            elif exito_pago_atual == 1:
                exito_data_rec = str(resumo.get('exito_data_pagamento') or '')
                exito_vlr_rec  = float(resumo.get('exito_valor_recebido') or 0)
                st.success(f"🏆 Honorários de êxito já recebidos em **{formatar_data(exito_data_rec)}**: "
                           f"**R$ {exito_vlr_rec:,.2f}**")
            else:
                if hon_exito_pct > 0:
                    st.info(f"Percentual de êxito acordado: **{hon_exito_pct:.2f}%** sobre o valor da causa.")
                if hon_exito_fix > 0:
                    st.info(f"Valor fixo de êxito acordado: **R$ {hon_exito_fix:,.2f}**")

                col_ex1, col_ex2 = st.columns(2)
                vlr_exito_rec  = col_ex1.number_input("Valor Recebido (R$)",
                                                       min_value=0.01, step=100.0, format="%.2f",
                                                       value=hon_exito_fix if hon_exito_fix > 0 else 100.0,
                                                       key=f"exit_vlr_{id_sel}")
                data_exito_rec = col_ex2.date_input("Data do Recebimento", value=date.today(),
                                                    key=f"exit_data_{id_sel}")

                if st.button("🏆 Confirmar Recebimento de Êxito", type="primary", key=f"exit_btn_{id_sel}"):
                    exec_db(
                        "UPDATE contratos SET exito_pago=1, exito_data_pagamento=%s, exito_valor_recebido=%s WHERE id=%s",
                        (data_exito_rec.strftime("%Y-%m-%d"), vlr_exito_rec, int(id_sel))
                    )
                    st.session_state['ultimo_recibo'] = (
                        f"⚖️ *RECIBO — HONORÁRIOS DE ÊXITO*\n"
                        f"---------------------------------------\n"
                        f"*Cliente:* {resumo['cliente']}\n"
                        f"*CPF/CNPJ:* {formatar_cpf_cnpj(resumo['cpf_cnpj'])}\n"
                        f"*Referente:* Honorários de Êxito\n"
                        f"*Valor Recebido:* R$ {vlr_exito_rec:,.2f}\n"
                        f"*Data:* {data_exito_rec.strftime('%d/%m/%Y')}\n"
                        f"---------------------------------------\n"
                        f"Obrigado pela confiança!"
                    )
                    st.session_state['tel_cliente'] = str(resumo['telefone'])
                    st.rerun()

        # ── TAB 4: COMBINADO ─────────────────────────────────────────────
        with tab_comb:
            st.markdown("Registre múltiplos recebimentos de uma vez (ex: inicial + liminar).")

            df_parc_c    = select_db(
                "SELECT * FROM parcelas WHERE contrato_id=%s AND pago=0 ORDER BY nr_parcela", (int(id_sel),))
            df_plim_c    = select_db(
                "SELECT * FROM parcelas_liminar WHERE contrato_id=%s AND pago=0 ORDER BY nr_parcela", (int(id_sel),))
            exito_pago_c    = int(resumo.get('exito_pago') or 0)
            hon_exito_fix_c = float(resumo.get('hon_exito_fixo') or 0)
            hon_exito_pct_c = float(resumo.get('hon_exito_percentual') or 0)
            tem_exito       = exito_pago_c == 0 and (hon_exito_pct_c > 0 or hon_exito_fix_c > 0)

            if df_parc_c.empty and df_plim_c.empty and not tem_exito:
                st.success("Não há pendências a registrar para este contrato.")
            else:
                comb_ini  = st.checkbox("💰 Honorários Iniciais",  key=f"comb_ini_{id_sel}",  disabled=df_parc_c.empty)
                comb_lim  = st.checkbox("⚖️ Liminar / Redução",    key=f"comb_lim_{id_sel}",  disabled=df_plim_c.empty)
                comb_exit = st.checkbox("🏆 Êxito",                 key=f"comb_exit_{id_sel}", disabled=not tem_exito)

                parc_comb_sel = None; vlr_ini_comb  = 0.0
                parc_lim_sel  = None; vlr_lim_comb  = 0.0
                vlr_exit_comb = 0.0

                if comb_ini and not df_parc_c.empty:
                    df_parc_c['valor_parcela'] = pd.to_numeric(df_parc_c['valor_parcela'], errors='coerce')
                    opcoes_ini_c = {f"Parc {r.nr_parcela} (Venc: {formatar_data(r.data_vencimento)})": r.nr_parcela
                                   for _, r in df_parc_c.iterrows()}
                    c1, c2 = st.columns(2)
                    parc_lbl_c    = c1.selectbox("Parcela inicial:", list(opcoes_ini_c.keys()),
                                                 key=f"comb_ini_sel_{id_sel}")
                    parc_comb_sel = opcoes_ini_c[parc_lbl_c]
                    vlr_ini_comb  = c2.number_input("Valor inicial (R$)",
                        value=float(df_parc_c[df_parc_c['nr_parcela']==parc_comb_sel]['valor_parcela'].values[0]),
                        format="%.2f", key=f"comb_ini_vlr_{id_sel}")

                if comb_lim and not df_plim_c.empty:
                    df_plim_c['valor_parcela'] = pd.to_numeric(df_plim_c['valor_parcela'], errors='coerce')
                    opcoes_lim_c = {f"Parcela {r.nr_parcela} — R$ {r.valor_parcela:,.2f}": r.nr_parcela
                                    for _, r in df_plim_c.iterrows()}
                    c3, c4 = st.columns(2)
                    lim_lbl_c    = c3.selectbox("Parcela liminar:", list(opcoes_lim_c.keys()),
                                                key=f"comb_lim_sel_{id_sel}")
                    parc_lim_sel = opcoes_lim_c[lim_lbl_c]
                    vlr_lim_comb = c4.number_input("Valor liminar (R$)",
                        value=float(df_plim_c[df_plim_c['nr_parcela']==parc_lim_sel]['valor_parcela'].values[0]),
                        format="%.2f", key=f"comb_lim_vlr_{id_sel}")

                if comb_exit and tem_exito:
                    vlr_exit_comb = st.number_input("Valor Êxito (R$)",
                        value=hon_exito_fix_c if hon_exito_fix_c > 0 else 100.0,
                        format="%.2f", key=f"comb_exit_vlr_{id_sel}")

                if comb_ini or comb_lim or comb_exit:
                    forma_comb = st.selectbox("Método de Recebimento",
                                              ["Pix","Dinheiro","Transferência","Cartão","Boleto"],
                                              key=f"comb_forma_{id_sel}")
                    data_comb  = st.date_input("Data do Recebimento", value=date.today(),
                                               key=f"comb_data_{id_sel}")
                    total_comb = vlr_ini_comb + vlr_lim_comb + vlr_exit_comb
                    st.metric("Total a Registrar", f"R$ {total_comb:,.2f}")

                    if st.button("✅ Confirmar Pagamento Combinado", type="primary", key=f"comb_btn_{id_sel}"):
                        linhas_recibo = [
                            f"⚖️ *RECIBO COMBINADO DE HONORÁRIOS*",
                            f"---------------------------------------",
                            f"*Cliente:* {resumo['cliente']}",
                            f"*CPF/CNPJ:* {formatar_cpf_cnpj(resumo['cpf_cnpj'])}",
                            f"*Data:* {data_comb.strftime('%d/%m/%Y')} | *Método:* {forma_comb}",
                            f"---------------------------------------",
                        ]
                        novo_saldo_c = sd
                        if comb_ini and parc_comb_sel is not None:
                            exec_db(
                                "UPDATE parcelas SET pago=1, data_pagamento=%s, forma_pagamento=%s WHERE contrato_id=%s AND nr_parcela=%s",
                                (data_comb.strftime("%Y-%m-%d"), forma_comb, int(id_sel), parc_comb_sel)
                            )
                            novo_saldo_c = round(novo_saldo_c - vlr_ini_comb, 2)
                            linhas_recibo.append(f"💰 Honorários Iniciais (Parc. {parc_comb_sel}): R$ {vlr_ini_comb:,.2f}")
                        if comb_lim and parc_lim_sel is not None:
                            exec_db(
                                "UPDATE parcelas_liminar SET pago=1, data_pagamento=%s WHERE contrato_id=%s AND nr_parcela=%s",
                                (data_comb.strftime("%Y-%m-%d"), int(id_sel), parc_lim_sel)
                            )
                            linhas_recibo.append(f"⚖️ Liminar / Redução (Parc. {parc_lim_sel}): R$ {vlr_lim_comb:,.2f}")
                        if comb_exit:
                            exec_db(
                                "UPDATE contratos SET exito_pago=1, exito_data_pagamento=%s, exito_valor_recebido=%s WHERE id=%s",
                                (data_comb.strftime("%Y-%m-%d"), vlr_exit_comb, int(id_sel))
                            )
                            linhas_recibo.append(f"🏆 Êxito: R$ {vlr_exit_comb:,.2f}")
                        if comb_ini:
                            exec_db("UPDATE contratos SET saldo_devedor=%s WHERE id=%s",
                                    (max(novo_saldo_c, 0), int(id_sel)))
                            if novo_saldo_c <= 0:
                                exec_db("UPDATE contratos SET observacoes='Pago' WHERE id=%s", (int(id_sel),))
                                st.balloons()
                        linhas_recibo += [
                            f"---------------------------------------",
                            f"*Total Recebido:* R$ {total_comb:,.2f}",
                        ]
                        if comb_ini:
                            linhas_recibo.append(f"*Saldo Devedor Restante:* R$ {max(novo_saldo_c, 0):,.2f}")
                        linhas_recibo.append("---------------------------------------\nObrigado pela confiança!")
                        st.session_state['ultimo_recibo'] = "\n".join(linhas_recibo)
                        st.session_state['tel_cliente']   = str(resumo['telefone'])
                        st.rerun()
 
# --- MEUS CONTRATOS ---
elif aba == "📂 Meus Contratos":
    st.header("Meus Contratos")
    st.markdown("Acesse, edite e acompanhe os detalhes de cada contrato — incluindo parcelas da redução da liminar.")

    df_todos = select_db("SELECT * FROM contratos ORDER BY cliente ASC")
    if df_todos.empty:
        st.info("Nenhum contrato cadastrado ainda.")
    else:
        df_todos['valor_total']   = pd.to_numeric(df_todos['valor_total'],   errors='coerce').fillna(0)
        df_todos['saldo_devedor'] = pd.to_numeric(df_todos['saldo_devedor'], errors='coerce').fillna(0)

        contrato_map = {f"{r['cliente']} (Contrato #{r['id']})": r['id']
                        for _, r in df_todos.iterrows()}

        nome_sel_mc = st.selectbox("Selecione o Contrato", options=list(contrato_map.keys()),
                                   key="meus_contratos_sel")
        id_mc       = contrato_map[nome_sel_mc]
        c           = df_todos[df_todos['id'] == id_mc].iloc[0]

        # ── Cabeçalho do cliente ──────────────────────────────────────────
        st.markdown(f"""
            <div class='info-cliente'>
                <b>👤 {c['cliente']}</b> &nbsp;|&nbsp;
                💳 {formatar_cpf_cnpj(c['cpf_cnpj'])} &nbsp;|&nbsp;
                📞 {formatar_telefone(c['telefone'])} &nbsp;|&nbsp;
                📅 Contrato: {formatar_data(c['data_contrato'])}
            </div>
        """, unsafe_allow_html=True)

        infos_proc_mc = []
        if not nulo(c.get('nr_processo', '')): infos_proc_mc.append(f"📄 {c['nr_processo']}")
        if not nulo(c.get('nr_vara',     '')): infos_proc_mc.append(f"🏛️ {c['nr_vara']}")
        if not nulo(c.get('nome_juiz',   '')): infos_proc_mc.append(f"👨‍⚖️ {c['nome_juiz']}")
        if not nulo(c.get('comarca',     '')): infos_proc_mc.append(f"📍 {c['comarca']}")
        if infos_proc_mc:
            st.caption(" &nbsp;|&nbsp; ".join(infos_proc_mc))

        vt_mc = float(c['valor_total'])
        sd_mc = float(c['saldo_devedor'])
        tutela_atual = str(c.get('tutela', '') or 'Pendente')

        # Parcelas da liminar: pagas x total
        df_plim_resumo = select_db(
            "SELECT pago FROM parcelas_liminar WHERE contrato_id = %s", (int(id_mc),)
        )
        if df_plim_resumo.empty:
            lim_label_metric  = "—"
            lim_delta_metric  = None
        else:
            df_plim_resumo['pago'] = pd.to_numeric(df_plim_resumo['pago'], errors='coerce').fillna(0).astype(int)
            lim_pagas  = int(df_plim_resumo['pago'].sum())
            lim_total  = len(df_plim_resumo)
            lim_label_metric = f"{lim_pagas} / {lim_total}"
            lim_delta_metric = f"{lim_total - lim_pagas} pendente(s)" if lim_pagas < lim_total else "✅ Todas recebidas"

        m1_mc, m2_mc, m3_mc, m4_mc = st.columns(4)
        m1_mc.metric("Honorários Iniciais",      f"R$ {vt_mc:,.2f}")
        m2_mc.metric("Saldo Devedor",             f"R$ {sd_mc:,.2f}")
        m3_mc.metric("Status da Tutela",          tutela_atual)
        m4_mc.metric("Parcelas da Redução",       lim_label_metric, delta=lim_delta_metric,
                     delta_color="inverse" if (lim_delta_metric and "pendente" in str(lim_delta_metric)) else "normal")

        st.divider()

        # ═══════════════════════════════════════════════════════════════════
        # EXPANDER 1 — EDITAR CONTRATO
        # ═══════════════════════════════════════════════════════════════════
        with st.expander("✏️ Editar Contrato", expanded=False):
            st.subheader("Dados Gerais")
            col_e1, col_e2 = st.columns(2)
            ed_nome   = col_e1.text_input("Nome do Cliente",  value=str(c['cliente']),          key=f"ed_nome_{id_mc}")
            ed_cpf    = col_e2.text_input("CPF / CNPJ",       value=str(c['cpf_cnpj'] or ''),   key=f"ed_cpf_{id_mc}")
            ed_tel    = col_e1.text_input("Telefone",          value=str(c['telefone'] or ''),   key=f"ed_tel_{id_mc}")
            ed_obs    = col_e2.text_area("Observações",        value=str(c['observacoes'] or ''), key=f"ed_obs_{id_mc}")

            st.subheader("💰 Honorários Iniciais")
            col_hi1, col_hi2 = st.columns(2)
            ed_hi_ativo = col_hi1.selectbox("Há cobrança inicial?", ["Não", "Sim"],
                                             index=0 if str(c.get('hon_inicial_ativo') or 'Não') == 'Não' else 1,
                                             key=f"ed_hi_ativo_{id_mc}")
            ed_hi_valor = col_hi2.number_input("Valor Total dos Honorários Iniciais (R$)",
                                                min_value=0.0, step=100.0, format="%.2f",
                                                value=float(c.get('hon_inicial_valor') or 0),
                                                key=f"ed_hi_valor_{id_mc}")
            col_hi3, col_hi4 = st.columns(2)
            ed_hi_parc = col_hi3.selectbox("Pagamento parcelado?", ["Não", "Sim"],
                                            index=0 if str(c.get('hon_inicial_parcelado') or 'Não') == 'Não' else 1,
                                            key=f"ed_hi_parc_{id_mc}")
            ed_hi_qtd  = col_hi4.number_input("Nº de Parcelas",
                                               min_value=1, max_value=60, step=1,
                                               value=int(c.get('hon_inicial_parcelas') or 1),
                                               key=f"ed_hi_qtd_{id_mc}")
            col_hi5, col_hi6 = st.columns(2)
            ed_valor_total = col_hi5.number_input("Valor Total do Contrato (R$)",
                                                   min_value=0.0, step=100.0, format="%.2f",
                                                   value=float(c['valor_total']),
                                                   key=f"ed_vt_{id_mc}")
            ed_saldo_dev   = col_hi6.number_input("Saldo Devedor Atual (R$)",
                                                   min_value=0.0, step=100.0, format="%.2f",
                                                   value=float(c['saldo_devedor']),
                                                   key=f"ed_sd_{id_mc}")

            st.subheader("⚖️ Honorários da Liminar")
            col_e3, col_e4 = st.columns(2)
            tutela_opts  = ["Pendente", "Deferido", "Indeferido", "Parcial"]
            tutela_idx   = tutela_opts.index(tutela_atual) if tutela_atual in tutela_opts else 0
            ed_tutela    = col_e3.selectbox("Status da Tutela", tutela_opts,
                                             index=tutela_idx, key=f"ed_tutela_{id_mc}")
            ed_lim_fixo  = col_e4.number_input("Honorários Fixos da Liminar (R$)",
                                                min_value=0.0, step=100.0, format="%.2f",
                                                value=float(c.get('hon_liminar_fixo') or 0),
                                                key=f"ed_lim_fixo_{id_mc}")
            col_e5, col_e6 = st.columns(2)
            ed_red_vlr   = col_e5.number_input("Valor da Redução Obtida (R$)",
                                                min_value=0.0, step=100.0, format="%.2f",
                                                value=float(c.get('hon_liminar_reducao_vlr') or 0),
                                                key=f"ed_red_vlr_{id_mc}")
            ed_red_prc   = col_e6.number_input("Nº de Parcelas da Redução",
                                                min_value=0, max_value=360, step=1,
                                                value=int(c.get('hon_liminar_reducao_prc') or 0),
                                                key=f"ed_red_prc_{id_mc}")

            st.subheader("🏆 Honorários de Êxito")
            col_e7, col_e8 = st.columns(2)
            ed_exito_pct  = col_e7.number_input("Percentual de Êxito (%)",
                                                  min_value=0.0, max_value=100.0, step=0.5, format="%.2f",
                                                  value=float(c.get('hon_exito_percentual') or 0),
                                                  key=f"ed_exito_pct_{id_mc}")
            ed_exito_fixo = col_e8.number_input("Valor Fixo de Êxito (R$)",
                                                  min_value=0.0, step=100.0, format="%.2f",
                                                  value=float(c.get('hon_exito_fixo') or 0),
                                                  key=f"ed_exito_fixo_{id_mc}")

            st.subheader("📁 Dados do Processo")
            col_e9, col_e10 = st.columns(2)
            ed_proc  = col_e9.text_input("Número do Processo",  value=str(c.get('nr_processo') or ''), key=f"ed_proc_{id_mc}")
            ed_vara  = col_e10.text_input("Número da Vara",     value=str(c.get('nr_vara')    or ''), key=f"ed_vara_{id_mc}")
            col_e11, col_e12 = st.columns(2)
            ed_juiz  = col_e11.text_input("Nome do Juiz",        value=str(c.get('nome_juiz')  or ''), key=f"ed_juiz_{id_mc}")
            ed_com   = col_e12.text_input("Comarca",             value=str(c.get('comarca')    or ''), key=f"ed_com_{id_mc}")

            if st.button("💾 Salvar Alterações", type="primary", key=f"btn_salvar_edicao_{id_mc}"):
                vlr_parc_novo = round(ed_hi_valor / ed_hi_qtd, 2) if ed_hi_qtd > 0 and ed_hi_valor > 0 else 0.0
                exec_db("""
                    UPDATE contratos SET
                        cliente                 = %s,
                        cpf_cnpj                = %s,
                        telefone                = %s,
                        observacoes             = %s,
                        valor_total             = %s,
                        saldo_devedor           = %s,
                        hon_inicial_ativo       = %s,
                        hon_inicial_valor       = %s,
                        hon_inicial_parcelado   = %s,
                        hon_inicial_parcelas    = %s,
                        hon_inicial_vlr_parcela = %s,
                        tutela                  = %s,
                        hon_liminar_fixo        = %s,
                        hon_liminar_reducao_vlr = %s,
                        hon_liminar_reducao_prc = %s,
                        hon_exito_percentual    = %s,
                        hon_exito_fixo          = %s,
                        nr_processo             = %s,
                        nr_vara                 = %s,
                        nome_juiz               = %s,
                        comarca                 = %s
                    WHERE id = %s
                """, (
                    ed_nome.strip(),
                    formatar_cpf_cnpj(re.sub(r'\D', '', ed_cpf)),
                    formatar_telefone(re.sub(r'\D', '', ed_tel)),
                    ed_obs.strip() or None,
                    ed_valor_total,
                    ed_saldo_dev,
                    ed_hi_ativo,
                    ed_hi_valor   or None,
                    ed_hi_parc,
                    ed_hi_qtd     or None,
                    vlr_parc_novo or None,
                    ed_tutela,
                    ed_lim_fixo   or None,
                    ed_red_vlr    or None,
                    ed_red_prc    or None,
                    ed_exito_pct  or None,
                    ed_exito_fixo or None,
                    ed_proc.strip()  or None,
                    ed_vara.strip()  or None,
                    ed_juiz.strip()  or None,
                    ed_com.strip()   or None,
                    int(id_mc)
                ))
                st.success("Contrato atualizado com sucesso!")
                time.sleep(0.8)
                st.rerun()

        # ═══════════════════════════════════════════════════════════════════
        # EXPANDER 2 — PARCELAS DA REDUÇÃO DA LIMINAR
        # ═══════════════════════════════════════════════════════════════════
        with st.expander("📋 Parcelas da Redução da Liminar", expanded=True):

            df_plim = select_db(
                "SELECT * FROM parcelas_liminar WHERE contrato_id = %s ORDER BY nr_parcela",
                (int(id_mc),)
            )

            # ── Ainda sem parcelas cadastradas: formulário de criação ──────
            if df_plim.empty:
                st.info("Nenhuma parcela da redução cadastrada para este contrato.")

                if tutela_atual in ("Deferido", "Parcial"):
                    st.markdown("**Cadastrar parcelas da redução obtida:**")
                    col_pl1, col_pl2, col_pl3 = st.columns(3)
                    pl_vlr_total = col_pl1.number_input(
                        "Valor Total da Redução (R$)", min_value=0.01, step=100.0,
                        format="%.2f", key="pl_vlr_total",
                        value=float(c.get('hon_liminar_reducao_vlr') or 0) or 100.0
                    )
                    pl_qtd = col_pl2.number_input(
                        "Número de Parcelas", min_value=1, max_value=360, step=1,
                        value=int(c.get('hon_liminar_reducao_prc') or 1),
                        key="pl_qtd"
                    )
                    pl_inicio = col_pl3.date_input("Data da 1ª Parcela", value=date.today(), key="pl_inicio")

                    vlr_parc_prev = round(pl_vlr_total / pl_qtd, 2) if pl_qtd > 0 else 0.0
                    st.metric("Valor de Cada Parcela", f"R$ {vlr_parc_prev:,.2f}")

                    if st.button("📥 Criar Parcelas da Redução", type="primary", key="btn_criar_plim"):
                        v_base_lim = round(pl_vlr_total / pl_qtd, 2)
                        for i in range(1, pl_qtd + 1):
                            v_f_lim  = round(pl_vlr_total - v_base_lim * (pl_qtd - 1), 2) if i == pl_qtd else v_base_lim
                            venc_lim = (datetime.combine(pl_inicio, datetime.min.time()) +
                                        pd.DateOffset(months=i - 1))
                            exec_db(
                                """INSERT INTO parcelas_liminar
                                   (contrato_id, nr_parcela, valor_parcela, data_prevista)
                                   VALUES (%s, %s, %s, %s)""",
                                (int(id_mc), i, v_f_lim, venc_lim.strftime("%Y-%m-%d"))
                            )
                        st.success(f"{pl_qtd} parcela(s) criada(s) com sucesso!")
                        time.sleep(0.8)
                        st.rerun()
                else:
                    st.warning("A tutela ainda está como **Pendente** ou **Indeferida**. "
                               "Edite o status da tutela acima para cadastrar parcelas da redução.")

            # ── Parcelas já existentes: acompanhamento ─────────────────────
            else:
                df_plim['pago']          = pd.to_numeric(df_plim['pago'], errors='coerce').fillna(0).astype(int)
                df_plim['valor_parcela'] = pd.to_numeric(df_plim['valor_parcela'], errors='coerce')

                total_lim     = df_plim['valor_parcela'].sum()
                pagas_lim     = df_plim[df_plim['pago'] == 1]['valor_parcela'].sum()
                pendentes_lim = df_plim[df_plim['pago'] == 0]['valor_parcela'].sum()

                ml1, ml2, ml3 = st.columns(3)
                ml1.metric("Total da Redução",     f"R$ {total_lim:,.2f}")
                ml2.metric("Já Recebido",           f"R$ {pagas_lim:,.2f}")
                ml3.metric("A Receber",             f"R$ {pendentes_lim:,.2f}")

                pct_lim = float(pagas_lim / total_lim) if total_lim > 0 else 0.0
                st.progress(pct_lim, text=f"Progresso: {pct_lim:.1%} recebido")

                # Tabela de acompanhamento
                df_lim_view                  = df_plim.copy()
                df_lim_view['Status']        = df_lim_view.apply(
                    lambda r: "🟢 Recebido" if r['pago'] == 1 else obter_status_parcela(r['pago'], r['data_prevista']),
                    axis=1
                )
                df_lim_view['Previsão']      = df_lim_view['data_prevista'].apply(formatar_data)
                df_lim_view['Recebido em']   = df_lim_view['data_pagamento'].apply(formatar_data)

                st.dataframe(
                    df_lim_view,
                    use_container_width=True,
                    hide_index=True,
                    column_order=['nr_parcela', 'valor_parcela', 'Previsão', 'Recebido em', 'Status'],
                    column_config={
                        "nr_parcela":    "Parcela",
                        "valor_parcela": st.column_config.NumberColumn("Valor", format="R$ %.2f"),
                    }
                )

                # Marcar parcela como recebida
                pendentes_lim_df = df_plim[df_plim['pago'] == 0]
                if not pendentes_lim_df.empty:
                    st.markdown("**Registrar Recebimento:**")
                    col_pl_a, col_pl_b = st.columns(2)

                    opcoes_lim = {
                        f"Parcela {r.nr_parcela} — Prev. {formatar_data(r.data_prevista)} — R$ {r.valor_parcela:,.2f}": r.nr_parcela
                        for _, r in pendentes_lim_df.iterrows()
                    }
                    lim_label = col_pl_a.selectbox(
                        "Qual parcela recebeu?", options=list(opcoes_lim.keys()), key="lim_sel_parc"
                    )
                    n_lim = opcoes_lim[lim_label]
                    vlr_lim_row = float(
                        pendentes_lim_df[pendentes_lim_df['nr_parcela'] == n_lim]['valor_parcela'].values[0]
                    )
                    vlr_lim_recebido = col_pl_b.number_input(
                        "Valor Recebido (R$)", value=vlr_lim_row, format="%.2f", key="lim_vlr_rec"
                    )
                    data_lim_receb = st.date_input("Data do Recebimento", value=date.today(), key="lim_data_rec")

                    if st.button("✅ Confirmar Recebimento da Parcela", type="primary", key="btn_conf_lim"):
                        exec_db(
                            """UPDATE parcelas_liminar
                               SET pago = 1, data_pagamento = %s
                               WHERE contrato_id = %s AND nr_parcela = %s""",
                            (data_lim_receb.strftime("%Y-%m-%d"), int(id_mc), n_lim)
                        )
                        st.success(f"Parcela {n_lim} marcada como recebida!")
                        time.sleep(0.8)
                        st.rerun()
                else:
                    st.success("🎉 Todas as parcelas da redução já foram recebidas!")

                st.divider()
                if st.button("🗑️ Apagar todas as parcelas da redução deste contrato",
                             key="btn_del_plim"):
                    exec_db("DELETE FROM parcelas_liminar WHERE contrato_id = %s", (int(id_mc),))
                    st.warning("Parcelas da redução removidas.")
                    time.sleep(0.8)
                    st.rerun()

# --- ARQUIVADOS ---
elif aba == "📁 Arquivados":
    st.header("Contratos Quitados")
    st.markdown("Histórico de clientes que já **zeraram** seus saldos devedores.")
 
    df_arq = select_db("""
        SELECT c.cliente, c.cpf_cnpj, c.telefone, c.valor_total, c.data_contrato,
               MAX(p.data_pagamento) AS data_quitacao, c.observacoes
        FROM contratos c
        LEFT JOIN parcelas p ON c.id = p.contrato_id
        WHERE c.saldo_devedor <= 0
        GROUP BY c.id
        ORDER BY c.cliente ASC
    """)
    if not df_arq.empty:
        df_arq['data_contrato'] = df_arq['data_contrato'].apply(formatar_data)
        df_arq['data_quitacao'] = df_arq['data_quitacao'].apply(formatar_data)
        df_arq['cpf_cnpj']      = df_arq['cpf_cnpj'].apply(formatar_cpf_cnpj)
        df_arq['telefone']      = df_arq['telefone'].apply(formatar_telefone)
        df_arq['observacoes']   = df_arq['observacoes'].apply(lambda x: "-" if nulo(x) else str(x))
        df_arq['valor_total']   = pd.to_numeric(df_arq['valor_total'], errors='coerce')
        df_arq.columns = ['Cliente','CPF/CNPJ','Telefone','Valor Total',
                          'Data Início','Data Quitação','Observações']
        st.dataframe(df_arq, use_container_width=True, hide_index=True,
                     column_config={"Valor Total": st.column_config.NumberColumn("Valor Contrato", format="R$ %.2f")})
        st.divider()
        col_exp1, col_exp2 = st.columns(2)
        with col_exp1:
            st.download_button("📥 Exportar Histórico para Excel",
                data=gerar_excel(df_arq),
                file_name=f"contratos_quitados_{date.today()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        with col_exp2:
            st.download_button("📄 Exportar Histórico para PDF",
                data=gerar_pdf(df_arq, "Relatório de Contratos Quitados"),
                file_name=f"contratos_quitados_{date.today()}.pdf",
                mime="application/pdf")
    else:
        st.info("Nenhum contrato arquivado até o momento.")
 
# --- GESTÃO ---
elif aba == "⚙️ Gestão":
    st.header("Gerenciar")
    df_all = select_db("SELECT id, cliente FROM contratos ORDER BY cliente ASC")
    if not df_all.empty:
        opcoes  = {f"{r['cliente']} (ID {r['id']})": r['id'] for _, r in df_all.iterrows()}
        del_sel = st.selectbox("Excluir contrato:", options=list(opcoes.keys()))
        if st.button("❌ APAGAR DEFINITIVAMENTE"):
            exec_db("DELETE FROM contratos WHERE id = %s", (int(opcoes[del_sel]),))
            st.rerun()
    else:
        st.info("Nenhum contrato cadastrado.")
