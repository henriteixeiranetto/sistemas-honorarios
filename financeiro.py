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
st.set_page_config(page_title="Gestão de Honorários PRO", layout="wide", page_icon="⚖️")

st.markdown("""
    <style>
        .stMetric { background-color: #f8f9fa; padding: 15px; border-radius: 10px; border: 1px solid #dee2e6; }
        button[kind="primary"] { width: 100%; height: 3em; font-weight: bold; }
        .observacao-box { background-color: #fff3cd; padding: 10px; border-radius: 5px; border-left: 5px solid #ffca2c; margin-bottom: 20px; }
        .info-cliente { background-color: #f0f2f6; padding: 15px; border-radius: 8px; margin-bottom: 10px; border-left: 5px solid #007bff; }
    </style>
""", unsafe_allow_html=True)

# =============================================================================
# 2. CONEXÃO COM SQL (PostgreSQL)
# =============================================================================
# Configure em Streamlit Cloud → Settings → Secrets:
#
#   [credenciais]
#   usuario = "seu_usuario"
#   senha   = "sua_senha"

def criar_conexao():
    return psycopg2.connect(
        host            = st.secrets["supabase"]["host"],
        port            = st.secrets["supabase"]["port"],
        dbname          = st.secrets["supabase"]["dbname"],
        user            = st.secrets["supabase"]["user"],
        password        = st.secrets["supabase"]["password"],
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
    """INSERT / UPDATE / DELETE / DDL."""
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            conn.commit()
    except Exception as e:
        conn.rollback()
        st.error(f"Erro no banco: {e}")

def exec_retorna(query, params=()):
    """INSERT com RETURNING — retorna o valor gerado."""
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
    """SELECT — retorna DataFrame."""
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
# Usamos if/session_state para evitar que a chamada fique exposta como
# expressão no nível do módulo (o que causaria o "None" do magic mode).
# =============================================================================
def inicializar_banco():
    exec_db("""
        CREATE TABLE IF NOT EXISTS contratos (
            id            SERIAL PRIMARY KEY,
            cliente       TEXT NOT NULL,
            cpf_cnpj      TEXT,
            telefone      TEXT,
            valor_total   REAL NOT NULL,
            saldo_devedor REAL NOT NULL,
            data_contrato TEXT NOT NULL,
            tutela        TEXT,
            observacoes   TEXT
        )
    """)
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

# if é um STATEMENT — o magic mode não age sobre ele, então não exibe None
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
            if u == st.secrets["credenciais"]["usuario"] and s == st.secrets["credenciais"]["senha"]:
                st.session_state['autenticado'] = True
                st.rerun()
            else:
                st.error("Credenciais inválidas")
    st.stop()

opcoes_menu = ["📊 Dashboard", "➕ Novo Contrato", "💰 Pagamentos", "📁 Arquivados", "⚙️ Gestão"]
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
    col1, col2 = st.columns(2)
    nome    = col1.text_input("Nome do Cliente")
    cpf_raw = col2.text_input("CPF ou CNPJ (Somente números)", placeholder="Ex: 00000000000")
    tel_raw = col1.text_input("Telefone (Somente números)", placeholder="Ex: 11999998888")
    data_c  = col2.date_input("Data do Contrato", value=date.today())
    valor   = col1.number_input("Valor Total (R$)", min_value=0.0, step=100.0, format="%.2f")
    tutela  = col2.selectbox("Status da Tutela", ["Pendente","Deferida","Indeferida","Parcial"])
    obs     = st.text_area("Observações (Nº do Processo, Vara, etc.)")

    if valor > 0:
        opcoes  = ([f"À vista: R$ {valor:,.2f}"] +
                   [f"R$ {valor:,.2f} ou {i}x de R$ {valor/i:,.2f} sem juros" for i in range(2, 11)])
        selecao = st.selectbox("Parcelamento:", opcoes)
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
                cpf_fmt = formatar_cpf_cnpj(doc_limpo)
                tel_fmt = formatar_telefone(tel_raw)
                obs_val = obs.strip() or None
                c_id = exec_retorna(
                    """INSERT INTO contratos
                       (cliente, cpf_cnpj, telefone, valor_total, saldo_devedor,
                        data_contrato, tutela, observacoes)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                    (nome, cpf_fmt, tel_fmt, valor, valor,
                     data_c.strftime("%Y-%m-%d"), tutela, obs_val)
                )
                v_base = round(valor / n_p, 2)
                for i in range(1, n_p + 1):
                    v_f  = round(valor - (v_base * (n_p - 1)), 2) if i == n_p else v_base
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

        df_parc = select_db(
            "SELECT * FROM parcelas WHERE contrato_id = %s ORDER BY nr_parcela", (int(id_sel),))
        if not df_parc.empty:
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
                parc_label   = col_p.selectbox("Qual parcela pagar?", options=list(opcoes_parc.keys()))
                n_p_f        = opcoes_parc[parc_label]
                valor_pago   = col_v.number_input("Valor Recebido (R$)",
                    value=float(pendentes[pendentes['nr_parcela'] == n_p_f]['valor_parcela'].values[0]),
                    format="%.2f")
                forma_p = st.selectbox("Método de Recebimento",
                                       ["Pix","Dinheiro","Transferência","Cartão","Boleto"])

                if st.button("Confirmar Pagamento", type="primary"):
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
                        f"*Referente:* Parcela {n_p_f}\n"
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


