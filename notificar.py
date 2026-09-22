# -*- coding: utf-8 -*-
"""
Aviso diário de vencimentos — roda uma vez por dia e notifica o escritório
sobre as parcelas que vencem hoje.

Pensado para rodar como Cron Job do Railway, do mesmo jeito que o
`keepalive.py`. Não depende do site estar aberto.

    python notificar.py

O canal é escolhido por variável de ambiente. Pode usar mais de um ao mesmo
tempo — cada um é tentado de forma independente.

CANAL: E-MAIL (funciona hoje, sem burocracia)
    NOTIFICAR_EMAIL_PARA      destinatário(s), separados por vírgula
    SMTP_HOST                 ex: smtp.gmail.com
    SMTP_PORTA                ex: 587
    SMTP_USUARIO              a conta que envia
    SMTP_SENHA                senha de app (não a senha normal da conta)

CANAL: WHATSAPP (exige API oficial da Meta — veja README-notificacoes.md)
    WHATSAPP_TOKEN            token de acesso permanente
    WHATSAPP_PHONE_ID         id do número remetente, dado pela Meta
    WHATSAPP_PARA             número que recebe, só dígitos com DDI
    WHATSAPP_TEMPLATE         nome do modelo aprovado (obrigatório fora da
                              janela de 24h)
    WHATSAPP_TEMPLATE_IDIOMA  padrão pt_BR

CANAL: TELEGRAM (grátis e imediato, se o escritório topar usar)
    TELEGRAM_TOKEN            token do bot, dado pelo @BotFather
    TELEGRAM_CHAT_ID          id do destino

OUTRAS:
    NOTIFICAR_INCLUIR_ATRASADAS=1   também lembra das que já venceram
    NOTIFICAR_TESTE=1               monta e mostra a mensagem, sem enviar
                                    e sem marcar como enviada

O script não repete aviso: cada parcela avisada fica registrada na tabela
`notificacoes_enviadas`, então rodar duas vezes no mesmo dia não incomoda
ninguém duas vezes. O registro é por DIA de aviso: uma parcela atrasada
volta a aparecer no dia seguinte, que é o útil para cobrança.
"""

from __future__ import annotations

import json
import os
import smtplib
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from email.message import EmailMessage

import psycopg2
from psycopg2.extras import RealDictCursor

try:
    from zoneinfo import ZoneInfo

    FUSO = ZoneInfo("America/Sao_Paulo")
except Exception:  # pragma: no cover
    FUSO = timezone(timedelta(hours=-3))

TEMPO_LIMITE = 25
TESTE = os.environ.get("NOTIFICAR_TESTE") == "1"


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------
def _log(mensagem: str) -> None:
    agora = datetime.now(FUSO).strftime("%d/%m/%Y %H:%M:%S")
    print(f"[notificar] {agora} — {mensagem}", flush=True)


def hoje() -> date:
    return datetime.now(FUSO).date()


def moeda(valor) -> str:
    """R$ 4.000,00 — o format do Python só produz separador americano."""
    try:
        bruto = f"{float(valor):,.2f}"
    except (TypeError, ValueError):
        return "R$ 0,00"
    return "R$ " + bruto.replace(",", "\x00").replace(".", ",").replace("\x00", ".")


def data_br(iso: str) -> str:
    try:
        return datetime.strptime(str(iso)[:10], "%Y-%m-%d").strftime("%d/%m/%Y")
    except ValueError:
        return str(iso)


def _obrigatorio(nome: str, padrao: str | None = None) -> str:
    valor = os.environ.get(nome) or padrao
    if not valor:
        _log(f"ERRO: variável de ambiente {nome} não definida.")
        sys.exit(1)
    return valor


def parametros_banco() -> dict:
    return {
        "host": _obrigatorio("SUPABASE_HOST"),
        "port": int(_obrigatorio("SUPABASE_PORT", "5432")),
        "dbname": _obrigatorio("SUPABASE_DBNAME", "postgres"),
        "user": _obrigatorio("SUPABASE_USER"),
        "password": _obrigatorio("SUPABASE_PASSWORD"),
        "sslmode": "require",
        "connect_timeout": TEMPO_LIMITE,
        "application_name": "honorarios-notificar",
    }


# ---------------------------------------------------------------------------
# Banco
# ---------------------------------------------------------------------------
DDL_REGISTRO = """
CREATE TABLE IF NOT EXISTS notificacoes_enviadas (
    id              SERIAL PRIMARY KEY,
    tipo            TEXT NOT NULL,
    contrato_id     INTEGER NOT NULL,
    nr_parcela      INTEGER NOT NULL,
    data_referencia TEXT NOT NULL,
    canal           TEXT,
    enviada_em      TIMESTAMP DEFAULT NOW()
)
"""

# Sem este índice, rodar o cron duas vezes no mesmo dia avisaria duas vezes.
INDICE_REGISTRO = """
CREATE UNIQUE INDEX IF NOT EXISTS uq_notificacoes_parcela
    ON notificacoes_enviadas (tipo, contrato_id, nr_parcela, data_referencia)
"""

# Os ::text existem porque as colunas de data têm tipos diferentes entre as
# duas tabelas (date em `parcelas`, text em `parcelas_liminar`). Sem o cast, o
# UNION não casa os tipos.
SQL_VENCENDO = """
SELECT 'Honorários Iniciais' AS tipo, c.id AS contrato_id, c.cliente,
       c.telefone, p.nr_parcela,
       p.valor_parcela::numeric AS valor,
       p.data_vencimento::text  AS vencimento
FROM parcelas p
JOIN contratos c ON c.id = p.contrato_id
WHERE p.pago = 0 AND p.data_vencimento::text {comparacao}
  AND c.arquivado_em IS NULL
UNION ALL
SELECT 'Redução da Liminar', c.id, c.cliente,
       c.telefone, pl.nr_parcela,
       pl.valor_parcela::numeric,
       pl.data_prevista::text
FROM parcelas_liminar pl
JOIN contratos c ON c.id = pl.contrato_id
WHERE pl.pago = 0 AND pl.data_prevista::text {comparacao}
  AND c.arquivado_em IS NULL
ORDER BY vencimento, cliente, nr_parcela
"""


def buscar_vencimentos(conexao, referencia: str, incluir_atrasadas: bool) -> list[dict]:
    comparacao = "<= %s" if incluir_atrasadas else "= %s"
    sql = SQL_VENCENDO.format(comparacao=comparacao)
    with conexao.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (referencia, referencia))
        return [dict(linha) for linha in cur.fetchall()]


def filtrar_nao_avisadas(conexao, itens: list[dict], dia: str) -> list[dict]:
    """Descarta o que já foi avisado HOJE.

    A chave inclui o dia do aviso, não a data de vencimento. Assim rodar o
    cron duas vezes no mesmo dia não incomoda ninguém duas vezes, mas uma
    parcela atrasada volta a aparecer no dia seguinte — que é o
    comportamento útil para cobrança.
    """
    if not itens:
        return []
    chaves = [(i["tipo"], i["contrato_id"], i["nr_parcela"], dia) for i in itens]
    with conexao.cursor() as cur:
        cur.execute(
            """SELECT tipo, contrato_id, nr_parcela, data_referencia
               FROM notificacoes_enviadas
               WHERE (tipo, contrato_id, nr_parcela, data_referencia)
                     IN %s""",
            (tuple(chaves),),
        )
        ja_avisados = {tuple(linha) for linha in cur.fetchall()}
    return [i for i, chave in zip(itens, chaves) if chave not in ja_avisados]


def registrar_envio(conexao, itens: list[dict], dia: str, canal: str) -> None:
    with conexao.cursor() as cur:
        for item in itens:
            cur.execute(
                """INSERT INTO notificacoes_enviadas
                       (tipo, contrato_id, nr_parcela, data_referencia, canal)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT DO NOTHING""",
                (item["tipo"], item["contrato_id"], item["nr_parcela"], dia, canal),
            )


# ---------------------------------------------------------------------------
# Mensagem
# ---------------------------------------------------------------------------
def montar_mensagem(itens: list[dict], referencia: date) -> tuple[str, str]:
    """Devolve (assunto, corpo) — o corpo serve para todos os canais."""
    hoje_iso = referencia.isoformat()
    vencendo = [i for i in itens if i["vencimento"] == hoje_iso]
    atrasadas = [i for i in itens if i["vencimento"] < hoje_iso]

    total = sum(float(i["valor"] or 0) for i in itens)
    assunto = f"Vencimentos de hoje ({referencia.strftime('%d/%m/%Y')}) — {len(itens)} parcela(s)"

    linhas: list[str] = [f"*Honorários — vencimentos de {referencia.strftime('%d/%m/%Y')}*", ""]

    if vencendo:
        linhas.append(f"*Vence hoje ({len(vencendo)}):*")
        for item in vencendo:
            linhas.append(
                f"• {item['cliente']} — {item['tipo']}, parcela {item['nr_parcela']}: "
                f"{moeda(item['valor'])}"
            )
        linhas.append("")

    if atrasadas:
        linhas.append(f"*Em atraso ({len(atrasadas)}):*")
        for item in atrasadas:
            dias = (referencia - datetime.strptime(item["vencimento"][:10], "%Y-%m-%d").date()).days
            linhas.append(
                f"• {item['cliente']} — {item['tipo']}, parcela {item['nr_parcela']}: "
                f"{moeda(item['valor'])} (venceu em {data_br(item['vencimento'])}, {dias} dia(s))"
            )
        linhas.append("")

    linhas.append(f"*Total: {moeda(total)}*")
    return assunto, "\n".join(linhas)


# ---------------------------------------------------------------------------
# Canais
# ---------------------------------------------------------------------------
def enviar_email(assunto: str, corpo: str) -> bool | None:
    destino = os.environ.get("NOTIFICAR_EMAIL_PARA", "").strip()
    if not destino:
        return None  # canal não configurado

    servidor = os.environ.get("SMTP_HOST", "smtp.gmail.com").strip()
    porta = int(os.environ.get("SMTP_PORTA", "587"))
    usuario = os.environ.get("SMTP_USUARIO", "").strip()
    senha = os.environ.get("SMTP_SENHA", "").strip()
    if not usuario or not senha:
        _log("E-mail: SMTP_USUARIO/SMTP_SENHA não definidos.")
        return False

    mensagem = EmailMessage()
    mensagem["Subject"] = assunto
    mensagem["From"] = usuario
    mensagem["To"] = destino
    # O asterisco é marcação do WhatsApp; no e-mail vira ruído.
    mensagem.set_content(corpo.replace("*", ""))

    try:
        with smtplib.SMTP(servidor, porta, timeout=TEMPO_LIMITE) as smtp:
            smtp.starttls()
            smtp.login(usuario, senha)
            smtp.send_message(mensagem)
        _log(f"E-mail enviado para {destino}.")
        return True
    except Exception as erro:
        _log(f"E-mail falhou: {str(erro).strip()[:200]}")
        return False


def enviar_whatsapp(corpo: str, itens: list[dict], referencia: date) -> bool | None:
    token = os.environ.get("WHATSAPP_TOKEN", "").strip()
    phone_id = os.environ.get("WHATSAPP_PHONE_ID", "").strip()
    para = "".join(c for c in os.environ.get("WHATSAPP_PARA", "") if c.isdigit())
    if not (token and phone_id and para):
        return None  # canal não configurado

    modelo = os.environ.get("WHATSAPP_TEMPLATE", "").strip()
    if modelo:
        # Fora da janela de 24h, a Meta só aceita modelo aprovado. Os
        # parâmetros entram na ordem em que aparecem no modelo.
        total = sum(float(i["valor"] or 0) for i in itens)
        corpo_envio = {
            "messaging_product": "whatsapp",
            "to": para,
            "type": "template",
            "template": {
                "name": modelo,
                "language": {"code": os.environ.get("WHATSAPP_TEMPLATE_IDIOMA", "pt_BR")},
                "components": [
                    {
                        "type": "body",
                        "parameters": [
                            {"type": "text", "text": referencia.strftime("%d/%m/%Y")},
                            {"type": "text", "text": str(len(itens))},
                            {"type": "text", "text": moeda(total)},
                        ],
                    }
                ],
            },
        }
    else:
        # Texto livre só funciona dentro de 24h da última mensagem recebida.
        corpo_envio = {
            "messaging_product": "whatsapp",
            "to": para,
            "type": "text",
            "text": {"preview_url": False, "body": corpo},
        }

    url = f"https://graph.facebook.com/v21.0/{phone_id}/messages"
    pedido = urllib.request.Request(
        url,
        data=json.dumps(corpo_envio).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(pedido, timeout=TEMPO_LIMITE) as resposta:
            _log(f"WhatsApp enviado para {para} (HTTP {resposta.status}).")
            return True
    except urllib.error.HTTPError as erro:
        detalhe = erro.read().decode("utf-8", "replace")[:300]
        _log(f"WhatsApp falhou (HTTP {erro.code}): {detalhe}")
        if not modelo and erro.code == 400:
            _log(
                "Sem WHATSAPP_TEMPLATE, a Meta só aceita texto livre dentro de 24h "
                "da última mensagem recebida. Cadastre um modelo aprovado."
            )
        return False
    except Exception as erro:
        _log(f"WhatsApp falhou: {str(erro).strip()[:200]}")
        return False


def enviar_telegram(corpo: str) -> bool | None:
    token = os.environ.get("TELEGRAM_TOKEN", "").strip()
    chat = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not (token and chat):
        return None  # canal não configurado

    dados = urllib.parse.urlencode(
        {"chat_id": chat, "text": corpo.replace("*", ""), "disable_web_page_preview": "true"}
    ).encode("utf-8")
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        with urllib.request.urlopen(url, data=dados, timeout=TEMPO_LIMITE) as resposta:
            _log(f"Telegram enviado (HTTP {resposta.status}).")
            return True
    except Exception as erro:
        _log(f"Telegram falhou: {str(erro).strip()[:200]}")
        return False


# ---------------------------------------------------------------------------
# Execução
# ---------------------------------------------------------------------------
def main() -> int:
    referencia = hoje()
    incluir_atrasadas = os.environ.get("NOTIFICAR_INCLUIR_ATRASADAS") == "1"

    conexao = None
    try:
        conexao = psycopg2.connect(**parametros_banco())
        with conexao.cursor() as cur:
            cur.execute(DDL_REGISTRO)
            cur.execute(INDICE_REGISTRO)
        conexao.commit()

        itens = buscar_vencimentos(conexao, referencia.isoformat(), incluir_atrasadas)
        if not itens:
            _log("Nenhuma parcela vencendo hoje. Nada a avisar.")
            return 0

        dia = referencia.isoformat()
        pendentes = filtrar_nao_avisadas(conexao, itens, dia)
        if not pendentes:
            _log(f"{len(itens)} parcela(s) vencendo, mas todas já foram avisadas hoje.")
            return 0

        assunto, corpo = montar_mensagem(pendentes, referencia)
        _log(f"{len(pendentes)} parcela(s) a avisar.")

        if TESTE:
            _log("Modo teste — nada será enviado nem registrado. Mensagem montada:")
            print("\n" + corpo + "\n", flush=True)
            return 0

        resultados = {
            "e-mail": enviar_email(assunto, corpo),
            "whatsapp": enviar_whatsapp(corpo, pendentes, referencia),
            "telegram": enviar_telegram(corpo),
        }
        configurados = {nome: ok for nome, ok in resultados.items() if ok is not None}

        if not configurados:
            _log(
                "Nenhum canal configurado. Defina NOTIFICAR_EMAIL_PARA, "
                "WHATSAPP_TOKEN ou TELEGRAM_TOKEN."
            )
            return 1

        enviados = [nome for nome, ok in configurados.items() if ok]
        if not enviados:
            _log("Nenhum canal conseguiu enviar. Nada foi marcado como avisado.")
            return 1

        # Só marca como avisado se pelo menos um canal entregou — assim uma
        # falha temporária não faz o aviso ser perdido para sempre.
        registrar_envio(conexao, pendentes, dia, ", ".join(enviados))
        conexao.commit()
        _log(f"Avisado por: {', '.join(enviados)}.")
        return 0

    except Exception as erro:
        _log(f"ERRO: {str(erro).strip()[:300]}")
        if conexao is not None:
            try:
                conexao.rollback()
            except Exception:
                pass
        return 1
    finally:
        if conexao is not None:
            try:
                conexao.close()
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())
