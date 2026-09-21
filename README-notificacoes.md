# Aviso diário de vencimentos

O `notificar.py` roda uma vez por dia, consulta as parcelas que vencem
naquele dia e avisa o escritório. Funciona como o `keepalive.py`: um Cron Job
do Railway, independente do site estar aberto.

Ele **não repete aviso**: cada parcela avisada fica registrada na tabela
`notificacoes_enviadas`, criada automaticamente na primeira execução.

---

## Antes de escolher o canal

O WhatsApp é o pedido mais comum e o mais burocrático. Vale entender por quê.

**Não existe envio automático por um número comum de WhatsApp.** Para mandar
mensagem programada é preciso a **API oficial do WhatsApp Business** (Meta
Cloud API), que exige:

- Conta no Meta Business com o **negócio verificado** (documentos da empresa)
- Um número de telefone **dedicado à API** — ele deixa de funcionar no app
  normal do WhatsApp
- **Modelos de mensagem aprovados pela Meta** para qualquer mensagem enviada
  por iniciativa do sistema
- Custo por conversa iniciada

Existem bibliotecas não-oficiais que automatizam o WhatsApp Web. Elas violam
os termos de uso e o risco concreto é o número ser banido. Para o número de
um escritório de advocacia, não compensa.

**Sugestão:** comece pelo e-mail, que funciona hoje e sem custo. Se o aviso
por WhatsApp for mesmo necessário, faça a configuração da Meta com calma — o
script já está pronto para ele.

---

## Opção A — E-mail (recomendada para começar)

Funciona em minutos e não depende de aprovação de ninguém.

Com Google Workspace, é preciso uma **senha de app** (a senha normal da conta
não funciona para SMTP):

1. Na conta Google que vai enviar, ative a verificação em duas etapas
2. Acesse **myaccount.google.com/apppasswords**
3. Gere uma senha de app e copie os 16 caracteres

No Railway, no serviço do aviso, cadastre:

```
NOTIFICAR_EMAIL_PARA=financeiro@gmfreitas.com.br
SMTP_HOST=smtp.gmail.com
SMTP_PORTA=587
SMTP_USUARIO=sistema@gmfreitas.com.br
SMTP_SENHA=<a senha de app de 16 caracteres>
```

`NOTIFICAR_EMAIL_PARA` aceita vários destinatários separados por vírgula.

---

## Opção B — Telegram (grátis, imediato)

Se o escritório topar usar Telegram, é o caminho mais simples de todos: sem
verificação, sem custo, sem modelo aprovado.

1. No Telegram, converse com **@BotFather** e mande `/newbot`
2. Ele devolve um **token**
3. Mande qualquer mensagem para o bot recém-criado
4. Abra `https://api.telegram.org/bot<TOKEN>/getUpdates` no navegador e
   copie o `chat.id` que aparecer

```
TELEGRAM_TOKEN=<o token do BotFather>
TELEGRAM_CHAT_ID=<o id copiado>
```

---

## Opção C — WhatsApp (API oficial da Meta)

Só siga se o escritório aceitar a burocracia descrita no início.

1. Crie uma conta no **Meta for Developers** e um app do tipo *Business*
2. Adicione o produto **WhatsApp**
3. Cadastre e verifique o número que vai **enviar** — lembre que ele não
   poderá mais ser usado no app comum
4. Gere um **token de acesso permanente** (o token de teste expira em 24h)
5. Em **Modelos de mensagem**, crie um modelo e aguarde a aprovação. Exemplo,
   com três variáveis na ordem que o script envia — data, quantidade, total:

   ```
   Vencimentos de {{1}}: {{2}} parcela(s), somando {{3}}.
   Confira o sistema de honorários.
   ```

```
WHATSAPP_TOKEN=<token permanente>
WHATSAPP_PHONE_ID=<id do número remetente>
WHATSAPP_PARA=5581995301233
WHATSAPP_TEMPLATE=<nome do modelo aprovado>
```

O `WHATSAPP_PARA` vai **só com dígitos**, incluindo o 55 do país. Confira o
nono dígito: celulares brasileiros têm 9 dígitos depois do DDD.

Sem `WHATSAPP_TEMPLATE` o script tenta texto livre, que a Meta só aceita
dentro de 24h da última mensagem que **você** recebeu daquele número. Serve
para testar, não para um aviso diário.

---

## Agendar no Railway

Igual ao keep-alive:

1. **+ New** → **GitHub Repo** → `sistemas-honorarios`
2. Renomeie para `notificacoes`
3. **Settings → Custom Start Command:** `python notificar.py`
4. **Settings → Cron Schedule:** `0 11 * * *`
   (11h UTC = **8h da manhã** em Brasília)
5. **Variables:** as cinco `SUPABASE_*` por *Variable Reference*, mais as do
   canal escolhido

---

## Testar antes de valer

Com `NOTIFICAR_TESTE=1`, o script monta a mensagem, mostra no log e **não
envia nem marca como avisada**:

```
NOTIFICAR_TESTE=1 python notificar.py
```

Serve para conferir o texto e ver se as parcelas certas foram encontradas.

---

## Outras opções

```
NOTIFICAR_INCLUIR_ATRASADAS=1
```

Além das que vencem hoje, lembra também das que já venceram e continuam em
aberto. Sem isso, o aviso é só do dia.

Vale notar: sem essa variável, uma parcela é avisada **uma única vez**, no dia
do vencimento. Com ela, as atrasadas voltam a aparecer todo dia até serem
baixadas — o que costuma ser o comportamento desejado para cobrança.
