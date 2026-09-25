# Sistema de Honorários

Controle de contratos, parcelas e recebimentos de honorários advocatícios.

**Stack:** Streamlit · PostgreSQL (Supabase) · deploy no Railway

---

## Telas

| Tela | Para quê |
|---|---|
| 📊 Dashboard | Totais, inadimplência, próximos vencimentos e recebimentos por mês |
| ➕ Novo Contrato | Cadastro com honorários iniciais, liminar, êxito e dados do processo |
| 💰 Pagamentos | Baixa de parcelas, sucumbência e emissão de recibo (WhatsApp ou PDF) |
| 📂 Meus Contratos | Edição, cronogramas da redução e do êxito, estorno de baixas e arquivamento |
| 📁 Arquivados | Contratos encerrados pelo escritório |
| ⚙️ Gestão | Listas de escolha, exclusão, backup em Excel e diagnóstico da conexão |

---

## Listas de escolha

**Tipo da Ação** e **Origem do Cliente** são listas que o escritório mantém
sozinho: as opções ficam na tabela `opcoes`, não numa constante do código.

Para criar uma opção nova, use o **➕** logo abaixo do campo, em Novo Contrato
ou Editar Contrato — ela já fica escolhida no contrato que está sendo
preenchido. Para conferir ou apagar, **⚙️ Gestão → 📝 Listas**.

As opções padrão entram **uma única vez**, quando a categoria ainda está
vazia. O que o escritório apagar não volta no deploy seguinte.

O contrato guarda o **texto** escolhido, não uma referência. Apagar uma opção
da lista tira ela das próximas escolhas e não mexe nos contratos que já a
usavam — eles continuam mostrando o que foi escolhido na época, e a opção
reaparece na lista se aquele contrato for editado.

---

## As quatro origens do dinheiro

Um contrato pode receber por quatro caminhos, e cada um entra na conta de um jeito:

| Origem | Como se cobra | Onde |
|---|---|---|
| **Honorários iniciais** | parcelado no cadastro do contrato | `parcelas` |
| **Redução da liminar** | cronograma criado depois, quando a tutela sai | `parcelas_liminar` |
| **Êxito** | cronograma criado depois, ou recebimento de uma vez | `parcelas_exito` ou `contratos.exito_*` |
| **Sucumbência** | não se cobra — quem paga é a parte contrária | `contratos.sucumbencia_*` |

O **êxito** só tem valor conhecido quando a causa resolve, então o cronograma
nasce em **📂 Meus Contratos → 🏆 Parcelas dos Honorários de Êxito**, não no
cadastro — ali existe apenas o percentual combinado. Havendo cronograma, ele
manda: o recebimento de uma vez fica de fora da conta, para o mesmo êxito não
ser somado duas vezes.

O **êxito por percentual** fica fora do total enquanto não é recebido nem
parcelado. O valor depende do resultado da causa, e somá-lo como incógnita
faria a barra de progresso nunca fechar em 100%.

A **sucumbência** entra no total e no recebido ao mesmo tempo, nunca como
pendência: só se sabe quanto é quando o dinheiro entra.

---

## Arquivamento

Arquivar é uma decisão do escritório, não um cálculo: fica gravado em
`contratos.arquivado_em`. O contrato sai do painel, dos avisos de vencimento e
da lista de Pagamentos, e passa a aparecer em **📁 Arquivados**. Nada é
apagado, e dá para desarquivar quando quiser.

O botão fica em **📂 Meus Contratos**, abaixo dos indicadores do contrato. Se
ainda houver algo a receber, o sistema diz o que é e pede uma confirmação
extra em vez de bloquear — contrato com êxito por percentual fica "em aberto"
para sempre quando a causa é perdida, e ninguém deve ficar preso a isso.

> Antes, "arquivado" era calculado como `saldo_devedor <= 0`. Como a maioria
> dos contratos deste escritório não tem honorário inicial, eles nasciam com
> saldo zero e apareciam como quitados no primeiro dia, ainda devendo a
> redução inteira.

---

## Rodando localmente

```bash
pip install -r requirements.txt
streamlit run financeiro.py
```

A configuração vem de variáveis de ambiente ou de `.streamlit/secrets.toml`.
Localmente, o caminho mais simples é criar o `secrets.toml` — ele está no
`.gitignore` e **nunca** deve ser versionado, já que este repositório é público:

```toml
[supabase]
host     = "..."
port     = "5432"
dbname   = "postgres"
user     = "..."
password = "..."

[credenciais]
usuario = "..."
senha   = "..."
```

---

## Deploy no Railway

O deploy é automático a cada push na `main`. O comando de start está no
`Procfile`. As variáveis necessárias:

| Variável | Observação |
|---|---|
| `SUPABASE_HOST` | obrigatória |
| `SUPABASE_USER` | obrigatória |
| `SUPABASE_PASSWORD` | obrigatória |
| `SUPABASE_PORT` | padrão `5432` |
| `SUPABASE_DBNAME` | padrão `postgres` |
| `CRED_USUARIO` | login do sistema |
| `CRED_SENHA` | senha do sistema |
| `DB_MAX_CONEXOES` | opcional, padrão `5` |
| `KEEPALIVE_HORAS` | opcional, padrão `6` |

Na primeira execução o sistema cria sozinho as tabelas, as colunas que
faltarem e os índices. Não é preciso rodar nada à mão.

**Rollback:** Railway → *Deployments* → o deploy anterior → `⋯` → *Redeploy*.

---

## Como testar

Há três formas, da mais segura para a mais arriscada. **Nenhuma delas é o
sistema em produção** — ali qualquer clique escreve nos dados reais do
escritório.

### 1. Suíte automática (segundos, risco zero)

Não precisa de banco: a camada de conexão é simulada.

```bash
pip install -r requirements-dev.txt
python tests/rodar_todos.py
```

| Suíte | Cobre |
|---|---|
| `tests/teste_funcoes.py` | Validação de CPF/CNPJ, formatação, divisão de parcelas, datas, Excel e PDF |
| `tests/teste_banco.py` | Transação, rollback, atomicidade, conexão morta, retry e cache |
| `tests/teste_sql.py` | Todo o SQL conferido contra o parser oficial do PostgreSQL |

O GitHub Actions roda a suíte a cada push, com pandas 2 e pandas 3 — as duas
faixas que o `requirements.txt` aceita.

### 2. Prévia com dados falsos (clicar por tudo, risco zero)

```bash
streamlit run previa.py
```

Abre o sistema inteiro com contratos de exemplo, **sem banco e sem
credencial**. Dá para navegar por todas as telas, conferir layout, formatação
e navegação. As gravações são ignoradas.

Os dados de exemplo cobrem de propósito os casos que costumam quebrar: valor
na casa do milhão, contrato quitado, parcela atrasada, cliente sem telefone e
observação com acento.

Para conferir se o painel aguenta uma consulta com erro sem derrubar as demais:

```bash
SIMULA_FALHA=1 streamlit run previa.py
```

### 3. Ambiente de teste de verdade (o que ainda falta)

A prévia não exercita o banco: não pega erro de tipo de coluna, de permissão
nem de constraint. Para isso é preciso um **segundo projeto Supabase**, e
apontar as variáveis locais para ele:

1. Crie um projeto novo no Supabase (o plano gratuito permite mais de um).
2. Preencha `.streamlit/secrets.toml` com os dados desse projeto.
3. `streamlit run financeiro.py` — o sistema cria as tabelas sozinho.

Aí dá para cadastrar, pagar, estornar e excluir à vontade. É o único jeito de
testar o caminho completo sem tocar nos contratos reais.

> **Nunca teste gravação apontando para o banco de produção.** Se precisar
> mesmo, tire antes um backup em ⚙️ Gestão → Backup.

---

## Outros arquivos

- **`migracoes.sql`** — consultas de diagnóstico, melhorias de banco e o
  procedimento de **limpeza dos dados de teste** (bloco 6), para o dia em que
  o sistema entrar em uso real. Cada bloco diz se já foi aplicado. Nada aqui é
  necessário para o sistema funcionar no dia a dia.
- **`keepalive.py`** e **`README-keepalive.md`** — como evitar que o Supabase
  pause o projeto por inatividade no plano gratuito.
- **`notificar.py`** e **`README-notificacoes.md`** — aviso diário das parcelas
  que vencem no dia, por e-mail, Telegram ou WhatsApp. Roda como Cron Job do
  Railway e não repete aviso no mesmo dia.
