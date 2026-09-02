# Sistema de Honorários

Controle de contratos, parcelas e recebimentos de honorários advocatícios.

**Stack:** Streamlit · PostgreSQL (Supabase) · deploy no Railway

---

## Telas

| Tela | Para quê |
|---|---|
| 📊 Dashboard | Totais, inadimplência, próximos vencimentos e recebimentos por mês |
| ➕ Novo Contrato | Cadastro com honorários iniciais, liminar, êxito e dados do processo |
| 💰 Pagamentos | Baixa de parcelas e emissão de recibo (WhatsApp ou PDF) |
| 📂 Meus Contratos | Edição, parcelas da redução da liminar e estorno de baixas |
| 📁 Arquivados | Histórico dos contratos quitados |
| ⚙️ Gestão | Exclusão, backup em Excel e diagnóstico da conexão |

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

- **`migracoes.sql`** — melhorias opcionais de banco (valores em `NUMERIC`,
  regras de integridade) e consultas de diagnóstico. Nada aqui é necessário
  para o sistema funcionar. Faça backup antes de rodar.
- **`keepalive.py`** e **`README-keepalive.md`** — como evitar que o Supabase
  pause o projeto por inatividade no plano gratuito.
