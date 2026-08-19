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

## Testes

Não precisam de banco: a camada de conexão é simulada.

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

---

## Outros arquivos

- **`migracoes.sql`** — melhorias opcionais de banco (valores em `NUMERIC`,
  regras de integridade) e consultas de diagnóstico. Nada aqui é necessário
  para o sistema funcionar. Faça backup antes de rodar.
- **`keepalive.py`** e **`README-keepalive.md`** — como evitar que o Supabase
  pause o projeto por inatividade no plano gratuito.
