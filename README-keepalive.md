# Impedir que o Supabase pause o projeto

## O problema

O Supabase pausa projetos do plano gratuito após **7 dias de baixa atividade**.
A documentação oficial diz que "algumas requisições de usuário ao banco por dia,
ao longo da semana anterior" costumam ser suficientes para evitar a pausa.

O `financeiro.py` já tem um keep-alive embutido que faz esse ping a cada 6 horas.
Mas ele tem um limite que nenhum ajuste de código resolve:

> **O agendador roda dentro do processo do Streamlit.**
> Ele só começa quando alguém abre a página, e morre junto com o contêiner.

Ou seja, ele **não** protege nestes casos:

| Situação | O ping acontece? |
|---|---|
| Site aberto de tempos em tempos, contêiner de pé | ✅ Sim |
| Railway reiniciou (deploy, queda, manutenção) e ninguém abriu o site desde então | ❌ Não |
| "Serverless / App Sleeping" ligado no Railway e o serviço dormiu | ❌ Não |
| Escritório de férias, ninguém abre o sistema por 10 dias | ❌ Não |

Por isso existe o `keepalive.py`: um script independente, que não depende do
site estar de pé nem de alguém tê-lo aberto.

---

## Opção A — Cron Job no Railway (recomendada)

Fica no mesmo projeto e reaproveita as variáveis de ambiente que já existem.

1. No seu projeto do Railway: **New** → **GitHub Repo** → escolha o mesmo repositório.
2. Nesse novo serviço, abra **Settings**:
   - **Custom Start Command**: `python keepalive.py`
   - **Cron Schedule**: `0 */6 * * *`  *(a cada 6 horas)*
3. Em **Variables**, use **Add Variable Reference** para puxar do serviço do site:
   `SUPABASE_HOST`, `SUPABASE_PORT`, `SUPABASE_DBNAME`, `SUPABASE_USER`, `SUPABASE_PASSWORD`.
4. Faça deploy e confira em **Deployments → Logs**. Você deve ver:

```
[keep-alive] 19/08/2026 06:00:03 — OK, 37 contrato(s) no banco.
```

Um cron job só é cobrado pelos segundos em que roda — este script leva menos de
5 segundos, quatro vezes por dia.

> **Atenção:** confirme que **Serverless / App Sleeping está DESLIGADO** no
> serviço do site. Com ele ligado, o site dorme e leva vários segundos para
> acordar no primeiro acesso — e o keep-alive interno para junto.

---

## Opção B — GitHub Actions

Vale se você quiser algo que funcione **mesmo que o Railway inteiro esteja fora**.

Crie `.github/workflows/keepalive.yml`:

```yaml
name: Keep-alive Supabase

on:
  schedule:
    - cron: "0 */6 * * *"   # a cada 6 horas (horário UTC)
  workflow_dispatch:         # permite rodar na mão pelo botão do GitHub

jobs:
  ping:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: actions/setup-python@v6
        with:
          python-version: "3.12"
      - run: pip install psycopg2-binary
      - run: python keepalive.py
        env:
          SUPABASE_HOST:     ${{ secrets.SUPABASE_HOST }}
          SUPABASE_PORT:     ${{ secrets.SUPABASE_PORT }}
          SUPABASE_DBNAME:   ${{ secrets.SUPABASE_DBNAME }}
          SUPABASE_USER:     ${{ secrets.SUPABASE_USER }}
          SUPABASE_PASSWORD: ${{ secrets.SUPABASE_PASSWORD }}
```

Cadastre os valores em **Settings → Secrets and variables → Actions**.

**Dois detalhes importantes do GitHub Actions:**

- O GitHub **desativa workflows agendados** em repositórios sem commits por
  60 dias. Ele avisa por e-mail, e basta reativar com um clique.
- Se o repositório for **público**, qualquer pessoa vê o log — o script nunca
  imprime credenciais, mas vale manter o repositório privado de qualquer forma.

---

## Opção C — Monitor de uptime (complementar, não substitui)

Serviços como UptimeRobot ou cron-job.org podem chamar a URL do seu site a cada
5 minutos. Isso mantém o contêiner do Railway acordado, o que por tabela mantém
o keep-alive interno rodando.

Mas **abrir a página de login não consulta o banco** — quem consulta é o
agendador interno. Então isso funciona de forma indireta e só enquanto o
processo estiver vivo. Use junto com a Opção A, nunca no lugar dela.

---

## Ajustando a frequência do ping interno

O keep-alive embutido no site aceita uma variável opcional:

```
KEEPALIVE_HORAS=4
```

O padrão é `6`. Diminuir aumenta a margem de segurança; abaixo de 1 hora não faz
diferença prática e só gasta conexão.

---

## A solução definitiva

Nada acima é garantia — são todos contornos para um limite do plano gratuito. O
Supabase é explícito: **projetos pagos não são pausados por inatividade**.

Se o sistema já está em uso real no escritório, o Pro (US$ 25/mês) elimina a
categoria inteira de problema: sem pausa, sem keep-alive, sem madrugada
descobrindo que o banco dormiu. Enquanto isso não acontecer, a Opção A é o que
mais se aproxima.

---

## Se o projeto pausar mesmo assim

Nada é perdido: tabelas, dados e extensões ficam preservados, só inacessíveis.
No painel do Supabase aparece um botão **Restore project**. A restauração leva
alguns minutos, e o sistema volta a funcionar sem nenhuma alteração de
configuração.
