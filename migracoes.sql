-- =============================================================================
-- Sistema de Honorários — migrações OPCIONAIS
--
-- O sistema funciona sem rodar nada deste arquivo: ele cria tabelas, colunas
-- e índices sozinho na inicialização. O que está aqui são melhorias de
-- integridade que valem a pena, mas que mexem em dados existentes e por isso
-- devem ser rodadas conscientemente no SQL Editor do Supabase.
--
-- FAÇA BACKUP ANTES  (⚙️ Gestão → Backup no próprio sistema).
-- Rode um bloco por vez e confira o resultado.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1) DIAGNÓSTICO — rode primeiro, não altera nada
-- -----------------------------------------------------------------------------

-- 1.1 Existem parcelas duplicadas? (impede o índice único do sistema)
SELECT contrato_id, nr_parcela, COUNT(*) AS repeticoes
FROM parcelas
GROUP BY contrato_id, nr_parcela
HAVING COUNT(*) > 1
ORDER BY repeticoes DESC;

SELECT contrato_id, nr_parcela, COUNT(*) AS repeticoes
FROM parcelas_liminar
GROUP BY contrato_id, nr_parcela
HAVING COUNT(*) > 1
ORDER BY repeticoes DESC;

-- 1.2 Saldo devedor bate com a soma das parcelas em aberto?
SELECT c.id, c.cliente,
       c.saldo_devedor                                   AS saldo_gravado,
       COALESCE(SUM(p.valor_parcela) FILTER (WHERE p.pago = 0), 0) AS saldo_calculado,
       c.saldo_devedor - COALESCE(SUM(p.valor_parcela) FILTER (WHERE p.pago = 0), 0) AS diferenca
FROM contratos c
LEFT JOIN parcelas p ON p.contrato_id = c.id
GROUP BY c.id, c.cliente, c.saldo_devedor
HAVING ABS(c.saldo_devedor - COALESCE(SUM(p.valor_parcela) FILTER (WHERE p.pago = 0), 0)) > 0.01
ORDER BY ABS(c.saldo_devedor - COALESCE(SUM(p.valor_parcela) FILTER (WHERE p.pago = 0), 0)) DESC;

-- 1.4 Que tipo tem cada coluna? (foi daqui que sairam os erros de UNION)
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN ('contratos', 'parcelas', 'parcelas_liminar')
ORDER BY table_name, ordinal_position;

-- 1.3 Contratos cuja observação virou "Pago" pelo bug antigo
--     (o texto original da anotação foi perdido; só dá para limpar o rótulo)
SELECT id, cliente, observacoes FROM contratos WHERE observacoes = 'Pago';


-- -----------------------------------------------------------------------------
-- 2) DINHEIRO EM NUMERIC  — JA APLICADO EM 02/09/2026
--
-- Todas as colunas de valor estao em NUMERIC. Nao ha nada a rodar aqui.
-- O bloco fica registrado para historico e para servir de referencia caso um
-- banco novo (ambiente de teste) precise da mesma correcao.
--
-- Por que importava: `real` e float de 4 bytes, com ~7 digitos de precisao.
-- Um contrato de R$ 1.234.567,89 nao cabia e era arredondado.
--
-- Para conferir que continua tudo certo (deve devolver ZERO linhas):
--
--   SELECT table_name, column_name, data_type
--   FROM information_schema.columns
--   WHERE table_schema = 'public' AND data_type = 'real'
--     AND table_name IN ('contratos', 'parcelas', 'parcelas_liminar');
--
-- O comando aplicado foi:
--
--   BEGIN;
--   ALTER TABLE contratos
--       ALTER COLUMN hon_inicial_valor       TYPE NUMERIC(14,2) USING ROUND(hon_inicial_valor::numeric, 2),
--       ALTER COLUMN hon_inicial_vlr_parcela TYPE NUMERIC(14,2) USING ROUND(hon_inicial_vlr_parcela::numeric, 2),
--       ALTER COLUMN hon_liminar_fixo        TYPE NUMERIC(14,2) USING ROUND(hon_liminar_fixo::numeric, 2),
--       ALTER COLUMN hon_liminar_reducao_vlr TYPE NUMERIC(14,2) USING ROUND(hon_liminar_reducao_vlr::numeric, 2),
--       ALTER COLUMN hon_exito_fixo          TYPE NUMERIC(14,2) USING ROUND(hon_exito_fixo::numeric, 2),
--       ALTER COLUMN exito_valor_recebido    TYPE NUMERIC(14,2) USING ROUND(exito_valor_recebido::numeric, 2),
--       ALTER COLUMN hon_exito_percentual    TYPE NUMERIC(6,2)  USING ROUND(hon_exito_percentual::numeric, 2);
--   ALTER TABLE parcelas_liminar
--       ALTER COLUMN valor_parcela TYPE NUMERIC(14,2) USING ROUND(valor_parcela::numeric, 2);
--   COMMIT;


-- 3) REGRAS DE INTEGRIDADE  — RECOMENDADO
--
-- Impedem que dados inconsistentes entrem no banco, mesmo por engano.
-- Se algum comando falhar, é porque já existem dados fora da regra: rode a
-- consulta de diagnóstico correspondente na seção 1 antes de insistir.
-- -----------------------------------------------------------------------------
BEGIN;

-- Nunca mais duas parcelas com o mesmo número no mesmo contrato.
CREATE UNIQUE INDEX IF NOT EXISTS uq_parcelas_contrato_nr
    ON parcelas (contrato_id, nr_parcela);
CREATE UNIQUE INDEX IF NOT EXISTS uq_plim_contrato_nr
    ON parcelas_liminar (contrato_id, nr_parcela);

-- "pago" só aceita 0 ou 1.
ALTER TABLE parcelas
    ADD CONSTRAINT ck_parcelas_pago CHECK (pago IN (0, 1)) NOT VALID;
ALTER TABLE parcelas_liminar
    ADD CONSTRAINT ck_plim_pago CHECK (pago IN (0, 1)) NOT VALID;

-- Valores não podem ser negativos.
ALTER TABLE parcelas
    ADD CONSTRAINT ck_parcelas_valor CHECK (valor_parcela >= 0) NOT VALID;
ALTER TABLE parcelas_liminar
    ADD CONSTRAINT ck_plim_valor CHECK (valor_parcela >= 0) NOT VALID;
ALTER TABLE contratos
    ADD CONSTRAINT ck_contratos_saldo CHECK (saldo_devedor >= 0) NOT VALID;

COMMIT;

-- NOT VALID acima faz a regra valer só para dados NOVOS, sem travar a migração
-- por causa de registros antigos. Quando o diagnóstico estiver limpo, valide:
-- ALTER TABLE parcelas        VALIDATE CONSTRAINT ck_parcelas_pago;
-- ALTER TABLE parcelas        VALIDATE CONSTRAINT ck_parcelas_valor;
-- ALTER TABLE parcelas_liminar VALIDATE CONSTRAINT ck_plim_pago;
-- ALTER TABLE parcelas_liminar VALIDATE CONSTRAINT ck_plim_valor;
-- ALTER TABLE contratos       VALIDATE CONSTRAINT ck_contratos_saldo;


-- -----------------------------------------------------------------------------
-- 4) LIMPEZA DO BUG ANTIGO DAS OBSERVAÇÕES  — OPCIONAL
--
-- A versão anterior gravava observacoes = 'Pago' quando o saldo zerava,
-- apagando a anotação real do contrato. A nova versão usa a coluna
-- `quitado_em` e nunca mais mexe em `observacoes`.
--
-- Este bloco só remove o rótulo "Pago" e preenche a data de quitação a partir
-- do último pagamento registrado. O texto original perdido não é recuperável.
-- -----------------------------------------------------------------------------
BEGIN;

UPDATE contratos c
   SET quitado_em = COALESCE(
        c.quitado_em,
        (SELECT LEFT(MAX(p.data_pagamento)::text, 10)
           FROM parcelas p
          WHERE p.contrato_id = c.id AND p.pago = 1)
   )
 WHERE c.saldo_devedor <= 0;

UPDATE contratos SET observacoes = NULL WHERE observacoes = 'Pago';

COMMIT;


-- -----------------------------------------------------------------------------
-- 5) DATAS COMO DATE  — NÃO RECOMENDADO POR ENQUANTO
--
-- As datas são TEXT no formato 'YYYY-MM-DD'. Isso ordena e compara
-- corretamente (é ISO 8601), então funciona bem hoje. Migrar para DATE/
-- TIMESTAMP exigiria mexer em todo o código Python que formata datas.
-- Deixado documentado apenas como direção futura:
--
-- ALTER TABLE parcelas
--     ALTER COLUMN data_vencimento TYPE DATE USING data_vencimento::date;
-- -----------------------------------------------------------------------------
