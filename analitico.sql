-- =====================================================================
-- 1) ROLLUP: Vendas por Ano e Mês, com totais por Ano e Total Geral
--    Dimensões: Tempo (ano, mês)
--    Função analítica: GROUP BY ROLLUP
-- =====================================================================

SELECT
  dd.year  AS ano,
  dd.month AS mes,
  SUM(fs.sales) AS total_vendas
FROM dw.fact_sales fs
JOIN dw.dim_date dd
  ON fs.order_date_sk = dd.date_sk
GROUP BY ROLLUP (dd.year, dd.month)
ORDER BY
  dd.year NULLS LAST,
  dd.month NULLS LAST;

-- Interpretação:
--  - Linhas com ano e mês preenchidos: total de vendas por ano/mês.
--  - Linhas com ano preenchido e mês NULL: subtotal do ano.
--  - Linha com ano NULL e mês NULL: total geral.


-- =====================================================================
-- 2) CUBE: Vendas por Região e Segmento, com totais por Região, por
--    Segmento e Total Geral
--    Dimensões: Local (região), Cliente (segmento)
--    Função analítica: GROUP BY CUBE
-- =====================================================================

SELECT
  dc.region  AS regiao,
  dc.segment AS segmento,
  SUM(fs.sales) AS total_vendas
FROM dw.fact_sales fs
JOIN dw.dim_customer dc
  ON fs.customer_sk = dc.customer_sk
GROUP BY CUBE (dc.region, dc.segment)
ORDER BY
  dc.region NULLS LAST,
  dc.segment NULLS LAST;

-- Interpretação:
--  - regiao + segmento: vendas por combinação região x segmento.
--  - regiao != NULL, segmento NULL: subtotal da região.
--  - regiao NULL, segmento != NULL: subtotal do segmento.
--  - ambos NULL: total geral.


-- =====================================================================
-- 3) RANK: Ranking das Categorias de Produto por Ano, ordenadas pelo
--    total de vendas (maior primeiro)
--    Dimensões: Tempo (ano), Produto (categoria)
--    Função analítica: RANK() OVER (PARTITION BY ...)
-- =====================================================================

WITH vendas_categoria AS (
  SELECT
    dd.year       AS ano,
    dp.category   AS categoria,
    SUM(fs.sales) AS total_vendas
  FROM dw.fact_sales fs
  JOIN dw.dim_date dd
    ON fs.order_date_sk = dd.date_sk
  JOIN dw.dim_product dp
    ON fs.product_sk = dp.product_sk
  GROUP BY
    dd.year,
    dp.category
)
SELECT
  ano,
  categoria,
  total_vendas,
  RANK() OVER (
    PARTITION BY ano
    ORDER BY total_vendas DESC
  ) AS rank_categoria_ano
FROM vendas_categoria
ORDER BY
  ano,
  rank_categoria_ano;

-- Interpretação:
--  - Para cada ano, as categorias são ranqueadas por total de vendas.
--  - Categorias com o mesmo total recebem o mesmo RANK.


-- =====================================================================
-- 4) DENSE_RANK: Top categorias por segmento de cliente, considerando
--    o total de vendas
--    Dimensões: Cliente (segmento), Produto (categoria)
--    Função analítica: DENSE_RANK() OVER (PARTITION BY ...)
-- =====================================================================

WITH vendas_segmento_categoria AS (
  SELECT
    dc.segment   AS segmento,
    dp.category  AS categoria,
    SUM(fs.sales) AS total_vendas
  FROM dw.fact_sales fs
  JOIN dw.dim_customer dc
    ON fs.customer_sk = dc.customer_sk
  JOIN dw.dim_product dp
    ON fs.product_sk = dp.product_sk
  GROUP BY
    dc.segment,
    dp.category
)
SELECT
  segmento,
  categoria,
  total_vendas,
  DENSE_RANK() OVER (
    PARTITION BY segmento
    ORDER BY total_vendas DESC
  ) AS dense_rank_categoria_segmento
FROM vendas_segmento_categoria
ORDER BY
  segmento,
  dense_rank_categoria_segmento,
  categoria;

-- Interpretação:
--  - Dentro de cada segmento, as categorias são ordenadas por total_vendas.
--  - DENSE_RANK não “pula” posições em caso de empate.
--  - Você pode filtrar, por exemplo, WHERE dense_rank_categoria_segmento <= 3
--    para pegar o “TOP 3” de cada segmento.


-- =====================================================================
-- 5) LAG: Vendas Mensais e Comparação com o Mês Anterior
--    Dimensões: Tempo (ano, mês)
--    Função analítica: LAG() OVER (ORDER BY ...)
-- =====================================================================

WITH vendas_mensais AS (
  SELECT
    dd.year  AS ano,
    dd.month AS mes,
    SUM(fs.sales) AS total_vendas
  FROM dw.fact_sales fs
  JOIN dw.dim_date dd
    ON fs.order_date_sk = dd.date_sk
  GROUP BY
    dd.year,
    dd.month
)
SELECT
  ano,
  mes,
  total_vendas,
  LAG(total_vendas) OVER (
    ORDER BY ano, mes
  ) AS vendas_mes_anterior,
  (total_vendas -
   LAG(total_vendas) OVER (ORDER BY ano, mes)
  ) AS diferenca_vs_mes_anterior
FROM vendas_mensais
ORDER BY
  ano,
  mes;

-- Interpretação:
--  - Para cada ano/mês, mostra o total do mês e o total do mês anterior.
--  - A coluna diferenca_vs_mes_anterior mostra quanto aumentou/diminuiu.
