# Projeto de Business Intelligence — Datamart de Vendas

## Entrega 1 — Descrição do Dataset e Objetivo de Negócio

### Descrição do Dataset

O conjunto de dados escolhido é o **Sales Forecasting Dataset**, disponível no [Kaggle](https://www.kaggle.com/datasets/rohitsahoo/sales-forecasting).  
O dataset contém **registros de vendas de uma operação de varejo**, abrangendo informações sobre **pedidos, clientes, produtos, localização e valor de venda**.

Cada linha representa **um item de pedido** (um produto vendido em uma data específica para um cliente).

#### Estrutura do dataset

| Coluna | Descrição |
|--------|------------|
| **Row ID** | Identificador único da linha do dataset. |
| **Order ID** | Identificador do pedido realizado. |
| **Order Date** | Data em que o pedido foi feito. |
| **Ship Date** | Data em que o pedido foi enviado. |
| **Ship Mode** | Modalidade de envio (Standard, First Class, etc.). |
| **Customer ID** | Identificador único do cliente. |
| **Customer Name** | Nome do cliente. |
| **Segment** | Segmento do cliente (Consumer, Corporate ou Home Office). |
| **Country** | País do cliente. |
| **City** | Cidade onde o pedido foi realizado. |
| **State** | Estado ou província. |
| **Postal Code** | Código postal. |
| **Region** | Região geográfica do país. |
| **Product ID** | Identificador único do produto. |
| **Category** | Categoria principal do produto. |
| **Sub-Category** | Subcategoria do produto. |
| **Product Name** | Nome do produto vendido. |
| **Sales** | Valor total da venda em dólares. |

---

###  Objetivo de Negócio

O objetivo do projeto é **analisar o desempenho das vendas da empresa** ao longo do tempo, identificando padrões e oportunidades de crescimento.  
A partir da construção de um **Datamart de Vendas**, pretende-se responder a perguntas gerenciais como:

1. **Como evoluem as vendas ao longo do tempo (mês, trimestre, ano)?**  
2. **Quais regiões, estados e cidades geram maior receita?**  
3. **Quais categorias e subcategorias de produtos são mais rentáveis?**  
4. **Quais segmentos de clientes mais contribuem para as vendas?**  
5. **Qual o tempo médio entre a realização e o envio dos pedidos?**  

Essas análises permitirão compreender o **comportamento de compra dos clientes**, otimizar o **planejamento logístico e de estoque**, e **apoiar decisões estratégicas** de marketing e operações.

---
