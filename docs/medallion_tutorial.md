# Arquitetura Medalhão no Databricks (SQL Serverless + Unity Catalog)

A **Arquitetura Medalhão** é um padrão de design de dados que organiza os dados em camadas, melhorando a qualidade e a estrutura a cada passo.

As três camadas são:
1. **Bronze (Dados Brutos):** Os dados exatamente como chegam da fonte (ex: um arquivo JSON ou CSV). Pode conter duplicatas e erros.
2. **Silver (Dados Limpos e Refinados):** Os dados são filtrados, limpos e padronizados. Aqui usamos o comando `MERGE` para atualizar registros que mudaram e inserir novos registros (Upsert).
3. **Gold (Dados Agregados para Negócios):** Dados prontos para consumo por relatórios e dashboards. Geralmente são agrupamentos ou cálculos finais.

Abaixo, vamos construir um fluxo simples de **Vendas (Sales)**.

---

## 🏗️ 1. Preparando o Ambiente (Unity Catalog)

No Unity Catalog, a hierarquia é: `Catalog` > `Schema` (Database) > `Table`.

```sql
-- Criando os schemas (bancos de dados) para cada camada
CREATE SCHEMA IF NOT EXISTS main.bronze;
CREATE SCHEMA IF NOT EXISTS main.silver;
CREATE SCHEMA IF NOT EXISTS main.gold;
```

---

## 🥉 2. Camada Bronze (Raw Data)

Na camada bronze, nós apenas salvamos os dados brutos. Se uma venda for atualizada na origem, nós recebemos uma nova linha aqui em vez de modificar a antiga (histórico puro).

```sql
-- 1. Cria a tabela Bronze (Append-only)
CREATE TABLE IF NOT EXISTS main.bronze.sales_raw (
    sale_id INT,
    customer_name STRING,
    amount DOUBLE,
    status STRING,
    ingested_at TIMESTAMP
);

-- 2. Simulando a chegada de dados novos (Carga Diária)
INSERT INTO main.bronze.sales_raw VALUES 
(1, 'Arthur', 150.00, 'Pending', current_timestamp()),
(2, 'Maria', 300.50, 'Completed', current_timestamp());
```

---

## 🥈 3. Camada Silver (Cleansed Data com MERGE)

Aqui garantimos que a tabela Silver tenha sempre a foto mais atual de cada venda. Se o 'Arthur' pagou o pedido que estava pendente, a nova linha na Bronze vai vir com 'Completed' e nós faremos o **MERGE** (Upsert) na Silver.

```sql
-- 1. Cria a tabela Silver
CREATE TABLE IF NOT EXISTS main.silver.sales_cleaned (
    sale_id INT,
    customer_name STRING,
    amount DOUBLE,
    status STRING,
    updated_at TIMESTAMP
);

-- 2. O comando MERGE (Upsert)
-- Ele lê da Bronze e compara com a Silver baseando-se no 'sale_id'
MERGE INTO main.silver.sales_cleaned AS target
USING main.bronze.sales_raw AS source
ON target.sale_id = source.sale_id

-- Se o ID já existir, atualizamos a linha (UPDATE)
WHEN MATCHED THEN
  UPDATE SET 
    target.customer_name = source.customer_name,
    target.amount = source.amount,
    target.status = source.status,
    target.updated_at = current_timestamp()

-- Se o ID não existir, inserimos a nova linha (INSERT)
WHEN NOT MATCHED THEN
  INSERT (sale_id, customer_name, amount, status, updated_at)
  VALUES (source.sale_id, source.customer_name, source.amount, source.status, current_timestamp());
```

*Por que isso é poderoso?* Se amanhã chegar um arquivo com a venda `1` marcada como `Completed`, e a nova venda `3`, o `MERGE` vai atualizar a `1` e inserir a `3` no mesmo comando, sem duplicar dados!

---

## 🥇 4. Camada Gold (Business-level Data)

Na camada Gold, nós não nos preocupamos mais com IDs individuais, mas sim com métricas para painéis ou BI. 

```sql
-- 1. Criando ou substituindo a tabela agregada
CREATE OR REPLACE TABLE main.gold.daily_sales_summary AS
SELECT 
    status,
    COUNT(sale_id) AS total_orders,
    SUM(amount) AS total_revenue
FROM main.silver.sales_cleaned
GROUP BY status;

-- 2. Visualizando os dados finais (Pronto para o Dashboard!)
SELECT * FROM main.gold.daily_sales_summary;
```

Isso é tudo o que você precisa para entender e aplicar a arquitetura Medalhão de forma simples no Databricks SQL!
