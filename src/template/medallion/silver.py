from ..baseTask import BaseTask

class SilverTask(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def run(self):
        print("Executando a Camada Silver (Cleansing & MERGE)...")

        # 1. Garantir que o schema existe
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS main.silver")

        # 2. Criar a tabela Silver
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS main.silver.sales_cleaned (
                sale_id INT,
                customer_name STRING,
                amount DOUBLE,
                status STRING,
                updated_at TIMESTAMP
            )
        """)

        # 3. Executar o MERGE (Upsert) lendo da Bronze
        print("Fazendo o MERGE da Bronze para a Silver...")
        self.spark.sql("""
            MERGE INTO main.silver.sales_cleaned AS target
            USING main.bronze.sales_raw AS source
            ON target.sale_id = source.sale_id
            
            WHEN MATCHED THEN
              UPDATE SET 
                target.customer_name = source.customer_name,
                target.amount = source.amount,
                target.status = source.status,
                target.updated_at = current_timestamp()
                
            WHEN NOT MATCHED THEN
              INSERT (sale_id, customer_name, amount, status, updated_at)
              VALUES (source.sale_id, source.customer_name, source.amount, source.status, current_timestamp())
        """)
        
        if self.config.get_value("debug"):
            self.spark.sql("SELECT * FROM main.silver.sales_cleaned").show()

        print("Camada Silver finalizada com sucesso!")
