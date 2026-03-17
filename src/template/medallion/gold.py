from ..baseTask import BaseTask

class GoldTask(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def run(self):
        print("Executando a Camada Gold (Aggregations)...")

        # 1. Garantir que o schema existe
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS main.gold")

        # 2. Criar a tabela agregada (Substituindo com dados frescos da silver)
        print("Agregando dados da Silver para a Gold...")
        self.spark.sql("""
            CREATE OR REPLACE TABLE main.gold.daily_sales_summary AS
            SELECT 
                status,
                COUNT(sale_id) AS total_orders,
                SUM(amount) AS total_revenue
            FROM main.silver.sales_cleaned
            GROUP BY status
        """)
        
        if self.config.get_value("debug"):
            self.spark.sql("SELECT * FROM main.gold.daily_sales_summary").show()

        print("Camada Gold finalizada com sucesso!")
