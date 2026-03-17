from ..baseTask import BaseTask

class BronzeTask(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def run(self):
        print("Executando a Camada Bronze...")

        # 1. Garantir que o schema existe
        self.spark.sql("CREATE SCHEMA IF NOT EXISTS main.bronze")

        # 2. Criar a tabela Bronze (Append-only)
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS main.bronze.sales_raw (
                sale_id INT,
                customer_name STRING,
                amount DOUBLE,
                status STRING,
                ingested_at TIMESTAMP
            )
        """)

        # 3. Inserir dados brutos (Simulando uma carga de dados)
        print("Inserindo dados brutos na Bronze...")
        self.spark.sql("""
            INSERT INTO main.bronze.sales_raw VALUES 
            (1, 'Arthur', 150.00, 'Pending', current_timestamp()),
            (2, 'Maria', 300.50, 'Completed', current_timestamp())
        """)
        
        if self.config.get_value("debug"):
            self.spark.sql("SELECT * FROM main.bronze.sales_raw").show()
        
        print("Camada Bronze finalizada com sucesso!")
