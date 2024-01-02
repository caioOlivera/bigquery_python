import os
from google.cloud import bigquery
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Recupera o caminho do arquivo JSON de suas credenciais
SERVICE_ACCOUNT_JSON = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

# Substitua pelos detalhes específicos do seu projeto, conjunto de dados e tabela.
project_id = 'corded-shard-360618'
dataset_name = 'dataset_py'
table_name = 'table_py'

# Construct a BigQuery client object.
client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

# Construa o ID completo da tabela.
table_id = f"{project_id}.{dataset_name}.{table_name}"

# Configuração do trabalho de carga.
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("gender", "STRING"),
        bigquery.SchemaField("count", "INTEGER")
    ],
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True,
)

# Substitua pelo caminho do seu arquivo CSV.
file_path = '/home/caio/Downloads/yob1880.txt'

# Carregue o arquivo para a tabela.
with open(file_path, "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)

# Aguarde a conclusão do trabalho.
job.result()

# Recupere informações da tabela e imprima.
table = client.get_table(table_id)
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)
