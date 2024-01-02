import os
from google.cloud import bigquery
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Recupera o caminho do arquivo JSON de suas credenciais a partir da variável de ambiente
SERVICE_ACCOUNT_JSON = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

dataset_id = "corded-shard-360618.dataset_py"

dataset = bigquery.Dataset(dataset_id)

dataset.location = "US"
dataset.description = "my new dataset"

dataset_ref = client.create_dataset(dataset, timeout=30)  # Make an API request.

print("Created dataset {}.{}".format(client.project, dataset_ref.dataset_id))
