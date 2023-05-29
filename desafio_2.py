import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Configurações do banco de dados de origem
db_host = '40b8f30251.nxcli.io'
db_user = 'a4f2b49a_padawan'
db_password = 'KaratFlanksUgliedSpinal'
db_name = 'a4f2b49a_sample_database'
db_port = 3306

# Configurações do banco de dados local
local_db_host = 'localhost'
local_db_user = 'root'
local_db_password = '1234'
local_db_name = 'dados_consolidados'
local_db_port = 3306

# Conectar ao banco de dados de origem
source_engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Consulta SQL para recuperar os dados da tabela
query = "SELECT datahora_acesso, modalidade, rake, clientes_id FROM raw_data"

# Ler os dados do banco de dados de origem em um DataFrame
data = pd.read_sql(query, source_engine)

# Fechar a conexão com o banco de dados de origem
source_engine.dispose()

# Converter a coluna datahora_acesso para o formato adequado
data['datahora_acesso'] = pd.to_datetime(data['datahora_acesso'], errors='coerce')

# Criar a coluna 'mes'
data['mes'] = data['datahora_acesso'].dt.to_period('M').dt.to_timestamp().dt.strftime('%m')

# Consolidação dos dados
consolidacao = data.groupby('mes').agg(
    rake=('rake', lambda x: round(x.sum(), 2)),
    jogadores=('clientes_id', 'nunique'),
    rake_cash_game=('rake', lambda x: round(x[data['modalidade'] == 'Cash Game'].sum(), 2)),
    rake_torneio=('rake', lambda x: round(x[data['modalidade'] == 'Torneio'].sum(), 2)),
    jogadores_cash_game=('clientes_id', lambda x: x[data['modalidade'] == 'Cash Game'].nunique()),
    jogadores_torneio=('clientes_id', lambda x: x[data['modalidade'] == 'Torneio'].nunique())
).reset_index()

# Conectar ao banco de dados local
local_engine = create_engine(f'mysql+mysqlconnector://{local_db_user}:{local_db_password}@{local_db_host}:{local_db_port}/{local_db_name}')

# Nome da tabela para salvar os dados consolidados
table_name = 'dado_consolidado'

# Salvar os dados consolidados no banco de dados local
consolidacao['mes'] = consolidacao['mes'].astype(str)
consolidacao.to_sql(table_name, local_engine, if_exists='replace', index=False)

# Fechar a conexão com o banco de dados local
local_engine.dispose()

print("Dados consolidados foram salvos com sucesso na tabela:", table_name)