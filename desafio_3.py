import requests
import mysql.connector
import pandas as pd

# Função para criar a conexão com o banco de dados
def create_connection():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="1234",
        database="api_python",
        port = 3306
    )
    return connection

# Conexão com API
url = "https://odds.p.rapidapi.com/v4/sports/soccer_brazil_campeonato/scores"
querystring = {"daysFrom": "3"}
headers = {
    "X-RapidAPI-Key": "7ce70cd8fbmsh16dd692b2bc06c3p186c76jsn4dfac271adfb",
    "X-RapidAPI-Host": "odds.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)
data = response.json()

# Lista para armazenar os dados das partidas
partidas = []
# Definindo condição para somente partidas completas
resultados_completados = [partida for partida in data if partida["completed"]]
# Extrai os dados das partidas da resposta da API
for partida in resultados_completados:
    datahora_partida = partida["commence_time"]
    data_partida = partida["commence_time"].split("T")[0]
    time_casa = partida["home_team"]
    time_fora = partida["away_team"]
    gols_time_casa = int(partida["scores"][0]["score"])
    gols_time_fora = int(partida["scores"][1]["score"])
    partidas.append([datahora_partida, data_partida, time_casa, time_fora, gols_time_casa, gols_time_fora])

# Criar DataFrame com os dados das partidas
df = pd.DataFrame(partidas, columns=["datahora_partida", "data_partida", "time_casa", "time_fora", "gols_time_casa", "gols_time_fora"])

# Salvar tabela no banco de dados
connection = create_connection()
if connection:
    try:
        cursor = connection.cursor()

        # Cria a tabela no banco de dados, se ainda não existir
        create_table_query = """
        CREATE TABLE IF NOT EXISTS partidas_brasileirao_serie_a_2023 (
            datahora_partida VARCHAR(50),
            data_partida DATE,
            time_casa VARCHAR(100),
            time_fora VARCHAR(100),
            gols_time_casa INT,
            gols_time_fora INT
        )
        """
        cursor.execute(create_table_query)
        # Insere os dados do DataFrame na tabela
        for row in df.itertuples(index=False):
            insert_query = """
            INSERT INTO partidas_brasileirao_serie_a_2023 (datahora_partida, data_partida, time_casa, time_fora, gols_time_casa, gols_time_fora)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, row)

        connection.commit()
        print("Dados salvos com sucesso na tabela 'partidas_brasileirao_serie_a_2023'!")
    # Finalizando conexão com o banco de dados
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexão com o banco de dados fechada.")

