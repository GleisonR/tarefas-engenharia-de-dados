# DESAFIOS DE UM ENGENHEIRO DE DADOS JUNIOR

### 1° DESAFIO: USANDO SQL PARA RESPONDER PERGUNTAS


Você encontrará as duas tabelas necessárias nessa pasta. Segue uma breve descrição de cada coluna: 


Tabela **resultado**:
- Data_acesso: o dia que o jogador realizou as ações.
- Clientes_id: o id do cliente, essa coluna pode ser usada para buscar as informações da 
  tabela clientes.
- Buyin: o valor total apostado pelo jogador.
- Winning: o valor total ganho pelo jogador, quando negativo, o prejuízo total do jogador.
- Rake: o lucro da empresa com esse jogador.
 
Tabela **clientes**:
- Id: o id do cliente, pode ser cruzado com a informação de clientes_id na tabela resultado.
- Sexo: sexo do jogador, sendo m=mascluno e f=feminino.
- Data_nascimento: ano, mês e dia de nascimento do jogador.
- Data_cadastro: data e hora de quando o jogador realizou cadastro.
- Cidade: cidade onde mora o jogador.
- Sigla: UF onde mora o jogador.


Considerando essas tabelas, responda o código em SQL que responde às seguintes perguntas:


- <b> 1.1 Quanto de rake foi gerado por cada Geração de jogadores? </b>
- <b> 1.2 Qual foi o rake gerado por mês? </b>
- <b> 1.3 Qual sexo tem uma maior proporção de ganhadores? </b>

Para essa atividade, considere cada geração tendo o seguinte critério: 
- Veteranos, geração  formada por pessoas que nasceram entre 1925 e 1940.   
- Baby Boomers são os nascidos entre 1941 e 1959. 
- Geração X, que compreende o período de 1960 a 1979.  
- Geração Y é composta por indivíduos que nasceram entre 1980 e 1995. 
- Geração Z é composta com os nascidos a partir de 1996 até 2010. 
- Geração Alpha, engloba todos os nascidos a partir de 2010 até a presente data. 

**Como ganhador, considere um jogador com Winning maior que 0**


### 1. SOLUÇÃO:

Para esse desafio foi usado o databricks, onde é uma ferramenta que possibilita utilizar python, pyspark e SQL no mesmo código. Mas nesse desafio o foco da consulta era SQL.

- IMPORT

Foi realizado o import dos 2 arquivos CSV para o databricks.

![image](https://github.com/GleisonR/Desafio/assets/116228613/2534ec86-879c-4528-bf3e-52b540f507f3)


Depois de realizar a importação, foi gerado um link para o arquivo de acesso, permitindo que eu o chame para o meu dataframe. Em Python, foi utilizado a codificação UTF-8, que é a mais comum no Brasil. A primeira linha do csv foi definido que se tratava de um cabeçalho com o código "header", e foi utilizado a vírgula como delimitador para separar as colunas 

![image](https://github.com/GleisonR/Desafio/assets/116228613/106fd14d-2a27-4458-90e3-c3aa8cfb4c97)

![image](https://github.com/GleisonR/Desafio/assets/116228613/a8174dba-0314-46ac-82c6-b25bc0bbd0e0)

Ainda em Python, foi criado uma TempView para utilizar SQL na tabela.

![image](https://github.com/GleisonR/Desafio/assets/116228613/ca2e0ad3-c539-4024-a335-bc836d58cb4a)

- VERIFICANDO INFORMAÇÕES

Agora, já conseguindo trabalhar com SQL, foi verificado as informações das tabelas, assim possibilitando visualizar os dados dentro do arquivo.

![image](https://github.com/GleisonR/Desafio/assets/116228613/983b8523-8d59-4110-9502-b86d8ae1598f)
![image](https://github.com/GleisonR/Desafio/assets/116228613/0dd02b09-a840-4712-ae90-b9db8bb466ec)




### 1.1 Quanto de rake foi gerado por cada Geração de jogadores?

- Nessa primeira etapa, é preciso do valor máximo gerado em rake para cada geração.

Nessa etapa, é preciso da coluna "rake" na tabela resultado e da coluna "data_nascimento" na tabela clientes. O valor de "rake" era uma string, então foi utilizado a função **'SUM(CAST(rake AS INT))'** para somar os valores da coluna "rake" após convertê-los para o tipo de dados inteiro (INT). Para a data de nascimento, foi utilizado o "between" para selecionar os dados que estão dentro de um intervalo específico. E, para finalizar, foi acrescentado o "inner join" para relacionar o "id" 
com "clientes_id" e assim unir as duas tabelas.

- Geração Baby Boomers

![image](https://github.com/GleisonR/Desafio/assets/116228613/dec63a5e-3ec7-4557-baaf-a95aee98f2c6)

- Geração X

![image](https://github.com/GleisonR/Desafio/assets/116228613/a0d0c0d5-1360-4b5f-892a-4301b5a321ee)

- Geração Y

![image](https://github.com/GleisonR/Desafio/assets/116228613/eeb8189f-b0bc-44d7-b5ea-0c06b12ed7c0)

- Geração Z

![image](https://github.com/GleisonR/Desafio/assets/116228613/2c898e10-ad4e-4b99-9366-5c882f59e599)

- Geração Alpha e Veteranos

Os dados começam em 1942 e terminam em 2008. Com isso, não há dados sobre a geração de Veteranos, formada por pessoas que nasceram entre 1925 e 1940, nem sobre a Geração Alpha, que engloba todos os nascidos a partir de 2010 até a presente data.


### 1.2 Qual foi o rake gerado por mês?

- Nessa segunda etapa, é preciso saber o valor máximo gerado em cada mês.

Foi verificado que só  é preciso usar a tabela resultado para essa consulta. Utilizando a função **EXTRACT(MONTH FROM resultado)**, que serve para extrair o mês de uma data especificada. Em relação ao "rake", utilizando a função **SUM(CAST(rake AS INT))** para somar os valores da coluna "rake" após convertê-los para o tipo de dados inteiro (INT). Foi finalizado com "GROUP BY" para agrupar os meses iguais e "ORDER BY" para ordenar em ordem crescente.

- ![image](https://github.com/GleisonR/Desafio/assets/116228613/44b15011-1d69-4ea9-9f18-c4902faa1910)


### 1.3 Qual sexo tem uma maior proporção de ganhadores

- Nessa terceira etapa é preciso veriricar qual sexo tem a quantidade maior quantidade de ganhadores.

Nessa etapa, foi verificado que iria utilizar novamente as duas tabelas, assim, o mesmo INNER JOIN que foi empregado na primeira etapa. Foi realizado a soma dos valores da coluna "winning" seguindo a mesma lógica da coluna "rake" das etapas anteriores. Utilizando a função **'WHERE c.sexo = 'm' AND r.winning > 0'** para filtrar os resultados de uma consulta com base em duas condições.

A primeira condição, c.sexo = 'm', indica que apenas os registros em que o valor da coluna "sexo" seja igual a 'm' (que geralmente representa o sexo masculino) serão incluídos no resultado da consulta.

Na segunda condição, r.winning > 0, indica que apenas os registros em que o valor da coluna "winning" seja maior que zero serão incluídos no resultado da consulta.

- Sexo masculino

![image](https://github.com/GleisonR/Desafio/assets/116228613/e5b1b538-da0d-4b09-9477-295dafb840df)

- Sexo Feminino

![image](https://github.com/GleisonR/Desafio/assets/116228613/44daee86-1d4a-4440-96f2-961fd1bf859d)

Analisando o valor retornado pelas duas consultas, verifica-se que o número de consultas do sexo masculino é maior, confirmando assim que o sexo masculino tem a maior quantidade de ganhadores.

<hr>

### 2° DESAFIO: MANIPULAÇÃO DE DADOS EM PYTHON

Escreva um script em Python que lê os dados de uma tabela num banco MySQL, consolida os dados e salva numa outra tabela de um banco local MySQL. Para a tarefa de read, iremos passar o acesso a um banco de dados. Para o write, queremos que você suba um banco localmente para testar o script.

Acesso ao banco de dados:

- Banco: a4f2b49a_sample_database
- Host:40b8f30251.nxcli.io
- User: a4f2b49a_padawan
- Password: KaratFlanksUgliedSpinal
- Port: 3306

Os dados estão presentes na tabela raw_data. Essa tabela contém as colunas:
- datahora_acesso: o timestamp em que o jogador realizou a ação
- modalidade: o tipo de jogo, podendo ser Cash Game ou Torneio
- rake: o lucro gerado por esse jogador
- clientes_id: id do jogador

Queremos que você consolide os resultados por mês, sendo que a tabela consolidada terá as seguintes colunas:

- mes: o mês em que os jogadores realizaram a ação
- rake: a soma total do rake no mês
- jogadores: a quantidade distinta de jogadores que jogaram cash game ou torneio
- rake_cash_game: a soma do rake da modalidade cash game gerado no mês
- rake_torneio: a soma do rake da modalidade torneio gerado no mês
- jogadores_cash_game: a quantidade distinta de jogadores que jogaram cash game no mês
- jogadores_torneio: a quantidade distinta de jogadores que jogaram torneio no mês


O script fará a seguinte sequência:
Ler os dados no banco MySQL -> Consolidar os dados -> Salvar os dados consolidados numa nova tabela. Utilize as bibliotecas que você se sentir mais confortável.


### 1. SOLUÇÃO:

- Verificando a tabela raw_data

Foi realizado a conexão com o banco de dados MySQL e também a extração da tabela raw_data para o banco de dados MySQL, assim possibilitand ter uma espectiva melhor da tabela.

![image](https://github.com/GleisonR/Desafio/assets/116228613/c5286d1e-d945-4c75-afa3-13ef0ae9f4da)

- Conexão com o banco de dados de origem

Agora, realizando as ações propostas pelo desafio, foi estabelecido a conexão com o banco de dados utilizando a biblioteca SQLAlchemy, pois o banco de dados de origem é SQL. A função 'create_engine' foi importada para que posteriormente eu possa acessar esse banco apenas pela variável 'source_engine'. Além disso, foi salvo em uma variável a consulta necessária para retornar todos os dados.

- Salvando os dados

O objetivo era trabalhar esses dados no pandas, então foi armazenado as variáveis referentes ao banco de dados e à consulta em um data frame. Em seguida, foi fechado a conexão, pois os dados necessários foram salvos data frame.

- Converter a coluna datahora_acesso para o formato adequado

Nessa coluna eu observei que existe 2 tipos de data, um que me fala a dia, mes, ano, hora, minutos e segundo e outro que me fala somente dia, mes e ano. Então importei a biblioteca "datetime" para fazer a formatação dessa coluna no meu dataframe.

- Criar a coluna 'mes'

É criada uma nova coluna chamada 'mes' no DataFrame data que contém o mês extraído da coluna 'datahora_acesso'. Os valores são convertidos para o formato de período mensal, usando dt.to_period('M'), e então convertidos de volta para o formato de data e hora usando dt.to_timestamp(). Por fim, os valores são convertidos em strings de dois dígitos usando dt.strftime('%m').

- Consolidação dos dados

Os dados são consolidados e salvos na variavel 'consolidacao', os dados são agrupandos pela coluna mês e realizando operações de agregação usando o groupby.

- Explicação da lógica

**rake=('rake', lambda x: round(x.sum(), 2)):** Essa operação calcula a soma dos valores da coluna 'rake' para cada grupo e arredonda o resultado para 2 casas decimais. Assim, o resultado é armazenado na coluna 'rake' 

**jogadores=('clientes_id', 'nunique'):** Essa operação calcula o número de valores únicos na coluna 'clientes_id' para cada grupo. Assim, o resultado é armazenado na coluna 'jogadores'.

**rake_cash_game=('rake', lambda x: round(x[data['modalidade'] == 'Cash Game'].sum(), 2)):** Essa operação calcula a soma dos valores da coluna 'rake' para as linhas onde a coluna 'modalidade' é igual a 'Cash Game'. Assim, o resultado é arredondado para 2 casas decimais e armazenado na coluna 'rake_cash_game'.

**rake_torneio=('rake', lambda x: round(x[data['modalidade'] == 'Torneio'].sum(), 2)):** Essa operação calcula a soma dos valores da coluna 'rake' para as linhas onde a coluna 'modalidade' é igual a 'Torneio'. O resultado é arredondado para 2 casas decimais e armazenado na coluna 'rake_torneio'.

**jogadores_cash_game=('clientes_id', lambda x: x[data['modalidade'] == 'Cash Game'].nunique()):** Essa operação calcula o número de valores únicos na coluna 'clientes_id' para as linhas onde a coluna 'modalidade' é igual a 'Cash Game'. Assim, o resultado é armazenado na coluna 'jogadores_cash_game'.

**jogadores_torneio=('clientes_id', lambda x: x[data['modalidade'] == 'Torneio'].nunique()):** Essa operação calcula o número de valores únicos na coluna 'clientes_id' para as linhas onde a coluna 'modalidade' é igual a 'Torneio'. Assim, resultado é armazenado na coluna 'jogadores_torneio'.

- Nome da tabela para salvar os dados consolidados

A variável table_name armazena o nome da tabela que será salva no banco de dados local.

- Salvamento dos dados consolidados

Os dados consolidados são salvos no banco de dados local usando o método to_sql, utilizando as variáveis table_name e local_engine. O parâmetro if_exists='replace' indica que a tabela deve ser substituída se já existir.

- Fechamento da conexão com o banco de dados local: 

A conexão com o banco de dados local é encerrada usando o método dispose() do SQLAlchemy.

- Dados salvos no banco de dados local:

![image](https://github.com/GleisonR/Desafio/assets/116228613/33762fe3-b627-476f-8371-150120e9e5ce)

<hr>

### 3° DESAFIO: ACESSANDO APIs COM PYTHONA
  
Para essa atividade queremos que você utilize essa 'https://rapidapi.com/theoddsapi/api/live-sports-odds/' API. Nela você encontrará resultados de partidas de diversos esportes. Queremos que você utilize o endpoint de scores para buscar determinadas informações do sport soccer_brazil_campeonato na resposta da API e salvar numa tabela chamada partidas_brasileirao_serie_a_2023 contendo as seguintes colunas:

- datahora_partida
- data_partida
- time_casa
- time_fora
- gols_time_casa
- gols_time_fora

**Obs: trazer apenas os resultados de jogos completos.**

Essas informações devem ser salvas num banco local em MYSQL, o mesmo utilizado para as outras atividades.


### 1. SOLUÇÃO:

- Função para criar a conexão com o banco de dados local
  
Uma função que cria e retorna uma conexão com um banco de dados MySQL usando os parâmetros fornecidos. Essa função vai ser chamada posteriormente para obter uma conexão ativa com o banco de dados e realizar operações de banco de dados, como consultas e atualizações.
  
- Conexão com API
  
Foi criado uma conexão que realiza uma solicitação HTTP GET para a URL de uma API usando os cabeçalhos e os parâmetros fornecidos. Em seguida, ele extrai o conteúdo da resposta em formato JSON e o armazena na variável data para uso posterior no código. Esse esse script foi realizado de acordo com a documentação da API.
  
- Lista para armazenar os dados das partidas

Uma lista é criada para armazenar os dados coletados pela API, permitindo que essa variável seja referenciada posteriormente para acessar os dados obtidos.
  
- Definindo condição para somente partidas completas
  
Objetivo é cria uma nova lista chamada resultados_completados que contém apenas os elementos da lista data onde o valor da chave "completed" é verdadeiro. Isso filtra os dados da lista original e armazena apenas os resultados completados na nova lista.
  
- Explicação da lógica
  
Um loop foi criado utilizado para iterar sobre cada partida na lista resultados_completados.

**datahora_partida** está recebendo a informação de data e hora de início da partida, armazenada na chave 'commence_time'.

**data_partida** está recebendo apenas a informação de data da partida, obtida a partir da chave 'commence_time'. A string é dividida usando o separador 'T' e a parte antes do separador é selecionada e fornecer apenas a data.

**time_casa** está recebendo o nome do time da casa na partida, obtido da chave 'home_team'.

**time_fora** está recebendo o nome do time visitante na partida, obtido da chave 'away_team'.

**gols_time_casa** está recebendo a quantidade de gols marcados pelo time da casa. Ele acessa a lista 'scores' na posição 0 e, em seguida, a chave 'score', convertendo o valor para um inteiro.

**gols_time_fora** está recebendo a quantidade de gols marcados pelo time visitante. Ele acessa a lista 'scores' na posição 1 e, em seguida, a chave 'score', convertendo o valor para um inteiro.

Uma nova lista é criada chamada partidas. Para cada partida, uma lista com as informações de **datahora_partida, data_partida, time_casa, time_fora, gols_time_casa e gols_time_fora** é adicionada à lista partidas. Essa lista representa uma partida completa com todas as informações extraídas.

- DataFrame com os dados das partidas
  
Foi criado um DataFrame chamado df. O df é construído a partir da lista partidas, onde cada item representa uma partida com informações como data e hora, times e gols marcados. As colunas do df são definidas como **'datahora_partida', 'data_partida', 'time_casa', 'time_fora', 'gols_time_casa' e 'gols_time_fora'**.
  
- Estabelecendo uma conexão com o banco de dados através de uma variável.

Uma variável chamada 'connection' é criada para armazenar o resultado de create_connection(), dessa forma, posteriormente, a conexão com o banco de dados será estabelecida utilizando essa variável. Assim, se a conexão foi estabelecida com sucesso usando a estrutura 'if connection:'.
  
- Criação da tabela no banco de dados 

Como o banco de dados local é MySQL foi criado um cursor a partir da conexão, que permite executar comandos SQL no banco de dados.

Uma variável foi criada chamada create_table_query contendo o comando SQL para criar uma tabela chamada 'partidas_brasileirao_serie_a_2023' no banco de dados, caso ela ainda não exista. 
  
- Inserindo os dados do DataFrame na tabela
 

Cria um loop for para iterar sobre as linhas do df. O método itertuples() é usado para gerar uma sequência de tuplas, onde cada tupla representa uma linha do df. Assim, é possível acessar os valores individuais de cada coluna da linha por meio da tupla. O parâmetro 'index=False' é utilizado para não incluir o índice das linhas na tupla resultante.

Uma variável chamada 'insert_query' contendo os comandos SQL para inserir os valores de cada linha do df na tabela "partidas_brasileirao_serie_a_2023".
  
- Fechando conexões

Nessa última etapa, as conexões com o DataFrame e com o banco de dados local são fechadas.
  
- Dados salvos no banco de dados MySQL

![image](https://github.com/GleisonR/Desafio/assets/116228613/c2c9d6e1-7d21-49aa-8ae0-3cb103822e60)

  
 



















