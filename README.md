# DESAFIO DE UM ENGENHEIRO DE DADOS JUNIOR

### 1. 1° DESAFIO: USANDO SQL PARA RESPONDER PERGUNTAS


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

Para esse desafio que optei por utilizar o databricks, onde é minha ferramenta favorita e eu consigo utilizar python, pyspark e SQL no mesmo código. Mas nesse desafio o foco da consulta era SQL.

- IMPORT

Fiz a import dos 2 arquivos CSV para o databricks.

![image](https://github.com/GleisonR/Desafio/assets/116228613/2534ec86-879c-4528-bf3e-52b540f507f3)


Depois de realizar a importação, foi gerado um link para o arquivo de acesso, permitindo que eu o chame para o meu dataframe. Em Python, chamei o arquivo e utilizei a codificação UTF-8, que é a mais comum no Brasil. Na primeira linha, defini que se tratava de um cabeçalho com o código "header", e utilizei a vírgula como delimitador para separar as colunas 

![image](https://github.com/GleisonR/Desafio/assets/116228613/106fd14d-2a27-4458-90e3-c3aa8cfb4c97)

![image](https://github.com/GleisonR/Desafio/assets/116228613/a8174dba-0314-46ac-82c6-b25bc0bbd0e0)

Ainda em Python, criei uma TempView para utilizar SQL na tabela.

![image](https://github.com/GleisonR/Desafio/assets/116228613/ca2e0ad3-c539-4024-a335-bc836d58cb4a)

- VERIFICANDO INFORMAÇÕES

Agora, já conseguindo trabalhar com SQL, verifiquei as informações das tabelas, assim consigo visualizar os dados com os quais estou trabalhando.

![image](https://github.com/GleisonR/Desafio/assets/116228613/983b8523-8d59-4110-9502-b86d8ae1598f)
![image](https://github.com/GleisonR/Desafio/assets/116228613/0dd02b09-a840-4712-ae90-b9db8bb466ec)




### 1.1 Quanto de rake foi gerado por cada Geração de jogadores?

- Nessa primeira etapa, é preciso do valor máximo gerado em rake para cada geração.

Nessa etapa, eu preciso da coluna "rake" na tabela resultado e da coluna "data_nascimento" na tabela clientes. O valor de "rake" era uma string, então utilizei
a função **'SUM(CAST(rake AS INT))'** para somar os valores da coluna "rake" após convertê-los para o tipo de dados inteiro (INT). Para a data de nascimento, 
utilizei o "between" para selecionar os dados que estão dentro de um intervalo específico. E, para finalizar, utilizei o "inner join" para relacionar o "id" 
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

Verifiquei que só preciso usar a tabela resultado para essa consulta. Comecei utilizando a função **EXTRACT(MONTH FROM resultado)**, que serve para extrair o mês 
de uma data especificada. Em relação ao "rake", fiz a mesma coisa que na consulta anterior, utilizando a função **SUM(CAST(rake AS INT))** para somar os valores da 
coluna "rake" após convertê-los para o tipo de dados inteiro (INT). Finalizei com "GROUP BY" para agrupar os meses iguais e "ORDER BY" para ordenar em ordem 
crescente.

- ![image](https://github.com/GleisonR/Desafio/assets/116228613/44b15011-1d69-4ea9-9f18-c4902faa1910)


### 1.3 Qual sexo tem uma maior proporção de ganhadores

- Nessa terceira etapa é preciso veriricar qual sexo tem a quantidade maior quantidade de ganhadores.

Nessa etapa, percebi que iria utilizar novamente as duas tabelas e utilizei o mesmo INNER JOIN que empreguei na primeira etapa. Fiz a soma dos valores da coluna "winning" seguindo a mesma lógica da coluna "rake" das etapas anteriores. Utilizei a função **'WHERE c.sexo = 'm' AND r.winning > 0'** para filtrar os resultados de uma consulta com base em duas condições.

A primeira condição, c.sexo = 'm', indica que apenas os registros em que o valor da coluna "sexo" seja igual a 'm' (que geralmente representa o sexo masculino) serão incluídos no resultado da consulta.

Na segunda condição, r.winning > 0, indica que apenas os registros em que o valor da coluna "winning" seja maior que zero serão incluídos no resultado da consulta.

- Sexo masculino

![image](https://github.com/GleisonR/Desafio/assets/116228613/e5b1b538-da0d-4b09-9477-295dafb840df)

- Sexo Feminino

![image](https://github.com/GleisonR/Desafio/assets/116228613/44daee86-1d4a-4440-96f2-961fd1bf859d)

Analisando o valor retornado pelas duas consultas, verifica-se que o número de consultas do sexo masculino é maior, confirmando assim que o sexo masculino tem a maior quantidade de ganhadores.
















