%python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# Subindo tabelas de dados.

%python
df_desafio 1 = spark.read.format("csv").option("header","true").option("sep",",").option("encoding","UTF-8").load("/FileStore/tables/clientes.csv")
display(desafio 1)

# Criando tabela temporária para comandos em SQL

% python
df_clientes . createOrReplaceTempView ( "clientes" )

% python
df_resultado . createOrReplaceTempView ( "resultado" )

%sql
SELECT * FROM resultado

# Quanto de rake foi gerado por cada Geração* de jogadores?

# Geração Baby Boomers

SELECT SUM(CAST(rake AS INT)) AS Rake_Baby_Boomers
FROM clientes c
INNER JOIN resultado r
ON c.id = r.clientes_id
WHERE c.data_nascimento BETWEEN '1941-01-01' AND '1959-12-31'

# Geração X

SELECT SUM(CAST(rake AS INT)) AS Rake_GeracaoX
FROM clientes c
INNER JOIN resultado r
ON c.id = r.clientes_id
WHERE c.data_nascimento BETWEEN '1960-01-01' AND '1979-12-31'

# Geração Y

SELECT SUM(CAST(rake AS INT)) AS Rake_GeracaoY
FROM clientes c
INNER JOIN resultado r
ON c.id = r.clientes_id
WHERE c.data_nascimento BETWEEN '1980-01-01' AND '1995-12-31'

# Geração Z

SELECT SUM(CAST(rake AS INT)) AS Rake_GeracaoZ
FROM clientes c
INNER JOIN resultado r
ON c.id = r.clientes_id
WHERE c.data_nascimento BETWEEN '1996-01-01' AND '2010-12-31'

# Geração Alpha

SELECT data_nascimento, SUM(CAST(rake AS INT)) AS Rake_GeracaoAlpha
FROM clientes c
INNER JOIN resultado r
ON c.id = r.clientes_id
WHERE c.data_nascimento >= '2010-01-01'
GROUP BY c.data_nascimento, r.rake

# Os dados começam em 1942 e terminam em 2008. Com isso não tem dados Veteranos, geração formada por pessoas que nasceram entre 1925 e 1940 e Geração Alpha, engloba todos os nascidos a partir de 2010 até a presente data.

SELECT data_nascimento
FROM clientes
GROUP BY data_nascimento
ORDER BY data_nascimento DESC

#Qual foi o rake gerado por mês?

#2020-06-04 até 2023-04-18

SELECT data_acesso
FROM resultado
GROUP BY data_acesso
ORDER BY data_acesso DESC


SELECT EXTRACT(MONTH FROM data_acesso) AS Mes, SUM(CAST(rake AS INT)) AS Rake
FROM resultado
GROUP BY EXTRACT(MONTH FROM data_acesso)
ORDER BY MES;


#Qual sexo tem uma maior proporção de ganhadores**?
 
SELECT sexo as SEXO, SUM(CAST(winning AS INT)) AS GANHADORES
FROM clientes c
INNER JOIN resultado r
ON c.id = r.clientes_id
WHERE c.sexo = 'm' 
AND r.winning > 0
GROUP BY c.sexo


SELECT sexo as SEXO, SUM(CAST(winning AS INT)) AS GANHADORES
FROM clientes c
INNER JOIN resultado r
ON c.id = r.clientes_id
WHERE c.sexo = 'f' 
AND r.winning > 0
GROUP BY c.sexo

# Analizando os dados, o sexo com maior proporção de ganhadores é o Masculino