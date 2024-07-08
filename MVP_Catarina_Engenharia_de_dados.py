# Databricks notebook source
# MAGIC %md
# MAGIC # MVP de Engenharia de Dados
# MAGIC
# MAGIC ## Catarina Carneiro da Silva

# COMMAND ----------

# MAGIC %md
# MAGIC Neste estudo queremos analisar a influência de algumas características de um funcionário no seu salário, com foco no setor terciário (comércio, serviços e turismo). O dataset utilizado neste projeto são os microdados da RAIS 2022 (Relação Anual de Informações Sociais), proveniente do Ministério do Trabalho. Essa base de informações é muito abrangente, podendo originar diversas inferências. No entanto, para ter resultados mais objetivos com o desejado, utilizaremos um subconjunto da base original, considerando apenas as variáveis mais relevantes.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Perguntas a serem respondidas:
# MAGIC
# MAGIC 1- A discriminação das mulheres pode ser comprovada por dados? Qual o salário médio das mulheres e dos homens?
# MAGIC
# MAGIC 2- A discriminação de raça pode ser comprovada por dados? Qual o salário médio de cada uma das raças analisadas?
# MAGIC
# MAGIC 3- Acredita-se que as pessoas mais novas costumam receber salários menores devido a menor experiencia, pode-se comprovar essa crença com dados?
# MAGIC
# MAGIC 4- Acredita-se que as pessoas com menor escolaridade costumam receber salários menores, pode-se comprovar essa crença com dados?
# MAGIC
# MAGIC 5- São Paulo (código 35) é considerado um dos estados mais avançados do país e, por isso, com maior nivel de salários. Pode-se comprovar essa crença com dados?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carga - Etapa Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM rais2022

# COMMAND ----------

# MAGIC %md
# MAGIC ###Catálogo de dados:
# MAGIC
# MAGIC CNAE - CNAE da atividade do emprego segundo a classificação 2.0 com a segmentação por classe. Neste momento encontram-se todas as CNAES registradas na RAIS, no entanto para analisarmos somente o setor terciário, como pretendido, teremos que filtrar apenas as classes entre 45111 e 69206.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT cnae
# MAGIC FROM rais2022
# MAGIC ORDER BY cnae DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Faixa_etaria - Faixa de idade do funcionário:
# MAGIC
# MAGIC 1-entre 10 a 14 anos/
# MAGIC
# MAGIC 2-entre 15 a 17 anos/
# MAGIC
# MAGIC 3-entre 18 a 24 anos/
# MAGIC
# MAGIC 4-entre 25 a 29 anos/
# MAGIC
# MAGIC 5-entre 30 a 39 anos/
# MAGIC
# MAGIC 6-entre 40 a 49 anos/
# MAGIC
# MAGIC 7-entre 50 a 64 anos/
# MAGIC
# MAGIC 8-65 anos ou mais.
# MAGIC
# MAGIC Na base de dados também encontramos a categoria 99, que são os dados informados como ignorados. Na próxima etapa precisaremos filtrar essas informações.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT faixa_etaria
# MAGIC FROM rais2022
# MAGIC ORDER BY faixa_etaria DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Escolaridade - Escolaridade do funcionário:
# MAGIC
# MAGIC 1-Analfabeto
# MAGIC
# MAGIC 2-Até 5ª Incompleto
# MAGIC
# MAGIC 3-5ª Completo Fundamental
# MAGIC
# MAGIC 4-6ª a 9ª Fundamental
# MAGIC
# MAGIC 5-Fundamental Completo
# MAGIC
# MAGIC 6-Médio Incompleto
# MAGIC
# MAGIC 7-Médio Completo
# MAGIC
# MAGIC 8-Superior Incompleto
# MAGIC
# MAGIC 9-Superior Completo
# MAGIC
# MAGIC 10-Mestrado
# MAGIC
# MAGIC 11-Doutorado
# MAGIC
# MAGIC Na base de dados também encontramos a categoria 99, que são os dados informados como ignorados. Na próxima etapa precisaremos filtrar essas informações.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT escolaridade
# MAGIC FROM rais2022
# MAGIC ORDER BY escolaridade DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Idade - Idade do funcionário

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT idade
# MAGIC FROM rais2022
# MAGIC ORDER BY idade DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Cor - Raça/cor segundo funcionário:
# MAGIC
# MAGIC 1-Indígena
# MAGIC
# MAGIC 2- Branca
# MAGIC
# MAGIC 4- Preta
# MAGIC
# MAGIC 6- Amarela
# MAGIC
# MAGIC 8- Parda
# MAGIC
# MAGIC 9- Não informado
# MAGIC
# MAGIC Na base de dados também encontramos a categoria 99, que são os dados informados como ignorados. Na próxima etapa precisaremos filtrar essas informações assim como a categoria 9 (não informado).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT cor
# MAGIC FROM rais2022
# MAGIC ORDER BY cor DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Sexo - Gênero do funcionário:
# MAGIC
# MAGIC 1- Masculino
# MAGIC
# MAGIC 2- Feminino
# MAGIC
# MAGIC 9- Ignorado
# MAGIC
# MAGIC Na base de dados também encontramos a categoria 99, que são os dados informados como ignorados. Na próxima etapa precisaremos filtrar essas informações.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT sexo
# MAGIC FROM rais2022
# MAGIC ORDER BY sexo DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC UF - 26 estados brasileiros e o Distrito Federal:
# MAGIC 11 - Rondônia /	12 - Acre /	13 - Amazonas /	14 - Roraima /	15 - Para /	16 - Amapa /	17 - Tocantins /	21 - Maranhão /	22 - Piaui /	23 - Ceará /	24 - Rio Grande do Norte /	25 - Paraíba /	26 - Pernambuco /	27 - Alagoas /	28 - Sergipe /	29 - Bahia /	31 - Minas Gerais /	32 - Espírito Santo	/ 33 - Rio de Janeiro /	35 - São Paulo /	41 - Paraná /	42 - Santa Catarina /	43 - Rio Grande do Sul /	50 - Mato Grosso do Sul /	51 - Mato Grosso /	52 - Goiás /	53 - Distrito Federal
# MAGIC
# MAGIC Na base de dados também encontramos a categoria 99, que são os dados informados como ignorados. Na próxima etapa precisaremos filtrar essas informações.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT uf
# MAGIC FROM rais2022
# MAGIC ORDER BY uf DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC remuneração - Remuneração média dos funcionários

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT remuneracao
# MAGIC FROM rais2022
# MAGIC ORDER BY remuneracao DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Transformação - Etapa Silver
# MAGIC Para facilitar a consulta e limpar a base de dados faremos as transformações necessárias.
# MAGIC Primeiro deletaremos a coluna idade, já que na coluna faixa_etaria temos os valores agrupados, o que facilita a análise.
# MAGIC
# MAGIC Para trabalhar apenas o setor terciário, como pretendido, filtraremos a coluna cnae para conter apenas as CNAES desse setor (entre as classes 45111 e 69206).
# MAGIC
# MAGIC Para limpar o dataset, excluiremos os seguintes dados:
# MAGIC o gênero que não corresponde ao sexo Feminino ou Masculino, a cor com informações diferentes das tradicionais e as uf ignoradas.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE rais2022_silver AS
# MAGIC SELECT cnae, escolaridade, faixa_etaria, cor, sexo, remuneracao, uf
# MAGIC FROM rais2022
# MAGIC WHERE cnae >= 45111 AND cnae <= 69206
# MAGIC   AND sexo!=9
# MAGIC   AND faixa_etaria !=99
# MAGIC   AND escolaridade !=99
# MAGIC   AND cor!=9
# MAGIC   AND cor!=99
# MAGIC   AND uf!=99;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT cnae
# MAGIC FROM rais2022_silver
# MAGIC ORDER BY cnae DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT sexo
# MAGIC FROM rais2022_silver
# MAGIC ORDER BY sexo DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT faixa_etaria
# MAGIC FROM rais2022_silver
# MAGIC ORDER BY faixa_etaria DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT escolaridade
# MAGIC FROM rais2022_silver
# MAGIC ORDER BY escolaridade DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT cor
# MAGIC FROM rais2022_silver
# MAGIC ORDER BY cor DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT uf
# MAGIC FROM rais2022_silver
# MAGIC ORDER BY uf DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC #####1- A discriminação das mulheres pode ser comprovada por dados? Qual o salário médio das mulheres e dos homens?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sexo, AVG(remuneracao) AS remuneracao_sexo
# MAGIC FROM rais2022_silver
# MAGIC GROUP BY sexo
# MAGIC ORDER BY remuneracao_sexo DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sexo, COUNT(*) AS genero
# MAGIC FROM rais2022_silver
# MAGIC GROUP BY sexo
# MAGIC ORDER BY genero DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC O salário médio das mulheres do setor terciário foi de R$ 3.483, o que representa 80% da remuneração média dos homens no setor e confirma o menor nível salarial. Deve-se considerar elas representam menos da metade (43,1%) dos trabalhadores do Brasil e precisam de uma maior entrada no mercado de trabalho.

# COMMAND ----------

# MAGIC %md
# MAGIC #####2- A discriminação de raça pode ser comprovada por dados? Qual o salário médio de cada uma das raças analisadas?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cor, AVG(remuneracao) AS remuneracao_cor
# MAGIC FROM rais2022_silver
# MAGIC GROUP BY cor
# MAGIC ORDER BY remuneracao_cor DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cor, COUNT(*) AS raca
# MAGIC FROM rais2022_silver
# MAGIC GROUP BY cor
# MAGIC ORDER BY raca DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC A maior média salarial corresponde a quem se considera "6- amarelo" (R$ 6.769), seguida pela população "2- branca" (R$ 4.776) e os dois últimos as pessoas denominadas "4- pretas" (R$ 3.036) e "8- pardas" (R$ 3.020).
# MAGIC
# MAGIC Apesar da população branca não ter as maiores remuneração, eles representam mais da metade (52,2%) dos trabalhadores brasileiros, confirmando a preferência por esses funcionários. 

# COMMAND ----------

# MAGIC %md
# MAGIC #####3- Acredita-se que as pessoas mais novas costumam receber salários menores devido a menor experiencia, pode-se comprovar essa crença com dados?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT faixa_etaria, AVG(remuneracao) AS remuneracao_idade
# MAGIC FROM rais2022_silver
# MAGIC GROUP BY faixa_etaria
# MAGIC ORDER BY remuneracao_idade DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT faixa_etaria, COUNT(*) AS idade
# MAGIC FROM rais2022_silver
# MAGIC GROUP BY faixa_etaria
# MAGIC ORDER BY idade DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Os colaboradores abaixo de 24 anos são os que recebem menores salários, confirmando a hipótese inicial, enquanto aqueles entre 40 e 49 anos recebemos em média os maiores valores.
# MAGIC Pode-se observar um preferência do mercado de trabalho por contratar pessoas entre 30 e 49 anos, somando esses dois grupos encontra-se 50,3% dos funcionários brasileiros.

# COMMAND ----------

# MAGIC %md
# MAGIC #####4- Acredita-se que as pessoas com menor escolaridade costumam receber salários menores, pode-se comprovar essa crença com dados?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT escolaridade, AVG(remuneracao) AS remuneracao_escolaridade
# MAGIC FROM rais2022_silver
# MAGIC GROUP BY escolaridade
# MAGIC ORDER BY remuneracao_escolaridade DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT escolaridade, COUNT(*) AS formacao
# MAGIC FROM rais2022_silver
# MAGIC GROUP BY escolaridade
# MAGIC ORDER BY formacao DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Os dados comprovam que há uma grande diferença salarial dependendo do grau de estudo, com as pessoas com escolaridade até o 5º ano incompleto recebendo os menores salários (R$ 1.998). É interessante observar que o nível de renda de quem possui mestrado (26.115) é acima de quem tem doutorado (R$ 18.217), provalvemente por terem menos opções de carreira especificamente voltada para os doutorando.
# MAGIC
# MAGIC Em relação a quantidade de pessoas empregadas, a maior parte possui o ensino médio completo (59,3%), seguida pela população com o nível superior completo (15,4%).

# COMMAND ----------

# MAGIC %md
# MAGIC #####5- São Paulo (código 35) é considerado um dos estados mais avançados do país e, por isso, com maior nivel de salários. Pode-se comprovar essa crença com dados?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT uf, AVG(remuneracao) AS remuneracao_uf
# MAGIC FROM rais2022_silver
# MAGIC GROUP BY uf
# MAGIC ORDER BY remuneracao_uf DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT uf, COUNT(*) AS estado
# MAGIC FROM rais2022_silver
# MAGIC GROUP BY uf
# MAGIC ORDER BY estado DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Os dados mostram que a localidade com maior nível salarial é o Distrito Federal (R$ 7.152), compatível por ser uma área política, seguida por São Paulo (R$ 4.816). Apesar de refutar ter o maior rendimento, São Paulo compreende a maior parte dos trabalhadores brasileiros (32,5%).

# COMMAND ----------

# MAGIC %md
# MAGIC ###Autoavaliação

# COMMAND ----------

# MAGIC %md
# MAGIC Com esses estudo conseguimos observar as principais características do mercado de trabalho brasileiro, sendo possível responder as perguntas propostas. Para isso não precisamos chegar até o nível Gold, com a tabela Silver sendo o suficiente para atingir nosso objetivo.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     (SELECT CORR(sexo, remuneracao) FROM rais2022_silver) AS correlation_sexo,
# MAGIC     (SELECT CORR(cor, remuneracao) FROM rais2022_silver) AS correlation_cor,
# MAGIC     (SELECT CORR(faixa_etaria, remuneracao) FROM rais2022_silver) AS correlation_idade,
# MAGIC     (SELECT CORR(escolaridade, remuneracao) FROM rais2022_silver) AS correlation_escolaridade,
# MAGIC     (SELECT CORR(uf, remuneracao) FROM rais2022_silver) AS correlation_uf;

# COMMAND ----------

# MAGIC %md
# MAGIC Pela tabela de correlação entre os dados podemos perceber uma correlação negativa entre gênero e salário, mostrando que o fato de ser mulher (indicador 2) afeta negativamente a remuneração. Em relação a raça, o indicador também ficou abaixo de 0, confirmando que os indicadores maiores (parda, amarela, preta) tem influência negativa. No entanto, ambas as correlação são próximas a 0, representando que o impacto não é tão significativo.
# MAGIC
# MAGIC Tanto a faixa etária, quando o estado de origem e a escolaridade apresentaram correlações positivas, sendo a da escolaridade a mais significativa. Por isso, podemos inferir que a escolaridade é a caraterística que apresenta maior impacto no nível salarial.
# MAGIC
# MAGIC Uma evolução desse estudo seria analisar as mesmas informações na segmentação de estado, para observar como São Paulo, por exemplo, se comporta em relação a contratação de mulheres, negros, pessoas mais velhas, com menor escolaridade ou quaisquer outras considerações relevantes.
