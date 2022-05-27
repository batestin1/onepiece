

                        #********************************************************************************#
                        #                                                                                #
                        #                                  нεℓℓσ,вαтεs!                                  #
                        #                                                                                #
                        #   filename: script_silver                                                      #
                        #   created: 2022-05-22                                                          #
                        #   system: Windows                                                              #
                        #   version: 64bit                                                               #
                        #                                       by: Bates <https://github.com/batestin1> #
                        #********************************************************************************#
                        #                           import your librarys below                           #
                        #********************************************************************************#
from pyspark.sql import SparkSession
from pyspark.sql.functions import max as max_


path_parametros="/home/bates/repositorio/big_data/one_piece/parameters/data.json"

spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

df_parar=spark.read.json(path_parametros)
config_spark_mongo = df_parar.agg(max_("config_spark_mongo")).collect()[0][0]
package_config_spark = df_parar.agg(max_("package_config_spark")).collect()[0][0]
uri = df_parar.agg(max_("uri")).collect()[0][0]
mode = df_parar.agg(max_("mode")).collect()[0][0]
format = df_parar.agg(max_("format")).collect()[0][0]
save_bronze = df_parar.agg(max_("save_bronze")).collect()[0][0]
save_silver = df_parar.agg(max_("save_silver")).collect()[0][0]
save_gold = df_parar.agg(max_("save_gold")).collect()[0][0]
cursor_execute = df_parar.agg(max_("cursor_execute")).collect()[0][0]
findspark_sql = df_parar.agg(max_("findspark_sql")).collect()[0][0]
host = df_parar.agg(max_("host")).collect()[0][0]
user = df_parar.agg(max_("user")).collect()[0][0]
passw = df_parar.agg(max_("pass")).collect()[0][0]
driver_sql = df_parar.agg(max_("driver_sql")).collect()[0][0]


# extract
print("#"*100)

df = spark.read.parquet(save_bronze).createOrReplaceTempView("df")


######################2º STEP #################################################

df = spark.sql("""SELECT * FROM 
(SELECT id, dia, data_captura, apelido, descricao, 
REPLACE(kanji, "Alcunha:Komurasaki", "none") as kanji,
nome_completo,
REPLACE(mes, 'zembro', 'dezembro') as mes,
REPLACE(recompensa, ',', '.') as recompensa,  url,
row_number() OVER (PARTITION BY id ORDER BY data_captura) 
as row_id FROM df WHERE TRIM(id) <> '')
WHERE row_id = 1  """).createOrReplaceTempView('df_temp')

df_final = spark.sql("""SELECT REPLACE(uuid(), '-','') as id,
data_captura, TRIM(dia) as dia_aniversario, TRIM(mes) as mes_aniversario, apelido, descricao, kanji, nome_completo, recompensa,  url FROM df_temp""")



print("#"*100)

df_final.write.mode(mode).format(format).partitionBy("data_captura").save(save_silver)
