

                        #********************************************************************************#
                        #                                                                                #
                        #                                  нεℓℓσ,вαтεs!                                  #
                        #                                                                                #
                        #   filename: script_bronze                                                      #
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

spark = SparkSession.builder.appName("MyApp").config(config_spark_mongo, package_config_spark).master("local").getOrCreate()

# extract
print("#"*100)
print("Load from mongo")
df = spark.read.format("mongo").option("uri", uri).load()

print("#"*100)

#transform
df.createOrReplaceTempView('df')

########################## 1º Transformação ######################
df = spark.sql("""SELECT
monotonically_increasing_id() as id,
current_date() as data_captura,
SUBSTRING_INDEX(aniversario, 'de', 1) as dia,
SUBSTRING_INDEX(aniversario, 'de', -1) as mes,
CASE WHEN apelido LIKE 'Recompensa: %' THEN 'none' ELSE apelido END as apelido,
CASE WHEN kanji LIKE 'Aniversário:%' THEN 'none' ELSE kanji END as kanji,
descricao,
nome_completo,
TRIM(`recompensa(beries)`) as recompensa,
TRIM(url) as url FROM df""")

df.write.mode(mode).format(format).partitionBy("data_captura").save(save_bronze)
