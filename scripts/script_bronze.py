

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


spark = SparkSession.builder.appName("MyApp").config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").master("local").getOrCreate()

# extract
print("#"*100)
print("Load from mongo")
df = spark.read.format("mongo").option("uri", "mongodb://localhost/onepiece.personagens").load()

print("#"*100)

#transform
df.createOrReplaceTempView('df')

########################## 1º Transformação ######################
df = spark.sql("""SELECT
monotonically_increasing_id() as id,
current_date() as data_captura,
aniversario,
apelido,
descricao,
kanji,
nome_completo,
TRIM(`recompensa(beries)`) as recompensa,
TRIM(url) as url FROM df""")

df.write.mode("append").format("parquet").partitionBy("data_captura").save("./datalake/bronze/parquet/")
