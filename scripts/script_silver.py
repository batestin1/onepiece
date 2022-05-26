

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


spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()


# extract
print("#"*100)

df = spark.read.parquet(f"./datalake/bronze/parquet/").createOrReplaceTempView("df")


######################2º STEP #################################################

df = spark.sql("""SELECT * FROM 
(SELECT id, data_captura, aniversario, apelido, descricao, kanji, nome_completo, recompensa,  url,
row_number() OVER (PARTITION BY id ORDER BY data_captura) 
as row_id FROM df WHERE TRIM(id) <> '')
WHERE row_id = 1  """)


print("#"*100)

df.write.mode("append").format("parquet").partitionBy("data_captura").save("./datalake/silver/parquet/")
