

                        #********************************************************************************#
                        #                                                                                #
                        #                                  нεℓℓσ,вαтεs!                                  #
                        #                                                                                #
                        #   filename: script_gold                                                        #
                        #   created: 2022-05-22                                                          #
                        #   system: Windows                                                              #
                        #   version: 64bit                                                               #
                        #                                       by: Bates <https://github.com/batestin1> #
                        #********************************************************************************#
                        #                           import your librarys below                           #
                        #********************************************************************************#
import mysql.connector
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark import SparkContext
import findspark
findspark.add_packages('mysql:mysql-connector-java:8.0.11')

#connection
bank = mysql.connector.connect(
    host = "localhost",
    user= "root",
    password = "root"
)

cursor = bank.cursor()
cursor.execute('CREATE DATABASE IF NOT EXISTS onepiece')
my_conn = create_engine('mysql+mysqldb://root:root@localhost/onepiece?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC')


##############################################Extract############################################################################
spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

df = spark.read.parquet(f"./datalake/silver/parquet/").createOrReplaceTempView("df")


######################2º STEP #################################################

df = spark.sql("""SELECT REPLACE(uuid(), '-','') as id,
data_captura, aniversario, apelido, descricao, kanji, nome_completo, recompensa,  url FROM df""")


print("#"*100)

df.write.mode("append").format("parquet").partitionBy("data_captura").save("./datalake/gold/parquet/")
df.write.format('jdbc').options(url='jdbc:mysql://localhost/onepiece?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver='com.mysql.cj.jdbc.Driver',dbtable='personagens',user='root',password='root').mode('append').save()
