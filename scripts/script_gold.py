

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
from pyspark.sql.functions import max as max_

path_parametros="/home/bates/repositorio/big_data/one_piece/parameters/data.json"

spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

df_parar=spark.read.json(path_parametros)
project = df_parar.agg(max_("project")).collect()[0][0]
config_spark_mongo = df_parar.agg(max_("config_spark_mongo")).collect()[0][0]
package_config_spark = df_parar.agg(max_("package_config_spark")).collect()[0][0]
uri = df_parar.agg(max_("uri")).collect()[0][0]
mode = df_parar.agg(max_("mode")).collect()[0][0]
format = df_parar.agg(max_("format")).collect()[0][0]
format_2 = df_parar.agg(max_("format_2")).collect()[0][0]
save_bronze = df_parar.agg(max_("save_bronze")).collect()[0][0]
save_silver = df_parar.agg(max_("save_silver")).collect()[0][0]
save_gold = df_parar.agg(max_("save_gold")).collect()[0][0]
cursor_execute = df_parar.agg(max_("cursor_execute")).collect()[0][0]
findspark_sql = df_parar.agg(max_("findspark_sql")).collect()[0][0]
host = df_parar.agg(max_("host")).collect()[0][0]
user_ = df_parar.agg(max_("user")).collect()[0][0]
passw = df_parar.agg(max_("pass")).collect()[0][0]
driver_sql = df_parar.agg(max_("driver_sql")).collect()[0][0]

#connection
bank = mysql.connector.connect(
    host = host,
    user= user_,
    password = passw
)

cursor = bank.cursor()
cursor.execute(cursor_execute)
my_conn = create_engine(f'mysql+mysqldb://{user_}:{user_}@{host}/{project}?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC')


##############################################Extract############################################################################
spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()

df = spark.read.parquet(save_silver).createOrReplaceTempView("df")


######################2º STEP #################################################

df = spark.sql("""SELECT * FROM df""")



aniversario = spark.sql("""SELECT total_aniversariantes_mes, TOTAL FROM(
SELECT 1 as index, 'JANEIRO' total_aniversariantes_mes, COUNT(mes_aniversario) as TOTAL FROM df where mes_aniversario = 'janeiro' UNION
SELECT 2 as index, 'FEVEREIRO' total_aniversariantes_mes, COUNT(mes_aniversario) as TOTAL FROM df where mes_aniversario = 'fevereiro' UNION
SELECT 3 as index, 'MARÇO' total_aniversariantes_mes, COUNT(mes_aniversario) as TOTAL FROM df where mes_aniversario = 'março' UNION
SELECT 4 as index, 'ABRIL' total_aniversariantes_mes, COUNT(mes_aniversario) as TOTAL FROM df where mes_aniversario = 'abril' UNION
SELECT 5 as index, 'MAIO' total_aniversariantes_mes, COUNT(mes_aniversario) as TOTAL FROM df where mes_aniversario = 'maio' UNION
SELECT 6 as index, 'JUNHO' total_aniversariantes_mes, COUNT(mes_aniversario) as TOTAL FROM df where mes_aniversario = 'junho' UNION
SELECT 7 as index, 'JULHO' total_aniversariantes_mes, COUNT(mes_aniversario) as TOTAL FROM df where mes_aniversario = 'julho' UNION
SELECT 8 as index, 'AGOSTO' total_aniversariantes_mes, COUNT(mes_aniversario) as TOTAL FROM df where mes_aniversario = 'agosto' UNION
SELECT 9 as index, 'SETEMBRO' total_aniversariantes_mes, COUNT(mes_aniversario) as TOTAL FROM df where mes_aniversario = 'setembro' UNION
SELECT 10 as index, 'OUTUBRO' total_aniversariantes_mes, COUNT(mes_aniversario) as TOTAL FROM df where mes_aniversario = 'outubro' UNION
SELECT 11 as index, 'NOVEMBRO' total_aniversariantes_mes, COUNT(mes_aniversario) as TOTAL FROM df where mes_aniversario = 'novembro' UNION
SELECT 12 as index, 'DEZEMBRO' total_aniversariantes_mes, COUNT(mes_aniversario) as TOTAL FROM df where mes_aniversario = 'dezembro' UNION
SELECT 13 as index, 'NÃO ENCONTRADOS' total_aniversariantes_mes, COUNT(mes_aniversario) as TOTAL FROM df where mes_aniversario = 'none'
)

""")

recompensas = spark.sql("""SELECT valores, TOTAL FROM(
SELECT 1 as index, 'ACIMA DE 1.000 BERIES' valores, COUNT(recompensa) as TOTAL FROM df where recompensa <= '1.000' UNION
SELECT 2 as index, 'ACIMA DE 10.000 BERIES' valores, COUNT(recompensa) as TOTAL FROM df where recompensa > '10.000' AND recompensa < '100.000' UNION
SELECT 3 as index, 'ACIMA DE 100.000 BERIES' valores, COUNT(recompensa) as TOTAL FROM df where recompensa > '100.000' AND recompensa < '500.000' UNION
SELECT 4 as index, 'ACIMA DE 500.000 BERIES' valores, COUNT(recompensa) as TOTAL FROM df where recompensa > '500.000' AND recompensa < '1.000.000' UNION
SELECT 5 as index, 'ACIMA DE 1.000.000 BERIES' valores, COUNT(recompensa) as TOTAL FROM df where recompensa > '1.000.000' AND recompensa < '10.000.000' UNION
SELECT 6 as index, 'SEM RECOMPENSA' valores, COUNT(recompensa) as TOTAL FROM df where recompensa = 'none'
)

""")



df.write.mode(mode).format(format).partitionBy("data_captura").save(save_gold)
aniversario.write.format(format_2).options(url=f'{format_2}:mysql://{host}/{project}?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver=driver_sql ,dbtable='db_metric_aniversarios',user=user_,password=passw).mode(mode).save()
recompensas.write.format(format_2).options(url=f'{format_2}:mysql://{host}/{project}?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver=driver_sql ,dbtable='db_metric_recompensa',user=user_,password=passw).mode(mode).save()
df.write.format(format_2).options(url=f'{format_2}:mysql://{host}/{project}?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver=driver_sql ,dbtable='db_full',user=user_,password=passw).mode(mode).save()
