

                        #********************************************************************************#
                        #                                                                                #
                        #                                  нεℓℓσ,вαтεs!                                  #
                        #                                                                                #
                        #   filename: scrapy_one_piece.py                                                #
                        #   created: 2022-05-22                                                          #
                        #   system: Windows                                                              #
                        #   version: 64bit                                                               #
                        #                                       by: Bates <https://github.com/batestin1> #
                        #********************************************************************************#
                        #                           import your librarys below                           #
                        #********************************************************************************#


import os
import re
import requests
from bs4 import BeautifulSoup
import pymongo
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import max as max_


path_parametros="/home/bates/repositorio/big_data/one_piece/parameters/scrapy.json"

spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

df_parar=spark.read.json(path_parametros)

df_parar.createOrReplaceTempView("df_parar")


url = df_parar.agg(max_("url")).collect()[0][0]
user_agent = df_parar.agg(max_("User-Agent")).collect()[0][0]
mozilla = df_parar.agg(max_("Mozilla")).collect()[0][0]
compil = df_parar.agg(max_("compil")).collect()[0][0]
dir_jsonfile = df_parar.agg(max_("dir_jsonfile")).collect()[0][0]
extension_json = df_parar.agg(max_("extension_json")).collect()[0][0]
local_host = df_parar.agg(max_("local_host")).collect()[0][0]
mongo_port = df_parar.agg(max_("mongo_port")).collect()[0][0]
db_mongo = df_parar.agg(max_("db_mongo")).collect()[0][0]
db_collection = df_parar.agg(max_("db_collection")).collect()[0][0]

headers = {f'{user_agent}': f'{mozilla}'}
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.content, 'html.parser')
list_link = []
for i in soup.findAll('a',attrs={'href': re.compile(f"^{compil}")}): 
    link = i.get('href') #salva todos os links de download na variavel link
    list_link.append(link)
list_link.remove(f"{compil}")
list_link.remove(f"{compil}")

for i in list_link:
    with open(f'{dir_jsonfile}{i[34:-1]}{extension_json}', "w", encoding="UTF-8") as output: 
        url = f"{i}"
        headers = {f'{user_agent}': f'{mozilla}'}
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        imagem = soup.find_all("div", class_="personagem-imagem")
        descricao = soup.find_all("div", class_="personagem-info")
        componentes_base = soup.find_all("ul", class_="personagem-extra")
        desc = soup.find_all("p")
        imagem_final = imagem[0].get("style").replace("background: url('", "").replace("') no-repeat center","") if imagem[0].get("style").replace("background: url('", "").replace("') no-repeat center","") in imagem[0].get("style").replace("background: url('", "").replace("') no-repeat center","") else "none"
        descricao_final = descricao[0].find_all("p")[0].get_text()

        for i in range(len(componentes_base[0].find_all("li"))):
            if i <= 2:
                nome = componentes_base[0].find_all("li")[0].get_text().replace("Nome:","\a").replace("\a","").replace("\n","")
                kanji = componentes_base[0].find_all("li")[1].get_text().replace("Kanji:","\a").replace("\a","").replace("\n","")
                apelido = componentes_base[0].find_all("li")[4].get_text().replace("Alcunha:","\a").replace("\a","").replace("\n","") if "apelido" in componentes_base[0].find_all("li") else "none"
                recompensa = componentes_base[0].find_all("li")[3].get_text().replace("Recompensa:","\a").replace("\a","").replace("\n","") if "recompensa" in componentes_base[0].find_all("li") else "none"
                aniversario = componentes_base[0].find_all("li")[2].get_text().replace("Aniversário:","\a").replace("\a","").replace("\n","") if "aniversario" in componentes_base[0].find_all("li") else "none"
            elif i <=3:
                nome = componentes_base[0].find_all("li")[0].get_text().replace("Nome:","\a").replace("\a","").replace("\n","")
                kanji = componentes_base[0].find_all("li")[1].get_text().replace("Kanji:","\a").replace("\a","").replace("\n","")
                apelido = componentes_base[0].find_all("li")[2].get_text().replace("Alcunha:","\a").replace("\a","").replace("\n","")
                recompensa = componentes_base[0].find_all("li")[4].get_text().replace("Recompensa:","\a").replace("\a","").replace("\n","") if "recompensa" in componentes_base[0].find_all("li") else "none"
                aniversario = componentes_base[0].find_all("li")[3].get_text().replace("Aniversário:","\a").replace("\a","").replace("\n","")
            elif i <=4:
                nome = componentes_base[0].find_all("li")[0].get_text().replace("Nome:","\a").replace("\a","").replace("\n","")
                kanji = componentes_base[0].find_all("li")[1].get_text().replace("Kanji:","\a").replace("\a","").replace("\n","")
                apelido = componentes_base[0].find_all("li")[2].get_text().replace("Alcunha:","\a").replace("\a","").replace("\n","")
                recompensa = componentes_base[0].find_all("li")[3].get_text().replace("Recompensa:","\a").replace("\a","").replace("\n","")
                aniversario = componentes_base[0].find_all("li")[4].get_text().replace("Aniversário:","\a").replace("\a","").replace("\n","")
            else:
                nome = componentes_base[0].find_all("li")[0].get_text().replace("Nome:","\a").replace("\a","").replace("\n","")
                kanji = componentes_base[0].find_all("li")[2].get_text().replace("Kanji:","\a").replace("\a","").replace("\n","") if "kanji" in componentes_base[0].find_all("li") else "none"
                apelido = componentes_base[0].find_all("li")[3].get_text().replace("Alcunha:","\a").replace("\a","").replace("\n","") if "apelido" in componentes_base[0].find_all("li") else "none"
                recompensa = componentes_base[0].find_all("li")[4].get_text().replace("Recompensa:","\a").replace("\a","").replace("\n","") if "recompensa" in componentes_base[0].find_all("li") else "none"
                aniversario = componentes_base[0].find_all("li")[1].get_text().replace("Aniversário:","\a").replace("\a","").replace("\n","") if "aniversario" in componentes_base[0].find_all("li") else "none"




            df = {
                "nome_completo": nome,
                "kanji": kanji,
                "apelido": apelido,
                "recompensa(beries)": recompensa,
                "aniversario": aniversario,
                "descricao": descricao_final.replace(" =)",""),
                "url": imagem_final
            }
        json.dump(df, output, allow_nan=True, indent=True, separators=(',',':'))
        print(f"Aquivo de {nome} criado com sucesso!")

print("#"*100)
print("putting in the mongo")
print("#"*100)
client = pymongo.MongoClient(f'{local_host}', mongo_port)
db = client[f'{db_mongo}']
Collection = db[f"{db_collection}"]

for i in list_link:
    with open(f'{dir_jsonfile}{i[34:-1]}{extension_json}') as file:
         db = json.load(file)
         Collection.insert_one(db)

for i in list_link:
  os.remove(f'{dir_jsonfile}{i[34:-1]}{extension_json}')
  print("arquivos removidos corretamente!")