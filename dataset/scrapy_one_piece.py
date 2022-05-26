

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




url = "https://onepieceex.net/personagem/"
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.content, 'html.parser')
testes = soup.find_all("main", id="conteudo")
list_link = []
for i in soup.findAll('a',attrs={'href': re.compile("^https://onepieceex.net/personagem/")}): 
    link = i.get('href') #salva todos os links de download na variavel link
    list_link.append(link)
list_link.remove("https://onepieceex.net/personagem/")
list_link.remove("https://onepieceex.net/personagem/")

for i in list_link:
    with open(f'./json_files/{i[34:-1]}.json', "w", encoding="UTF-8") as output: 
        url = f"{i}"
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        imagem = soup.find_all("div", class_="personagem-imagem")
        descricao = soup.find_all("div", class_="personagem-info")
        componentes_base = soup.find_all("ul", class_="personagem-extra")
        desc = soup.find_all("p")
        imagem_final = imagem[0].get("style").replace("background: url('", "").replace("') no-repeat center","")
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

print("#"*100)
print("putting in the mongo")
print("#"*100)
client = pymongo.MongoClient('localhost', 27017)
db = client['onepiece']
Collection = db["personagens"]

for i in list_link:
    with open(f'./json_files/{i[34:-1]}.json') as file:
         db = json.load(file)
         Collection.insert_one(db)

for i in list_link:
  os.remove(f'./json_files/{i[34:-1]}.json')