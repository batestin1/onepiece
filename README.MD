<h1 align="center">
<img src="https://img.shields.io/static/v1?label=PYSPARK%20POR&message=MAYCON%20BATESTIN&color=7159c1&style=flat-square&logo=ghost"/>


<h3> <p align="center">BIGDATA ONE PIECE WORLD CENSUS </p> </h3>
<h3> <p align="center"> ================= </p> </h3>

>> <h3> Description </h3>
-------------------------

<p> ONE PIECE is an internationally successful Japanese manga. The story becomes inhabited in a fictional world, it tells the adventures of a young man whose body gained rubber properties after accidentally eating a devil fruit (AKUMA NO MI).
This project uses a data scraper from the website https://onepieceex.net/ storing its data, already processed, in an SQL database.
In this process, three modeling steps are carried out: 1) BRONZE call where we extract data from a NOSQL database for a first normalization treatment. 2) SILVER call, where we perform the data deduplication and 3) GOLD, where we write your code to upload it to the database.</p>


>> <h3> Start the Project </h3>
-------------------------

<p> To run the project, you need to install the dependencies located in the "dependencies" folder and in the root of the project, run the shell_script "run.sh". </p>


>> <h3> Sample of Payload in Stagin </h3>
-------------------------
> personagens
```
> db.personagens.find().pretty()
{
        "_id" : ObjectId("628ed981f515d293adad3e31"),
        "nome_completo" : "A.o. ",
        "kanji" : "A・O(アーオー) ",
        "apelido" : "none",
        "recompensa(beries)" : "none",
        "aniversario" : "none",
        "descricao" : "Um dos Aliados do Barba Branca. Quando soube da captura do Ace, participou da operação de resgate.",
        "url" : "https://onepieceex.net/wp-content/uploads/2020/06/A.o._full.jpg"
}
{
        "_id" : ObjectId("628ed981f515d293adad3e32"),
        "nome_completo" : "Absalom ",
        "kanji" : "アブサロム ",
        "apelido" : "Absalom do Cemitério ",
        "recompensa(beries)" : "none",
        "aniversario" : "30 de dezembro ",
        "descricao" : "Em breve mais informações",
        "url" : "https://onepieceex.net/wp-content/uploads/2020/06/Absalom_full.jpg"
}
{
        "_id" : ObjectId("628ed981f515d293adad3e33"),
        "nome_completo" : "Acrobatic Fuwas ",
        "kanji" : "軽業フワーズ ",
        "apelido" : "none",
        "recompensa(beries)" : "none",
        "aniversario" : "none",
        "descricao" : "Grupo de 4 piratas acrobatas no bando dos Piratas Buggy e foram fundados em 13 de Junho.",
        "url" : "https://onepieceex.net/wp-content/uploads/2020/06/acrobatic-fuwas_full.jpg"
}
{
        "_id" : ObjectId("628ed981f515d293adad3e34"),
        "nome_completo" : "Agyo ",
        "kanji" : "アギョウ ",
        "apelido" : "none",
        "recompensa(beries)" : "none",
        "aniversario" : "none",
        "descricao" : "Em breve mais informações",
        "url" : "https://onepieceex.net/wp-content/uploads/2020/06/agyou_full.jpg"
}
{
        "_id" : ObjectId("628ed981f515d293adad3e35"),
        "nome_completo" : "Aisa ",
        "kanji" : "アイサ ",
        "apelido" : "none",
        "recompensa(beries)" : "none",
        "aniversario" : "none",
        "descricao" : "Em breve mais informações",
        "url" : "https://onepieceex.net/wp-content/uploads/2020/06/Aisa_full.jpg"
}
{
        "_id" : ObjectId("628ed981f515d293adad3e36"),
        "nome_completo" : "Akumai ",
        "kanji" : "Aniversário:27 de dezembro ",
        "apelido" : "none",
        "recompensa(beries)" : "none",
        "aniversario" : "none",
        "descricao" : "Era um agente da Billions da Baroque Works seguia o Mr. 2 Bon Clay em todos os lugares antes de ser capturado por Hina quando seu ídolo e seu bando se sacrificaram para salvar os Piratas do Chapéu de Palha. Seu destino atual é desconhecido.",
        "url" : "https://onepieceex.net/wp-content/uploads/2020/06/akumai_full.jpg"
}
{
        "_id" : ObjectId("628ed981f515d293adad3e37"),
        "nome_completo" : "Aladdin ",
        "kanji" : "アラディン ",
        "apelido" : "none",
        "recompensa(beries)" : "none",
        "aniversario" : "none",
        "descricao" : "Em breve mais informações",
        "url" : "https://onepieceex.net/wp-content/uploads/2020/06/Aladdin_full.jpg"
}
{
        "_id" : ObjectId("628ed981f515d293adad3e38"),
        "nome_completo" : "Albion ",
        "kanji" : "アルビオン ",
        "apelido" : "O Retalhado ",
        "recompensa(beries)" : " 92,000,000 ",
        "aniversario" : "09 de novembro ",
        "descricao" : "Em breve mais informações",
        "url" : "https://onepieceex.net/wp-content/uploads/2020/06/Albion_full.jpg"
}
{
        "_id" : ObjectId("628ed981f515d293adad3e39"),
        "nome_completo" : "Alvida ",
        "kanji" : "アルビダ ",
        "apelido" : "Alvida da Clava de Ferro ",
        "recompensa(beries)" : " 5,000,000 ",
        "aniversario" : "14 de março ",
        "descricao" : "Em breve mais informações",
        "url" : "https://onepieceex.net/wp-content/uploads/2020/06/Alvida_full.jpg"
}
{
        "_id" : ObjectId("628ed981f515d293adad3e3a"),
        "nome_completo" : "Amadob ",
        "kanji" : "アマドブ ",
        "apelido" : "none",
        "recompensa(beries)" : "none",
        "aniversario" : "none",
        "descricao" : "Em breve mais informações",
        "url" : "https://onepieceex.net/wp-content/uploads/2020/06/Amadobu_full.jpg"
}
{
        "_id" : ObjectId("628ed981f515d293adad3e3b"),
        "nome_completo" : "Amazon ",
        "kanji" : "アマゾン ",
        "apelido" : "none",
        "recompensa(beries)" : "none",
        "aniversario" : "none",
        "descricao" : "Em breve mais informações",
        "url" : ""
}
{
        "_id" : ObjectId("628ed981f515d293adad3e3c"),
        "nome_completo" : "An Zengaiina ",
        "kanji" : "Aniversário:31 de janeiro ",
        "apelido" : "none",
        "recompensa(beries)" : "none",
        "aniversario" : "none",
        "descricao" : "Em breve mais informações",
        "url" : "https://onepieceex.net/wp-content/uploads/2020/06/an-zengaiina_full.jpg"
}
{
        "_id" : ObjectId("628ed981f515d293adad3e3d"),
        "nome_completo" : "Andre ",
        "kanji" : "アンドレ ",
        "apelido" : "none",
        "recompensa(beries)" : "none",
        "aniversario" : "none",
        "descricao" : "Em breve mais informações",
        "url" : "https://onepieceex.net/wp-content/uploads/2020/06/Andre_full.jpg"
}
{
        "_id" : ObjectId("628ed981f515d293adad3e3e"),
        "nome_completo" : "Anjo ",
        "kanji" : "アンジョウ ",
        "apelido" : "none",
        "recompensa(beries)" : "none",
        "aniversario" : "none",
        "descricao" : "Em breve mais informações",
        "url" : "https://onepieceex.net/wp-content/uploads/2020/06/Anjou_full.jpg"
}
{
        "_id" : ObjectId("628ed981f515d293adad3e3f"),
        "nome_completo" : "Ankoro ",
        "kanji" : "アンコロ ",
        "apelido" : "none",
        "recompensa(beries)" : "none",
        "aniversario" : "none",
        "descricao" : "Em breve mais informações",
        "url" : "https://onepieceex.net/wp-content/uploads/2020/06/Ankoro_full.jpg"
}
{
        "_id" : ObjectId("628ed981f515d293adad3e40"),
        "nome_completo" : "Antonio ",
        "kanji" : "アントニオ ",
        "apelido" : "none",
        "recompensa(beries)" : "none",
        "aniversario" : "none",
        "descricao" : "Em breve mais informações",
        "url" : "https://onepieceex.net/wp-content/uploads/2020/06/Antonio_full-1.jpg"
}
{
        "_id" : ObjectId("628ed981f515d293adad3e41"),
        "nome_completo" : "Aphelandra ",
        "kanji" : "アフェランドラ ",
        "apelido" : "none",
        "recompensa(beries)" : "none",
        "aniversario" : "none",
        "descricao" : "Em breve mais informações",
        "url" : "https://onepieceex.net/wp-content/uploads/2020/06/Aphelandra_full.jpg"
}
{
        "_id" : ObjectId("628ed981f515d293adad3e42"),
        "nome_completo" : "Arlong ",
        "kanji" : "アーロン ",
        "apelido" : "Serra ",
        "recompensa(beries)" : " 20,000,000 ",
        "aniversario" : "03 de maio ",
        "descricao" : "Em breve mais informações",
        "url" : "https://onepieceex.net/wp-content/uploads/2020/06/Arlong_full.jpg"
}
{
        "_id" : ObjectId("628ed981f515d293adad3e43"),
        "nome_completo" : "Atmos ",
        "kanji" : "アトモス ",
        "apelido" : "Búfalo D'Água ",
        "recompensa(beries)" : "none",
        "aniversario" : "19 de março ",
        "descricao" : "Em breve mais informações",
        "url" : "https://onepieceex.net/wp-content/uploads/2020/06/atmos_full.jpg"
}
{
        "_id" : ObjectId("628ed981f515d293adad3e44"),
        "nome_completo" : "Avalo Pizarro ",
        "kanji" : "アバロ・ピサロ ",
        "apelido" : "Rei Corrupto ",
        "recompensa(beries)" : "none",
        "aniversario" : "30 de setembro ",
        "descricao" : "É conhecido como “Rei Corrupto”. Embora estava preso em Impel Down no Nível 6, ele foi libertado pelo invasão do Barba Negra e se juntou ao bando como Capitão Colossal da 4º Capitânia dos Piratas do Barba Negra.",
        "url" : "https://onepieceex.net/wp-content/uploads/2020/06/Avalo-Pizarro_full.jpg"
}
```
