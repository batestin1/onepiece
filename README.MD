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
> address
```
{
 "_id":"2W1159879A",
 "region_birth":"East Blue",
 "country_birth":"Warship Island",
 "city_birth":"North Wayne",
 "current_region":"East Blue",
 "current_country":"Warship Island",
 "current_city":"East Joshua",
 "street":"Christine Fields",
 "number":"4104",
 "postalcode":"04650",
 "mailer":"mary.east4104@onepiece.com.br",
 "register_data":"20210423"
}
```

> fruit
```
{
 "_id":"3Y6898825C",
 "type_of_fruit":"it does not have",
 "fruit_name":"it does not have",
 "fruit_category":"it does not have",
 "number_times_resurrected":"0",
 "register_data":"20210622"
}
```


> job
```
{
 "_id":"2W1159879A",
 "job":"Freight forwarder",
 "current_job":"YES",
 "contracting_company":"Robinson, Simon and Hernandez",
 "start_date":"1981/11/02",
 "year_working_time":40,
 "initial_salary":4904.0,
 "current_wage":5345.36,
 "register_data":"20210423"
}
```

> persona
```
{
 "_id":"7P1521176A",
 "first_name":"Kristin",
 "last_name":"Smith",
 "gender":"F",
 "race":"Minks",
 "birthday":"1967-03-26",
 "age":"54",
 "devil_fruit_user":"it does not have",
 "has_job":"has",
 "has_tatoo":"it does not have",
 "has_scar":"has",
 "has_disability":"no deficiency",
 "security_social_number":"575-40-5565",
 "phone":"001-985-833-8626x33224",
 "has_rewards":"has",
 "sketch":"https://www.lorempixel.com/350/215",
 "register_data":"20210816"
}
```

> physical_characteristics
```
{
 "_id":"1S6151128X",
 "color_hair":"SeaShell",
 "color_skill":"BLUISH",
 "type_of_tatoo":"it does not have",
 "where_in_body":"it does not have",
 "color_of_tatoo":"it does not have",
 "scar":"Left arm",
 "color_eyes":"SeaShell",
 "register_data":"20210828"
}
```

> rewards
```
{
 "_id":"2W1159879A",
 "ssn_people":"165-53-1723",
 "main_crime":"female violence",
 "code_crime":13,
 "tax_collected_government":37824.56,
 "debt_with_government":31503.56,
 "rewards":961679.94,
 "register_data":"20210423"
}
```

----------------------------------------------
>> <h3> Sample of Payload in Datalake </h3>
-------------------------
> one_piece
>> collection not_fruit_user

```
> db.not_fruit_user.findOne()
{
        "_id" : ObjectId("61ae0e981a5f75689018f0ca"),
        "payload" : {
                "personal_characteristics" : {
                        "first_name" : "Antonio",
                        "last_name" : "Weaver",
                        "gender" : "M",
                        "race" : "Tenryūbitos",
                        "birthday" : "1964-08-19",
                        "age" : "57"
                },
                "physical_characteristics" : {
                        "has_disability" : "left leg amputation",
                        "color_hair" : "Crimson",
                        "color_skill" : "BLUISH",
                        "scar" : "Left arm",
                        "color_eyes" : "Crimson"
                },
                "social_characteristics" : {
                        "security_social_number" : "622-15-8792",
                        "phone" : "001-935-652-4806x982",
                        "sketch" : "https://www.lorempixel.com/820/394"
                },
                "address_information" : {
                        "region" : "West Blue",
                        "country" : "Country of Kano",
                        "city" : "West Wendy",
                        "street" : "Timothy Trail",
                        "number" : "047",
                        "postalcode" : "75496",
                        "mailer" : "antonio.sout047@onepiece.com.br"
                }
        }
}

```
>> collection fruit_user

```
> db.fruit_user.findOne()
{
        "_id" : ObjectId("61ae115a1a5f7568901933b4"),
        "payload" : {
                "personal_characteristics" : {
                        "first_name" : "Patrick",
                        "last_name" : "Miller",
                        "gender" : "M",
                        "race" : "Giants",
                        "birthday" : "1942-03-20",
                        "age" : "79"
                },
                "fruit_characteristics" : {
                        "type_of_fruit" : "Paramecia",
                        "fruit_name" : "Nail file Nail file no Mi",
                        "fruit_category" : "Threatening",
                        "number_times_resurrected" : "63"
                },
                "job_characteristics" : {
                        "job" : "Medice",
                        "current_job" : "NO",
                        "contracting_company" : "Espinoza PLC",
                        "start_date" : "1957/03/15",
                        "year_working_time" : "64",
                        "initial_salary" : "6980.0",
                        "current_wage" : "7608.2"
                },
                "physical_characteristics" : {
                        "type_of_tatoo" : "it does not have",
                        "where_in_body" : "it does not have",
                        "color_of_tatoo" : "it does not have",
                        "color_eyes" : "GreenYellow",
                        "color_hair" : "GreenYellow",
                        "has_disability" : "no deficiency"
                },
                "social_characteristics" : {
                        "security_social_number" : "088-98-1504",
                        "phone" : "001-067-966-9205x26526",
                        "sketch" : "https://www.lorempixel.com/900/849"
                },
                "rewards_informations" : {
                        "main_crime" : "Murders",
                        "code_crime" : "2",
                        "tax_collected_government" : 55174.7,
                        "debt_with_government" : "52004.7",
                        "rewards" : "947598.4"
                },
                "address_information" : {
                        "region" : "Sky",
                        "country" : "Shandora",
                        "city" : "Riggschester",
                        "street" : "Denise Villages",
                        "number" : "61234",
                        "postalcode" : "06882",
                        "mailer" : "patrick.sout61234@onepiece.com.br",
                        "sketch" : "https://www.lorempixel.com/900/849"
                }
        }
}
```