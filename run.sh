#/bash/bin
                        #********************************************************************************#
                        #                                                                                #
                        #                                  нεℓℓσ,вαтεs!                                  #
                        #                                                                                #
                        #   filename: run.sh                                                             #
                        #   created: 2022-05-22                                                          #
                        #   system: Windows                                                              #
                        #   version: 64bit                                                               #
                        #                                       by: Bates <https://github.com/batestin1> #
                        #********************************************************************************#
                        #                           import your librarys below                           #
                        #********************************************************************************#

LOG=/home/bates/repositorio/big_data/one_piece/logs/logs.log
echo "Começando o projeto"
cd /home/bates/repositorio/big_data/one_piece/
cd dataset
mkdir json_files >> ${LOG}
python3 scrapy_one_piece.py >>${LOG}

cd ..
python3 scripts/script_bronze.py >>${LOG}
python3 scripts/script_silver.py >>${LOG}
python3 scripts/script_gold.py >>${LOG}


echo "Projeto Finalizado"