

                        #********************************************************************************#
                        #                                                                                #
                        #                                  нεℓℓσ,вαтεs!                                  #
                        #                                                                                #
                        #   filename: init.sh                                                            #
                        #   created: 2022-05-22                                                          #
                        #   system: Windows                                                              #
                        #   version: 64bit                                                               #
                        #                                       by: Bates <https://github.com/batestin1> #
                        #********************************************************************************#
                        #                           import your librarys below                           #
                        #********************************************************************************#

echo "copiando o crontab para o servidor"
service cron start
cd /home/bates/repositorio/big_data/one_piece/scheduler/
chmod +x crontab
cp /home/bates/repositorio/big_data/one_piece/scheduler/crontab /tmp/crontab.hUDskU/
cd /tmp/crontab.hUDskU/
echo "Trabalho agendado!"


