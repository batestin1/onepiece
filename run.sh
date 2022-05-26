#/bash/bin


echo "Começando o projeto"
cd dataset
mkdir json_files
python scrapy_one_piece.py

cd ..
python scripts/script_bronze.py
python scripts/script_silver.py
python scripts/script_gold.py

echo "Projeto Finalizado"