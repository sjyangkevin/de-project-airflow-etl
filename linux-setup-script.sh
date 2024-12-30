mkdir -p ./dags ./logs ./plugins ./config
rm .env
echo -e "AIRFLOW_PROJ_DIR=$(pwd)" >> .env
echo -e "TRINO_PROJ_DIR=$(pwd)" >> .env
echo -e "AIRFLOW_UID=$(id -u)" >> .env