mkdir -p ./logs ./plugins ./config
if [[ -f .env ]]; then
    rm .env
fi
if [[ -z "${AIRFLOW_UID}" ]]; then
    echo -e "AIRFLOW_UID=$(id -u)" >> .env
fi
echo -e "AIRFLOW_PROJ_DIR=$(pwd)" >> .env