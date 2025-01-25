mkdir -p ./logs ./plugins ./config
if [[ -f .env ]]; then
    rm .env
fi
if [[ -z "${AIRFLOW_UID}" ]]; then
    echo "AIRFLOW_UID=$(id -u)" >> .env
fi
echo "AIRFLOW_PROJ_DIR=$(pwd)" >> .env