mkdir -p ./logs ./plugins ./config
if [[ -f .env ]]; then
    rm .env
fi
echo "AIRFLOW_PROJ_DIR=$(pwd)" >> .env