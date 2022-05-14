set -eou pipefail

SPARK_HOME="${SPARK_HOME:-/opt/spark-3.1.2-bin-hadoop3.2}"
readonly APP_JAR_PATH="${1}"
readonly INPUT_DATA_PATH="${2}"
readonly OUTPUT_DATA_PATH="${3}"

export SPARK_PRINT_LAUNCH_COMMAND=true
"${SPARK_HOME}"/bin/spark-submit --name "Write_and_show_district_kpis" \
--master local[*] \
--class org.example.jobs.CrimeKPIs \
"${APP_JAR_PATH}" \
"${INPUT_DATA_PATH}" \
"district" \
"${OUTPUT_DATA_PATH}"
