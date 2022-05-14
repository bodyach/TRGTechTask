set -eou pipefail

SPARK_HOME="${SPARK_HOME:-/opt/spark-3.1.2-bin-hadoop3.2}"
readonly APP_JAR_PATH="${1}"
readonly STREET_CRIME_DATA_PATH="${2}"
readonly NUMBER_OF_ROWS_TO_SHOW="${3}"
readonly CRIME_TABLE_NAME="streetCrimes"
readonly SQL_TO_EXECUTE="${4:-"NOSQL"}"

export SPARK_PRINT_LAUNCH_COMMAND=true
"${SPARK_HOME}"/bin/spark-submit --name "Crime_table_read" \
--master local[*] \
--class org.example.jobs.ReadParquetInputData \
"${APP_JAR_PATH}" \
"${STREET_CRIME_DATA_PATH}" \
"${NUMBER_OF_ROWS_TO_SHOW}" \
"${CRIME_TABLE_NAME}" \
"${SQL_TO_EXECUTE}"
