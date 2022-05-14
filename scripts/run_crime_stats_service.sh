set -eou pipefail

readonly JAR_PATH="${1}"
readonly STREET_CRIMES_DATA_PATH="${2}"

java -cp "${JAR_PATH}" org.example.service.CrimeStatsService "${STREET_CRIMES_DATA_PATH}" |& tee -a crime_stat_service.log