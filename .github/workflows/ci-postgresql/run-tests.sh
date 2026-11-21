#!/usr/bin/env bash
set -euo pipefail

CI_DATABASE_DIRECTORY=/tmp/ci-postgresql
CI_SOCKET_DIRECTORY=/tmp/ci-postgresql-socket
CI_DATABASE_LOG=/tmp/ci-postgresql.log
CI_REPORT_DIRECTORY=/ci-reports
CI_PHASE=all
if [ "$#" -gt 0 ]; then
    CI_PHASE=$1
fi

stop_database() {
    if [ -f "$CI_DATABASE_DIRECTORY/postmaster.pid" ]; then
        runuser -u postgres -- pg_ctl -D "$CI_DATABASE_DIRECTORY" \
            --wait --timeout=120 --mode=fast stop
    fi
}

finish_phase() {
    local result=$?
    trap - EXIT INT TERM
    set +e
    if [ "$CI_PHASE" = all ]; then
        stop_database
        local stop_result=$?
        if [ "$result" -eq 0 ] && [ "$stop_result" -ne 0 ]; then
            result=$stop_result
        fi
    fi
    if [ -f "$CI_DATABASE_LOG" ]; then
        cp "$CI_DATABASE_LOG" "$CI_REPORT_DIRECTORY/postgresql.log" \
            && chmod 0644 "$CI_REPORT_DIRECTORY/postgresql.log"
        local report_result=$?
        if [ "$result" -eq 0 ] && [ "$report_result" -ne 0 ]; then
            result=$report_result
        fi
    fi
    exit "$result"
}

start_database() {
    install -d -o postgres -g postgres -m 700 "$CI_DATABASE_DIRECTORY" "$CI_SOCKET_DIRECTORY"
    install -o postgres -g postgres -m 600 /dev/null "$CI_DATABASE_LOG"
    runuser -u postgres -- initdb -D "$CI_DATABASE_DIRECTORY" \
        --auth-local=peer --auth-host=scram-sha-256 --encoding=UTF8 --locale=C.UTF-8
    runuser -u postgres -- pg_ctl -D "$CI_DATABASE_DIRECTORY" \
        --log "$CI_DATABASE_LOG" --wait --timeout=30 \
        --options "-k $CI_SOCKET_DIRECTORY -h 127.0.0.1 -p 5432" start
    runuser -u postgres -- psql -X --set ON_ERROR_STOP=1 \
        --host "$CI_SOCKET_DIRECTORY" --dbname postgres --command 'SELECT version();'
    cat /etc/os-release
    /opt/venv/bin/python --version
}

prepare_database() {
    runuser -u postgres -- psql -X --set ON_ERROR_STOP=1 \
        --host "$CI_SOCKET_DIRECTORY" --dbname postgres <<'CI_DATABASE_SQL'
CREATE ROLE ci_user LOGIN CREATEDB PASSWORD 'ci_password';
CREATE DATABASE ci_test OWNER ci_user;
CI_DATABASE_SQL
    PGPASSWORD=ci_password psql -X --set ON_ERROR_STOP=1 \
        --host 127.0.0.1 --port 5432 --username ci_user --dbname ci_test --command 'SELECT 1;'
    runuser -u ci -- env CI_PHASE=prepare /bin/bash -euo pipefail <<'CI_PROJECT_PREPARATION'
cd /workspace
export POSTGRES_DB_HOST=127.0.0.1
export POSTGRES_DB_PORT=5432
export POSTGRES_DB_USER=ci_user
export POSTGRES_DB_PASS=ci_password
export POSTGRES_DB_NAME=ci_test
# Insert into the preparation or test shell with CI_PHASE set explicitly.
case "${CI_PHASE:?CI_PHASE must be set}" in
    prepare|tests) ;;
    *) printf 'Unsupported command phase: %s\n' "$CI_PHASE" >&2; exit 2 ;;
esac
set +e
(
    set -euo pipefail
    printf 'No migrations or shared fixtures required.\n'
) 2>&1 | tee "/ci-reports/$CI_PHASE.log"
CI_COMMAND_STATUSES=("${PIPESTATUS[@]}")
set -e

CI_COMMAND_EXIT_CODE=${CI_COMMAND_STATUSES[0]}
printf '%s\n' "$CI_COMMAND_EXIT_CODE" > "/ci-reports/$CI_PHASE-exit-code.txt"
if [ "$CI_COMMAND_EXIT_CODE" -ne 0 ]; then
    exit "$CI_COMMAND_EXIT_CODE"
fi
exit "${CI_COMMAND_STATUSES[1]}"
CI_PROJECT_PREPARATION
}

run_project_tests() {
    runuser -u ci -- env CI_PHASE=tests /bin/bash -euo pipefail <<'CI_PROJECT_TESTS'
cd /workspace
export POSTGRES_DB_HOST=127.0.0.1
export POSTGRES_DB_PORT=5432
export POSTGRES_DB_USER=ci_user
export POSTGRES_DB_PASS=ci_password
export POSTGRES_DB_NAME=ci_test
# Insert into the preparation or test shell with CI_PHASE set explicitly.
case "${CI_PHASE:?CI_PHASE must be set}" in
    prepare|tests) ;;
    *) printf 'Unsupported command phase: %s\n' "$CI_PHASE" >&2; exit 2 ;;
esac
set +e
(
    set -euo pipefail
    /opt/venv/bin/python -m pytest --junitxml=/ci-reports/junit.xml
) 2>&1 | tee "/ci-reports/$CI_PHASE.log"
CI_COMMAND_STATUSES=("${PIPESTATUS[@]}")
set -e

CI_COMMAND_EXIT_CODE=${CI_COMMAND_STATUSES[0]}
printf '%s\n' "$CI_COMMAND_EXIT_CODE" > "/ci-reports/$CI_PHASE-exit-code.txt"
if [ "$CI_COMMAND_EXIT_CODE" -ne 0 ]; then
    exit "$CI_COMMAND_EXIT_CODE"
fi
exit "${CI_COMMAND_STATUSES[1]}"
CI_PROJECT_TESTS
}

trap finish_phase EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir -p "$CI_REPORT_DIRECTORY"
chown ci:ci "$CI_REPORT_DIRECTORY"

case "$CI_PHASE" in
    start) start_database ;;
    prepare) prepare_database ;;
    tests) run_project_tests ;;
    stop) stop_database ;;
    all)
        start_database
        prepare_database
        run_project_tests
        ;;
    *) printf 'Unknown CI phase: %s\n' "$CI_PHASE" >&2; exit 2 ;;
esac
