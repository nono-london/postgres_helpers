#!/usr/bin/env bash
set -euo pipefail
umask 077

# Run from any directory; all local state belongs to this checkout.
script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
project_directory=$(cd -- "$script_directory/../../.." && pwd -P)
workflow=ci-postgresql
[[ $(basename -- "$script_directory") = "$workflow" ]]
cd -- "$project_directory"
[[ $# = 0 ]] || { echo 'Usage: bash local-build.sh' >&2; exit 2; }
command -v docker >/dev/null
docker buildx version
daemon_id=$(docker info --format '{{.ID}}')
[[ -n $daemon_id ]]

# Exclude local state before creating it. Never inspect private environment files.
[[ -z $(git ls-files -- .github/ci-local) ]] || {
    echo 'Refusing to use a local-state directory containing tracked files.' >&2; exit 1;
}
[[ ! -L .gitignore && ! -L "$script_directory/Dockerfile.dockerignore" ]]
if ! git check-ignore --no-index --quiet .github/ci-local/probe; then
    printf '\n/.github/ci-local/\n' >> .gitignore
fi
git check-ignore --no-index --quiet .github/ci-local/probe
ignore_file="$script_directory/Dockerfile.dockerignore"
[[ -f $ignore_file ]]
if [[ $(tail -n 1 "$ignore_file") != '**/.github/ci-local' ]]; then
    printf '\n.github/ci-local\n**/.github/ci-local\n' >> "$ignore_file"
fi

state_root="$project_directory/.github/ci-local/$workflow"
mkdir -p -- "$state_root"
[[ $(realpath -- "$state_root") = "$state_root" ]]
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
run_directory="$state_root/$run_id"
mkdir -- "$run_directory"
mkdir -- "$run_directory/reports"
# The non-root application user must be able to create report files.
chmod 0777 "$run_directory/reports"
project_name=$(basename -- "$project_directory" | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9' '-')
project_name=${project_name%-}
resource_name="ci-${project_name:0:30}-$workflow-${run_id,,}"
image_id=
container_id=
record_resources() {
    printf 'daemon_id=%s\nbuilder_name=%s\nimage_id=%s\ncontainer_id=%s\n' \
        "$daemon_id" "$resource_name" "$image_id" "$container_id" > "$run_directory/resources.tmp"
    mv -- "$run_directory/resources.tmp" "$run_directory/resources.txt"
}
record_resources
finish() {
    local result=$? cleanup_result
    trap - EXIT INT TERM
    set +e
    bash "$script_directory/local-cleanup.sh" "$run_id" 2>&1 | tee "$run_directory/reports/cleanup.log"
    cleanup_result=$?
    printf '%s\n' "$result" > "$run_directory/reports/build-test-exit-code.txt"
    printf '%s\n' "$cleanup_result" > "$run_directory/reports/cleanup-exit-code.txt"
    printf 'Local reports: %s\n' "$run_directory/reports"
    if [[ $result = 0 && $cleanup_result != 0 ]]; then result=$cleanup_result; fi
    exit "$result"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# The planned unique names are recorded before creation, allowing recovery if
# an interruption happens before Docker returns an ID. Never change the default builder.
docker buildx create --name "$resource_name" --driver docker-container
docker buildx build --builder "$resource_name" --load --progress plain \
    --label "ci.local.run=$resource_name" --tag "$resource_name:local" \
    --file "$script_directory/Dockerfile" . 2>&1 | tee "$run_directory/reports/build.log"
image_id=$(docker image inspect --format '{{.Id}}' "$resource_name:local")
record_resources
container_id=$(docker create --name "$resource_name" --label "ci.local.run=$resource_name" \
    --mount "type=bind,source=$run_directory/reports,target=/ci-reports" \
    --entrypoint /usr/bin/sleep "$image_id" infinity)
record_resources
docker start "$container_id"
for phase in start prepare tests; do
    docker exec "$container_id" /bin/bash ".github/workflows/$workflow/run-tests.sh" "$phase" \
        2>&1 | tee "$run_directory/reports/$phase-console.log"
done
