#!/usr/bin/env bash
set -euo pipefail
umask 077

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
project_directory=$(cd -- "$script_directory/../../.." && pwd -P)
workflow=ci-postgresql
[[ $(basename -- "$script_directory") = "$workflow" ]]
[[ $# = 1 && $1 =~ ^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{16}$ ]] || {
    echo 'Usage: bash local-cleanup.sh <run-id>' >&2; exit 2;
}
run_directory="$project_directory/.github/ci-local/$workflow/$1"
[[ $(realpath -- "$run_directory") = "$run_directory" ]]
record="$run_directory/resources.txt"
[[ ! -L $record ]]
if [[ ! -f $record ]]; then
    echo 'No resource record remains; nothing to remove.'
    exit 0
fi
project_name=$(basename -- "$project_directory" | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9' '-')
project_name=${project_name%-}
resource_name="ci-${project_name:0:30}-$workflow-${1,,}"
declare -A resources=()
while IFS='=' read -r key value; do
    case "$key" in daemon_id|builder_name|image_id|container_id) ;; *) echo 'Invalid record key.' >&2; exit 1 ;; esac
    [[ ! -v resources[$key] ]] || { echo 'Duplicate record key.' >&2; exit 1; }
    resources[$key]=$value
done < "$record"
[[ ${#resources[@]} = 4 && ${resources[builder_name]} = "$resource_name" ]]
[[ ${resources[image_id]} = '' || ${resources[image_id]} =~ ^sha256:[a-f0-9]{64}$ ]]
[[ ${resources[container_id]} = '' || ${resources[container_id]} =~ ^[a-f0-9]{64}$ ]]
[[ $(docker info --format '{{.ID}}') = "${resources[daemon_id]}" ]]

# List first: a Docker connection error must not be treated as an absent resource.
result=0
container_id=$(docker container ls --all --quiet --no-trunc --filter "name=^/${resource_name}$")
if [[ -n $container_id ]]; then
    [[ ${resources[container_id]} = '' || $container_id = "${resources[container_id]}" ]]
    [[ $(docker container inspect --format '{{index .Config.Labels "ci.local.run"}}' "$container_id") = "$resource_name" ]]
    if [[ $(docker container inspect --format '{{.State.Running}}' "$container_id") = true ]]; then
        if ! docker exec "$container_id" /bin/bash ".github/workflows/$workflow/run-tests.sh" stop; then
            echo 'Database shutdown failed; removing this disposable container and preserving the failure.' >&2
            result=1
        fi
    fi
    if ! docker logs "$container_id" > "$run_directory/reports/container.log" 2>&1; then result=1; fi
    if ! docker container rm --force "$container_id"; then result=1; fi
fi
image_id=$(docker image ls --quiet --no-trunc "$resource_name:local")
if [[ -n $image_id ]]; then
    [[ ${resources[image_id]} = '' || $image_id = "${resources[image_id]}" ]]
    [[ $(docker image inspect --format '{{index .Config.Labels "ci.local.run"}}' "$image_id") = "$resource_name" ]]
    # No force and no parent pruning: other tags or users prevent deletion.
    if ! docker image rm --no-prune "$image_id"; then result=1; fi
elif [[ -n ${resources[image_id]} ]]; then
    remaining_ids=$(docker image ls --all --quiet --no-trunc)
    if grep --fixed-strings --line-regexp --quiet "${resources[image_id]}" <<< "$remaining_ids"; then
        echo 'Recorded image remains without its original tag; manual review required.' >&2
        result=1
    fi
fi
builders=$(docker buildx ls --format '{{.Name}}')
if grep --fixed-strings --line-regexp --quiet "$resource_name" <<< "$builders"; then
    # Removing this dedicated docker-container builder also removes its build cache.
    if ! docker buildx rm "$resource_name"; then result=1; fi
fi
if [[ $result = 0 ]]; then
    rm -- "$record"
    echo 'Removed this run’s Docker resources and dedicated build cache; reports retained.'
else
    echo 'Cleanup reported a failure; resource record retained for review.' >&2
fi
exit "$result"
