#!/usr/bin/env bash
set -e

WORKERS_COUNT=${1:-2}

cd "../"
./gradlew :mr-app:clean :mr-app:jar
cp "mr-app/build/libs/mr-app-1.0.jar" "./scripts"

cd "./scripts"

docker compose down --rmi all --volumes --remove-orphans

docker compose up --scale worker=$WORKERS_COUNT

echo "Cluster started successfully!"