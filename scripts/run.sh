#!/usr/bin/env bash
set -e

if [ -f .env ]; then
    source .env
fi

cd "$GRADLEW_DIR"
./gradlew $GRADLE_TASKS

mkdir -p "$TARGET_DIR"
cp "$JAR_PATH" "$TARGET_DIR"

cp "$DOCKER_FILES_DIR/Dockerfile" "$TARGET_DIR"
cp "$DOCKER_FILES_DIR/docker-compose.yml" "$TARGET_DIR"

cd "$TARGET_DIR"

docker compose down --rmi all --volumes --remove-orphans

docker compose up --scale worker=$WORKERS_COUNT

echo "Deployment finished successfully!"
