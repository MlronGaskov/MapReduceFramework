#!/usr/bin/env bash
set -e

MINIO_ALIAS="minio"
MINIO_ENDPOINT="http://127.0.0.1:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
BUCKET_NAME="jobs"
COORDINATOR_URL="http://127.0.0.1:8080"

if ! command -v mc &> /dev/null; then
    echo "Error: mc (MinIO Client) is not installed. Please install mc and try again."
    exit 1
fi

echo "Waiting for MinIO to be ready at ${MINIO_ENDPOINT}..."
until [ "$(curl -s -o /dev/null -w '%{http_code}' ${MINIO_ENDPOINT}/minio/health/ready)" == "200" ]; do
    echo "MinIO is not ready yet..."
    sleep 2
done
echo "MinIO is ready."

echo "Setting alias for MinIO..."
until mc alias set $MINIO_ALIAS $MINIO_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY; do
    echo "Failed to set alias. Retrying in 2 seconds..."
    sleep 2
done

if ! mc ls ${MINIO_ALIAS}/${BUCKET_NAME} &> /dev/null; then
    echo "Bucket '${BUCKET_NAME}' not found. Creating bucket..."
    mc mb ${MINIO_ALIAS}/${BUCKET_NAME}
fi

if [ "$#" -lt 8 ]; then
    echo "Usage: $0 <file_path> <inputsPath> <mappersOutputsPath> <reducersOutputsPath> <dataStorageConnectionString> <mappersCount> <reducersCount> <sorterInMemoryRecords>"
    exit 1
fi

FILE_PATH="$1"
JOB_FILENAME=$(basename "$FILE_PATH")
INPUTS_PATH="$2"
MAPPERS_OUTPUTS_PATH="$3"
REDUCERS_OUTPUTS_PATH="$4"
DATA_STORAGE_CONNECTION_STRING="$5"
MAPPERS_COUNT="$6"
REDUCERS_COUNT="$7"
SORTER_IN_MEMORY_RECORDS="$8"

echo "Uploading file '$FILE_PATH' to bucket '${BUCKET_NAME}'..."
mc cp "$FILE_PATH" ${MINIO_ALIAS}/${BUCKET_NAME}/
echo "File '$FILE_PATH' has been successfully uploaded to the '${BUCKET_NAME}' bucket on MinIO."

JOB_PAYLOAD=$(cat <<EOF
{
    "jobId": 1,
    "jobPath": "$JOB_FILENAME",
    "jobStorageConnectionString": "MINIO:endpoint=http://minio:9000;accessKey=minioadmin;secretKey=minioadmin;bucketName=jobs",
    "inputsPath": "$INPUTS_PATH",
    "mappersOutputsPath": "$MAPPERS_OUTPUTS_PATH",
    "reducersOutputsPath": "$REDUCERS_OUTPUTS_PATH",
    "dataStorageConnectionString": "$DATA_STORAGE_CONNECTION_STRING",
    "mappersCount": $MAPPERS_COUNT,
    "reducersCount": $REDUCERS_COUNT,
    "sorterInMemoryRecords": $SORTER_IN_MEMORY_RECORDS
}
EOF
)

echo $JOB_PAYLOAD

echo "Submitting job to coordinator at ${COORDINATOR_URL}..."
curl -X PUT -H "Content-Type: application/json" -d "$JOB_PAYLOAD" "$COORDINATOR_URL/jobs"
echo "Job submitted successfully."
