#!/usr/bin/bash

set -e

echo "Closing docker containers..."

cd docker-spark-env
docker compose down
cd ..

echo "Done"

echo "Booting docker environment..."

cd docker-spark-env
docker compose up -d --scale spark-worker=3
cd ..

echo "Done"