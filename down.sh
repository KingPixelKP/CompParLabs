#!/usr/bin/bash

set -e

echo "Closing docker containers..."

cd docker-spark-env
docker compose down
cd ..

echo "Done"