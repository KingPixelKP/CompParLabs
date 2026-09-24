#!/usr/bin/bash

set -e

./down.sh

echo "Rebuilding docker containers..."

cd docker-spark-env
docker compose build
cd ..


echo "Done"
echo "To boot up the containers run boot.sh"