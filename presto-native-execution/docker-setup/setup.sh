#!/bin/bash

set -e

echo "Setting up Presto Coordinator and Native Worker containers..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if we're in the right directory
if [ ! -f "docker-compose.yml" ]; then
    print_error "docker-compose.yml not found. Please run this script from the docker-setup directory."
    exit 1
fi

# Build the prestissimo runtime image if it doesn't exist
if ! docker image inspect presto/prestissimo-runtime:centos9 > /dev/null 2>&1; then
    print_warning "Prestissimo runtime image not found. Building it now..."
    cd ..
    docker compose build centos-native-dependency
    docker compose build centos-native-runtime
    cd docker-setup
    print_status "Prestissimo runtime image built successfully."
else
    print_status "Prestissimo runtime image already exists."
fi

# Pull the official Presto coordinator image
print_status "Pulling official Presto coordinator image..."
docker pull prestodb/presto:latest

# Create directories if they don't exist
mkdir -p coordinator-config/catalog
mkdir -p worker-config/catalog
mkdir -p queries

# Start the containers
print_status "Starting containers..."
docker compose up -d

# Wait for services to be healthy
print_status "Waiting for coordinator to be ready..."
timeout=120
counter=0
while [ $counter -lt $timeout ]; do
    if docker compose exec presto-coordinator curl -f http://localhost:8080/v1/info > /dev/null 2>&1; then
        print_status "Coordinator is ready!"
        break
    fi
    sleep 2
    counter=$((counter + 2))
    if [ $counter -ge $timeout ]; then
        print_error "Coordinator failed to start within $timeout seconds"
        docker compose logs presto-coordinator
        exit 1
    fi
done

# Wait a bit more for worker to connect
print_status "Waiting for worker to connect..."
sleep 10

# Check if worker is connected
print_status "Checking cluster status..."
docker compose exec presto-coordinator curl -s http://localhost:8080/v1/node | jq '.'

print_status "Setup complete!"
echo ""
echo "You can now:"
echo "1. Access Presto UI at: http://localhost:8080"
echo "2. Connect to CLI: docker compose exec presto-cli presto-cli --server presto-coordinator:8080"
echo "3. Run a test query: docker compose exec presto-cli presto-cli --server presto-coordinator:8080 --execute 'SELECT * FROM tpch.tiny.nation LIMIT 5'"
echo "4. View logs: docker compose logs -f [presto-coordinator|presto-worker]"
echo "5. Stop containers: docker compose down" 