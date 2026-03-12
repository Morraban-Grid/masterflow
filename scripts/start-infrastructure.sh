#!/bin/bash

# Medallion Architecture Infrastructure Startup Script
# This script starts all infrastructure services

set -e

echo "=========================================="
echo "Starting Medallion Architecture Infrastructure"
echo "=========================================="
echo ""

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "ERROR: .env file not found!"
    echo "Please create .env file from .env.example:"
    echo "  cp .env.example .env"
    echo ""
    echo "Then edit .env and change all default passwords."
    exit 1
fi

echo "Starting Docker Compose services..."
docker-compose up -d

echo ""
echo "Waiting for services to be healthy..."
sleep 10

# Check if services are running
echo ""
echo "Checking service status..."
docker-compose ps

echo ""
echo "=========================================="
echo "Infrastructure Startup Complete"
echo "=========================================="
echo ""
echo "Services are available at:"
echo "  - Grafana: http://localhost:3000"
echo "  - Prometheus: http://localhost:9090"
echo "  - Kibana: http://localhost:5601"
echo "  - MinIO Console: http://localhost:9001"
echo "  - Spark Master: http://localhost:8080"
echo ""
echo "To validate infrastructure health, run:"
echo "  bash scripts/validate-infrastructure.sh"
echo ""
echo "To stop services, run:"
echo "  docker-compose down"
echo ""
