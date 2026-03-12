#!/bin/bash

# Medallion Architecture Infrastructure Shutdown Script
# This script stops all infrastructure services

set -e

echo "=========================================="
echo "Stopping Medallion Architecture Infrastructure"
echo "=========================================="
echo ""

# Check if docker-compose is running
if ! docker-compose ps | grep -q "Up"; then
    echo "No services are currently running."
    exit 0
fi

echo "Stopping Docker Compose services..."
docker-compose down

echo ""
echo "=========================================="
echo "Infrastructure Shutdown Complete"
echo "=========================================="
echo ""
echo "To start services again, run:"
echo "  bash scripts/start-infrastructure.sh"
echo ""
echo "To remove all data (WARNING: destructive), run:"
echo "  docker-compose down -v"
echo ""
