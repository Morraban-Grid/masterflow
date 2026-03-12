#!/bin/bash

# Medallion Architecture Infrastructure Validation Script
# This script validates that all infrastructure services are running and healthy

set -e

echo "=========================================="
echo "Medallion Architecture Infrastructure Validation"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check service health
check_service() {
    local service=$1
    local port=$2
    local endpoint=$3
    
    echo -n "Checking $service... "
    
    if curl -s -f "http://localhost:$port$endpoint" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ OK${NC}"
        return 0
    else
        echo -e "${RED}✗ FAILED${NC}"
        return 1
    fi
}

# Function to check Docker container
check_container() {
    local container=$1
    
    echo -n "Checking container $container... "
    
    if docker ps --filter "name=$container" --filter "status=running" | grep -q "$container"; then
        echo -e "${GREEN}✓ Running${NC}"
        return 0
    else
        echo -e "${RED}✗ Not running${NC}"
        return 1
    fi
}

# Check Docker
echo "Checking Docker..."
if ! command -v docker &> /dev/null; then
    echo -e "${RED}✗ Docker is not installed${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker is installed${NC}"
echo ""

# Check Docker Compose
echo "Checking Docker Compose..."
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}✗ Docker Compose is not installed${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker Compose is installed${NC}"
echo ""

# Check containers
echo "Checking containers..."
containers=(
    "medallion-zookeeper"
    "medallion-kafka-1"
    "medallion-kafka-2"
    "medallion-kafka-3"
    "medallion-postgres"
    "medallion-minio"
    "medallion-spark-master"
    "medallion-spark-worker-1"
    "medallion-spark-worker-2"
    "medallion-prometheus"
    "medallion-grafana"
    "medallion-elasticsearch"
    "medallion-kibana"
)

failed_containers=0
for container in "${containers[@]}"; do
    if ! check_container "$container"; then
        ((failed_containers++))
    fi
done
echo ""

# Check services
echo "Checking services..."
services=(
    "Prometheus:9090:/-/healthy"
    "Grafana:3000:/api/health"
    "Elasticsearch:9200:/"
    "Kibana:5601:/api/status"
    "MinIO:9000:/minio/health/live"
    "Spark Master:8080:/"
)

failed_services=0
for service in "${services[@]}"; do
    IFS=':' read -r name port endpoint <<< "$service"
    if ! check_service "$name" "$port" "$endpoint"; then
        ((failed_services++))
    fi
done
echo ""

# Check PostgreSQL
echo "Checking PostgreSQL..."
if docker-compose exec -T postgres pg_isready -U medallion_user > /dev/null 2>&1; then
    echo -e "${GREEN}✓ PostgreSQL is ready${NC}"
else
    echo -e "${RED}✗ PostgreSQL is not ready${NC}"
    ((failed_services++))
fi
echo ""

# Check Kafka
echo "Checking Kafka..."
if docker-compose exec -T kafka-1 kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Kafka is ready${NC}"
else
    echo -e "${RED}✗ Kafka is not ready${NC}"
    ((failed_services++))
fi
echo ""

# Summary
echo "=========================================="
echo "Validation Summary"
echo "=========================================="
echo "Failed containers: $failed_containers"
echo "Failed services: $failed_services"
echo ""

if [ $failed_containers -eq 0 ] && [ $failed_services -eq 0 ]; then
    echo -e "${GREEN}✓ All infrastructure services are healthy!${NC}"
    exit 0
else
    echo -e "${RED}✗ Some services are not healthy${NC}"
    exit 1
fi
