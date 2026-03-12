# Medallion Architecture Scripts

This directory contains utility scripts for managing the medallion architecture infrastructure.

## Scripts

### start-infrastructure.sh
Starts all Docker Compose services for the medallion architecture.

**Usage:**
```bash
bash scripts/start-infrastructure.sh
```

**Prerequisites:**
- `.env` file must exist (copy from `.env.example`)
- Docker and Docker Compose must be installed
- All default passwords in `.env` should be changed

**What it does:**
1. Checks if `.env` file exists
2. Starts all Docker Compose services
3. Waits for services to be healthy
4. Displays service status and access URLs

### stop-infrastructure.sh
Stops all Docker Compose services.

**Usage:**
```bash
bash scripts/stop-infrastructure.sh
```

**What it does:**
1. Checks if services are running
2. Stops all Docker Compose services
3. Displays shutdown confirmation

### validate-infrastructure.sh
Validates that all infrastructure services are running and healthy.

**Usage:**
```bash
bash scripts/validate-infrastructure.sh
```

**What it checks:**
- Docker and Docker Compose installation
- All container status
- Service health endpoints
- PostgreSQL readiness
- Kafka broker availability

**Exit codes:**
- 0: All services are healthy
- 1: Some services are not healthy

## Quick Start

### 1. Setup Environment
```bash
# Copy environment template
cp .env.example .env

# Edit .env and change all default passwords
nano .env
```

### 2. Start Infrastructure
```bash
bash scripts/start-infrastructure.sh
```

### 3. Validate Infrastructure
```bash
bash scripts/validate-infrastructure.sh
```

### 4. Access Services
- **Grafana**: http://localhost:3000 (admin / password from .env)
- **Prometheus**: http://localhost:9090
- **Kibana**: http://localhost:5601
- **MinIO Console**: http://localhost:9001 (minioadmin / password from .env)
- **Spark Master**: http://localhost:8080

### 5. Stop Infrastructure
```bash
bash scripts/stop-infrastructure.sh
```

## Troubleshooting

### Services won't start
```bash
# Check Docker Compose logs
docker-compose logs

# Check specific service logs
docker-compose logs <service-name>

# Verify Docker is running
docker ps
```

### Services are running but not healthy
```bash
# Run validation script
bash scripts/validate-infrastructure.sh

# Check individual service logs
docker-compose logs <service-name>
```

### Port conflicts
If you get "port already in use" errors:
1. Check what's using the port: `lsof -i :<port>`
2. Stop the conflicting service
3. Or modify docker-compose.yml to use different ports

### Database connection issues
```bash
# Test PostgreSQL connection
docker-compose exec postgres psql -U medallion_user -d medallion_metadata -c "SELECT 1"

# Check PostgreSQL logs
docker-compose logs postgres
```

## Security Notes

- **Never commit `.env` to version control** - it contains passwords
- **Change all default passwords** in `.env` before running in production
- **Use strong passwords** for all services
- **Restrict network access** to services in production
- **Use TLS/SSL** for all communications in production
- **Implement proper backup** procedures for data

## Advanced Usage

### View service logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f <service-name>

# Last 100 lines
docker-compose logs --tail=100
```

### Execute commands in containers
```bash
# PostgreSQL
docker-compose exec postgres psql -U medallion_user -d medallion_metadata

# Kafka
docker-compose exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --list

# Spark
docker-compose exec spark-master spark-shell
```

### Backup and Restore

#### PostgreSQL Backup
```bash
docker-compose exec postgres pg_dump -U medallion_user medallion_metadata > backup.sql
```

#### PostgreSQL Restore
```bash
docker-compose exec -T postgres psql -U medallion_user medallion_metadata < backup.sql
```

### Clean up everything
```bash
# Stop services and remove volumes (WARNING: deletes all data)
docker-compose down -v

# Remove all containers and images
docker-compose down --rmi all
```

## References

- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Docker CLI Reference](https://docs.docker.com/engine/reference/commandline/cli/)
- [Bash Scripting Guide](https://www.gnu.org/software/bash/manual/)
