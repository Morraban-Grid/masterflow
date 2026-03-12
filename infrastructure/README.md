# Medallion Architecture Infrastructure

This directory contains all infrastructure configuration files for the medallion architecture.

## Directory Structure

```
infrastructure/
├── postgres/
│   └── init.sql              # PostgreSQL initialization script
├── prometheus/
│   ├── prometheus.yml        # Prometheus configuration
│   └── alerts.yml            # Alert rules
├── grafana/
│   └── provisioning/
│       ├── datasources/      # Grafana datasource configurations
│       └── dashboards/       # Grafana dashboard definitions
└── README.md                 # This file
```

## Services

### PostgreSQL
- **Container**: medallion-postgres
- **Port**: 5432
- **Database**: medallion_metadata
- **Initialization**: Runs `init.sql` on first startup
- **Schemas**: medallion, lineage, access_control, monitoring

### Kafka Cluster
- **Containers**: medallion-kafka-1, medallion-kafka-2, medallion-kafka-3
- **Ports**: 9092, 9093, 9094
- **Replication Factor**: 3
- **Min ISR**: 2
- **Retention**: 7 days

### MinIO
- **Container**: medallion-minio
- **Ports**: 9000 (API), 9001 (Console)
- **Buckets**: bronze, silver, gold, quarantine

### Spark Cluster
- **Master**: medallion-spark-master (port 8080, 7077)
- **Workers**: medallion-spark-worker-1, medallion-spark-worker-2
- **Memory per Worker**: 2GB
- **Cores per Worker**: 2

### Prometheus
- **Container**: medallion-prometheus
- **Port**: 9090
- **Retention**: 30 days
- **Scrape Interval**: 15 seconds

### Grafana
- **Container**: medallion-grafana
- **Port**: 3000
- **Default User**: admin
- **Datasources**: Prometheus, Elasticsearch

### Elasticsearch
- **Container**: medallion-elasticsearch
- **Port**: 9200
- **Heap Size**: 512MB

### Kibana
- **Container**: medallion-kibana
- **Port**: 5601

## Getting Started

### 1. Create Environment File

```bash
cp .env.example .env
```

**IMPORTANT**: Edit `.env` and change all default passwords:
- POSTGRES_PASSWORD
- MINIO_ROOT_PASSWORD
- GRAFANA_PASSWORD

### 2. Start Services

```bash
docker-compose up -d
```

### 3. Verify Services

```bash
docker-compose ps
```

All services should show "healthy" status.

### 4. Access Services

- **Grafana**: http://localhost:3000 (admin / password)
- **Prometheus**: http://localhost:9090
- **Kibana**: http://localhost:5601
- **MinIO Console**: http://localhost:9001 (minioadmin / password)
- **Spark Master**: http://localhost:8080

## Configuration Files

### PostgreSQL (init.sql)
- Creates schemas: medallion, lineage, access_control, monitoring
- Creates tables for schema registry, lineage tracking, access control, and monitoring
- Sets up indexes for performance
- Grants permissions to medallion_user

### Prometheus (prometheus.yml)
- Configures scrape targets for all services
- Sets up metrics collection intervals
- Defines alert rules file

### Prometheus Alerts (alerts.yml)
- Kafka broker down alerts
- Spark master/worker down alerts
- PostgreSQL down alerts
- MinIO down alerts
- High CPU/Memory/Disk usage alerts

### Grafana Datasources (datasources/prometheus.yml)
- Configures Prometheus as primary datasource
- Configures Elasticsearch for logs

### Grafana Dashboards (dashboards/)
- medallion-overview.json: Overview dashboard with service status
- dashboards.yml: Dashboard provisioning configuration

## Security Considerations

### Passwords
- All default passwords are in `.env.example`
- **NEVER** commit `.env` to version control
- `.env` is in `.gitignore`
- Change all passwords in production

### Network
- All services communicate over internal Docker network
- Only necessary ports are exposed
- Use firewall rules to restrict access

### Data
- PostgreSQL data is persisted in volumes
- MinIO data is persisted in volumes
- Volumes are not backed up automatically
- Implement backup procedures for production

### Credentials
- No credentials are hardcoded in configuration files
- All credentials come from environment variables
- Use secure vaults for production credentials

## Troubleshooting

### Services Won't Start
```bash
# Check logs
docker-compose logs <service-name>

# Verify network
docker network ls

# Check volumes
docker volume ls
```

### Database Connection Issues
```bash
# Test PostgreSQL connection
docker-compose exec postgres psql -U medallion_user -d medallion_metadata -c "SELECT 1"
```

### Kafka Issues
```bash
# Check broker status
docker-compose exec kafka-1 kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# List topics
docker-compose exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Spark Issues
```bash
# Check Spark master status
curl http://localhost:8080

# Check worker status
curl http://localhost:8081
```

## Maintenance

### Backup
```bash
# Backup PostgreSQL
docker-compose exec postgres pg_dump -U medallion_user medallion_metadata > backup.sql

# Backup MinIO
# Use MinIO client (mc) for backup
```

### Cleanup
```bash
# Stop services
docker-compose down

# Remove volumes (WARNING: deletes data)
docker-compose down -v

# Remove all containers and images
docker-compose down --rmi all
```

## Production Deployment

For production deployment:

1. Use managed services (RDS, S3, etc.) instead of containers
2. Implement proper backup and disaster recovery
3. Use secrets management (Vault, AWS Secrets Manager)
4. Implement proper monitoring and alerting
5. Use load balancers for high availability
6. Implement proper logging and audit trails
7. Use TLS/SSL for all communications
8. Implement proper access control and authentication

## References

- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Prometheus Documentation](https://prometheus.io/docs/)
- [Grafana Documentation](https://grafana.com/docs/)
- [MinIO Documentation](https://docs.min.io/)
- [Spark Documentation](https://spark.apache.org/docs/)
