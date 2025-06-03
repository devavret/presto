# Presto Coordinator + Native Worker Docker Setup

This directory contains a complete Docker setup for running a Java Presto coordinator and a native Prestissimo worker in separate containers with proper networking.

## Architecture

```
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│   Presto CLI        │    │  Java Coordinator   │    │  Native Worker      │
│   (prestodb/presto) │    │  (prestodb/presto)  │    │  (prestissimo)      │
│   Port: -           │    │  Port: 8080         │    │  Port: 7777         │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
           │                          │                          │
           └──────────────────────────┼──────────────────────────┘
                                      │
                              ┌───────────────┐
                              │ Docker Network│
                              │ presto-network│
                              └───────────────┘
```

## Prerequisites

1. **Docker and Docker Compose** installed
2. **Prestissimo runtime image** built (the setup script will build it if needed)
3. **At least 4GB RAM** available for containers

## Quick Start

### 1. Build Prestissimo Images (if not already built)

From the `presto-native-execution` directory:

```bash
# Build dependency image
docker compose build centos-native-dependency

# Build runtime image  
docker compose build centos-native-runtime
```

### 2. Run the Setup

```bash
cd presto-native-execution/docker-setup
chmod +x setup.sh
./setup.sh
```

The setup script will:
- Pull the official Presto coordinator image
- Build Prestissimo images if needed
- Start all containers with proper networking
- Wait for services to be ready
- Verify the cluster is working

### 3. Verify the Setup

After setup completes, you should see:
- Coordinator accessible at http://localhost:8080
- Worker connected and visible in the cluster

## Manual Setup (Alternative)

If you prefer to run commands manually:

```bash
cd presto-native-execution/docker-setup

# Start containers
docker compose up -d

# Check logs
docker compose logs -f presto-coordinator
docker compose logs -f presto-worker

# Check cluster status
docker compose exec presto-coordinator curl -s http://localhost:8080/v1/node
```

## Testing the Setup

### Using Presto CLI

```bash
# Connect to CLI
docker compose exec presto-cli presto-cli --server presto-coordinator:8080

# Run test queries
SELECT * FROM tpch.tiny.nation LIMIT 5;
SHOW CATALOGS;
SHOW SCHEMAS FROM tpch;
```

### Using curl

```bash
# Check coordinator status
curl http://localhost:8080/v1/info

# Check connected nodes
curl http://localhost:8080/v1/node

# Check worker status
curl http://localhost:7777/v1/info
```

### Using Web UI

Open http://localhost:8080 in your browser to access the Presto Web UI.

## Configuration Details

### Coordinator Configuration

- **Container**: `presto-coordinator`
- **Image**: `prestodb/presto:latest`
- **Port**: 8080
- **Config**: `coordinator-config/`
  - `config.properties`: Coordinator settings
  - `jvm.config`: JVM memory settings (2GB)
  - `node.properties`: Node identification
  - `catalog/tpch.properties`: TPCH connector for testing

### Worker Configuration

- **Container**: `presto-worker`
- **Image**: `presto/prestissimo-runtime:centos9`
- **Port**: 7777
- **Config**: `worker-config/`
  - `config.properties`: Worker settings with coordinator discovery
  - `node.properties`: Worker node identification
  - `velox.properties`: Velox engine settings
  - `catalog/tpch.properties`: TPCH connector

### Key Configuration Points

1. **Discovery URI**: Worker connects to coordinator using `http://presto-coordinator:8080`
2. **Network**: Both containers use the `presto-network` bridge network
3. **Hostnames**: Containers can reach each other using their service names
4. **Memory**: Coordinator gets 2GB, worker uses 2GB system memory

## Troubleshooting

### Worker Not Connecting

1. Check coordinator is running:
   ```bash
   docker compose logs presto-coordinator
   curl http://localhost:8080/v1/info
   ```

2. Check worker logs:
   ```bash
   docker compose logs presto-worker
   ```

3. Verify network connectivity:
   ```bash
   docker compose exec presto-worker ping presto-coordinator
   ```

### Memory Issues

If containers are killed due to memory:
1. Reduce JVM heap in `coordinator-config/jvm.config`
2. Reduce system memory in `worker-config/config.properties`
3. Ensure Docker has enough memory allocated

### Port Conflicts

If ports 8080 or 7777 are in use:
1. Change port mappings in `docker-compose.yml`
2. Update any hardcoded references in scripts

## Development Workflow

### Making Changes to Worker

1. Rebuild the worker image:
   ```bash
   cd presto-native-execution
   docker compose build centos-native-runtime
   ```

2. Restart the worker:
   ```bash
   cd docker-setup
   docker compose restart presto-worker
   ```

### Adding New Catalogs

1. Add catalog properties to both:
   - `coordinator-config/catalog/`
   - `worker-config/catalog/`

2. Restart containers:
   ```bash
   docker compose restart
   ```

### Debugging

1. **Interactive shell in coordinator**:
   ```bash
   docker compose exec presto-coordinator bash
   ```

2. **Interactive shell in worker**:
   ```bash
   docker compose exec presto-worker bash
   ```

3. **View real-time logs**:
   ```bash
   docker compose logs -f presto-coordinator presto-worker
   ```

## Cleanup

```bash
# Stop containers
docker compose down

# Remove containers and networks
docker compose down --volumes

# Remove images (optional)
docker rmi presto/prestissimo-runtime:centos9
docker rmi prestodb/presto:latest
```

## Integration Development

For developing integrations with Prestissimo:

1. **Mount your source code**:
   ```yaml
   # Add to worker service in docker-compose.yml
   volumes:
     - ./worker-config:/opt/presto-server/etc
     - /path/to/your/integration:/opt/integration
   ```

2. **Rebuild with your changes**:
   ```bash
   # Modify the Dockerfile to include your integration
   docker compose build presto-worker
   docker compose restart presto-worker
   ```

3. **Test your integration**:
   ```bash
   # Use the CLI to test your new functionality
   docker compose exec presto-cli presto-cli --server presto-coordinator:8080
   ```

## Advanced Configuration

### Enabling Additional Features

To enable Parquet, S3, or other features, modify the worker build:

```bash
# In presto-native-execution directory
PRESTO_ENABLE_PARQUET=ON PRESTO_ENABLE_S3=ON docker compose build centos-native-runtime
```

### Multiple Workers

To add more workers, modify `docker-compose.yml`:

```yaml
presto-worker-2:
  extends: presto-worker
  container_name: presto-worker-2
  hostname: presto-worker-2
  ports:
    - "7778:7777"
  volumes:
    - ./worker-config-2:/opt/presto-server/etc
```

### Custom Images

To use custom images, update the image names in `docker-compose.yml` and ensure they're built or available 