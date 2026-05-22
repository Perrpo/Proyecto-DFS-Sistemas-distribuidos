# ============================================================
# namenode/Dockerfile
# Imagen del NameNode – genera stubs proto en build time
# ============================================================
FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias primero (cache de capas)
COPY namenode/requirements.txt /app/namenode/requirements.txt
RUN pip install --no-cache-dir -r /app/namenode/requirements.txt

# Copiar fuentes
COPY proto/   /app/proto/
COPY config.py /app/
COPY namenode/ /app/namenode/

# Generar stubs gRPC dentro del contenedor
RUN python -m grpc_tools.protoc \
    -I /app \
    --python_out=/app \
    --grpc_python_out=/app \
    /app/proto/dfs.proto

# Directorio de trabajo y datos
WORKDIR /app/namenode
RUN mkdir -p /data

# Puerto gRPC
EXPOSE 50051

# Variables de entorno por defecto (sobrescribibles en docker-compose)
ENV NAMENODE_PORT=50051 \
    NAMENODE_DB_PATH=/data/namenode.db \
    BLOCK_SIZE=67108864 \
    REPLICATION_FACTOR=3 \
    HEARTBEAT_INTERVAL=10 \
    HEARTBEAT_TIMEOUT=30 \
    BLOCK_REPORT_INTERVAL=60 \
    LOG_LEVEL=INFO

CMD ["python", "server.py"]
