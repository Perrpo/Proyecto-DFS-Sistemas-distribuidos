# ============================================================
# datanode/Dockerfile
# Imagen del DataNode – genera stubs proto en build time
# ============================================================
FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias
COPY datanode/requirements.txt /app/datanode/requirements.txt
RUN pip install --no-cache-dir -r /app/datanode/requirements.txt

# Copiar fuentes
COPY proto/    /app/proto/
COPY config.py /app/
COPY datanode/ /app/datanode/

# Generar stubs gRPC
RUN python -m grpc_tools.protoc \
    -I /app \
    --python_out=/app \
    --grpc_python_out=/app \
    /app/proto/dfs.proto

# Directorio de almacenamiento de bloques
WORKDIR /app/datanode
RUN mkdir -p /data/blocks

# Puerto gRPC (cada instancia lo sobreescribe via env)
EXPOSE 50061

# Variables de entorno por defecto
ENV NODE_ID=dn1 \
    NAMENODE_HOST=namenode \
    NAMENODE_PORT=50051 \
    DATANODE_HOST=datanode1 \
    DATANODE_PORT=50061 \
    DATANODE_STORAGE_PATH=/data/blocks \
    HEARTBEAT_INTERVAL=10 \
    HEARTBEAT_TIMEOUT=30 \
    BLOCK_REPORT_INTERVAL=60 \
    LOG_LEVEL=INFO

CMD ["python", "server.py"]
