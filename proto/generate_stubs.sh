#!/bin/bash
# generate_stubs.sh – Genera los stubs gRPC desde dfs.proto (Linux/Mac)
# Requiere: pip install grpcio-tools

echo "Generando stubs de Protocol Buffers..."

python -m grpc_tools.protoc \
    -I. \
    --python_out=. \
    --grpc_python_out=. \
    proto/dfs.proto

if [ $? -eq 0 ]; then
    echo "Stubs generados exitosamente en proto/"
    echo "  - proto/dfs_pb2.py"
    echo "  - proto/dfs_pb2_grpc.py"
else
    echo "Error generando stubs. Instala grpcio-tools:"
    echo "  pip install grpcio-tools==1.62.2"
    exit 1
fi
