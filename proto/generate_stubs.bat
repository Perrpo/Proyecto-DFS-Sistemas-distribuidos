@echo off
REM generate_stubs.bat – Genera los stubs gRPC desde dfs.proto (Windows)
REM Requiere: pip install grpcio-tools

echo Generando stubs de Protocol Buffers...

python -m grpc_tools.protoc ^
    -I. ^
    --python_out=. ^
    --grpc_python_out=. ^
    proto/dfs.proto

if %ERRORLEVEL% EQU 0 (
    echo Stubs generados exitosamente en proto/
    echo   - proto/dfs_pb2.py
    echo   - proto/dfs_pb2_grpc.py
) else (
    echo Error generando stubs. Asegurate de tener grpcio-tools instalado:
    echo   pip install grpcio-tools==1.62.2
)
