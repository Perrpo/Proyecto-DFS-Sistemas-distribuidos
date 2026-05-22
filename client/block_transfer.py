"""
client/block_transfer.py
Transferencia de bloques entre cliente y DataNodes vía gRPC streaming.
Incluye reintentos con backoff exponencial y fallback a réplicas alternativas.
Implementación completa en Día 2.
Día 1: esqueleto con firmas correctas.
"""
import time
import logging

logger = logging.getLogger('client.block_transfer')

MAX_RETRIES = 3
RETRY_DELAYS = [1, 2, 4]   # segundos (backoff exponencial)


def upload_block(datanode_host: str, port: int, block_id: str,
                 data: bytes, checksum: str, pipeline_nodes: list = None):
    """
    Sube un bloque a un DataNode vía gRPC client-streaming.
    Implementa pipeline de replicación si pipeline_nodes no está vacío.

    Args:
        datanode_host:  Host del DataNode primario
        port:           Puerto gRPC del DataNode
        block_id:       UUID del bloque
        data:           Datos del bloque
        checksum:       SHA-256 del bloque
        pipeline_nodes: Lista de DataNodeInfo para replicación pipeline

    Returns:
        bool: True si el bloque fue almacenado exitosamente

    Implementación completa en Día 2.
    """
    raise NotImplementedError("upload_block: implementación completa en Día 2")


def download_block(datanode_host: str, port: int, block_id: str,
                   expected_checksum: str) -> bytes:
    """
    Descarga un bloque de un DataNode vía gRPC server-streaming.
    Verifica integridad con SHA-256.
    Reintentos con backoff exponencial ante fallos transitorios.

    Args:
        datanode_host:     Host del DataNode
        port:              Puerto gRPC
        block_id:          UUID del bloque
        expected_checksum: SHA-256 esperado para verificación

    Returns:
        bytes: Datos del bloque verificados

    Raises:
        IOError: Si el bloque está corrupto o no disponible tras todos los reintentos

    Implementación completa en Día 2.
    """
    raise NotImplementedError("download_block: implementación completa en Día 2")
