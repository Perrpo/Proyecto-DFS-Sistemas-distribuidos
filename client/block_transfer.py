"""
client/block_transfer.py
Transferencia de bloques entre cliente y DataNodes vía gRPC streaming.
Día 2: implementación completa con backoff exponencial y fallback a réplicas alternativas.
"""
import time
import logging
import hashlib
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import config

from proto import dfs_pb2, dfs_pb2_grpc
import grpc

logger = logging.getLogger('client.block_transfer')

MAX_RETRIES  = 3
RETRY_DELAYS = [1, 2, 4]   # segundos (backoff exponencial)
CHUNK_SIZE   = 1 * 1024 * 1024  # 1 MB por chunk


# ══════════════════════════════════════════════════════════════════════════════
# Helpers internos
# ══════════════════════════════════════════════════════════════════════════════

def _grpc_channel(host: str, port: int):
    """Crea un canal gRPC con límites de mensaje grandes."""
    return grpc.insecure_channel(
        f"{host}:{port}",
        options=[
            ('grpc.max_send_message_length',    256 * 1024 * 1024),
            ('grpc.max_receive_message_length', 256 * 1024 * 1024),
        ]
    )


def _make_store_stream(block_id: str, data: bytes, checksum: str,
                       pipeline_nodes: list):
    """
    Generador de StoreBlockRequest para client-streaming.
    Primer chunk incluye block_id, checksum y pipeline; el resto solo datos.
    """
    offset  = 0
    chunk_i = 0
    while offset < len(data):
        end   = min(offset + CHUNK_SIZE, len(data))
        piece = data[offset:end]
        req   = dfs_pb2.StoreBlockRequest(
            block_id    = block_id,
            data        = piece,
            chunk_index = chunk_i,
            checksum    = checksum if chunk_i == 0 else "",
            pipeline    = [
                dfs_pb2.DataNodeInfo(
                    node_id=n.node_id, host=n.host, port=n.port
                ) for n in pipeline_nodes
            ] if chunk_i == 0 else [],
        )
        yield req
        offset  += CHUNK_SIZE
        chunk_i += 1


# ══════════════════════════════════════════════════════════════════════════════
# API pública
# ══════════════════════════════════════════════════════════════════════════════

def upload_block(datanode_host: str, port: int, block_id: str,
                 data: bytes, checksum: str, pipeline_nodes: list = None) -> bool:
    """
    Sube un bloque a un DataNode vía gRPC client-streaming.
    Implementa pipeline de replicación si pipeline_nodes no está vacío.

    Args:
        datanode_host:  Host del DataNode primario
        port:           Puerto gRPC del DataNode
        block_id:       UUID del bloque
        data:           Datos del bloque (bytes completos)
        checksum:       SHA-256 del bloque
        pipeline_nodes: Lista de DataNodeInfo para replicación pipeline (puede ser [])

    Returns:
        bool: True si el bloque fue almacenado exitosamente

    Raises:
        IOError: Si el bloque no pudo subirse tras todos los reintentos
    """
    pipeline_nodes = pipeline_nodes or []

    for attempt in range(MAX_RETRIES):
        channel = None
        try:
            channel  = _grpc_channel(datanode_host, port)
            stub     = dfs_pb2_grpc.DataNodeServiceStub(channel)
            response = stub.StoreBlock(
                _make_store_stream(block_id, data, checksum, pipeline_nodes),
                timeout=120,
            )
            if response.success:
                logger.info(
                    f"upload_block OK: {block_id} → {datanode_host}:{port} "
                    f"({len(data):,} bytes, pipeline={[n.node_id for n in pipeline_nodes]})"
                )
                return True
            else:
                logger.warning(
                    f"upload_block (intento {attempt+1}): {block_id} → {datanode_host}:{port} "
                    f"falló: {response.message}"
                )
        except grpc.RpcError as e:
            logger.error(
                f"upload_block RPC (intento {attempt+1}/{MAX_RETRIES}): "
                f"{block_id} → {datanode_host}:{port}: {e.code()} – {e.details()}"
            )
        except Exception as e:
            logger.error(
                f"upload_block error (intento {attempt+1}/{MAX_RETRIES}): "
                f"{block_id}: {e}"
            )
        finally:
            if channel:
                channel.close()

        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_DELAYS[attempt])

    raise IOError(
        f"upload_block: bloque {block_id} no pudo subirse a {datanode_host}:{port} "
        f"después de {MAX_RETRIES} intentos"
    )


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
        expected_checksum: SHA-256 esperado para verificación de integridad

    Returns:
        bytes: Datos del bloque verificados

    Raises:
        IOError: Si el bloque está corrupto o no disponible tras todos los reintentos
    """
    for attempt in range(MAX_RETRIES):
        channel = None
        try:
            channel = _grpc_channel(datanode_host, port)
            stub    = dfs_pb2_grpc.DataNodeServiceStub(channel)
            chunks  = []

            for response in stub.RetrieveBlock(
                dfs_pb2.RetrieveBlockRequest(block_id=block_id),
                timeout=120,
            ):
                chunks.append(response.data)

            data            = b"".join(chunks)
            actual_checksum = hashlib.sha256(data).hexdigest()

            if expected_checksum and actual_checksum != expected_checksum:
                logger.error(
                    f"download_block: checksum mismatch para {block_id} en {datanode_host}:{port} "
                    f"(esperado={expected_checksum[:12]} actual={actual_checksum[:12]})"
                )
                # Bloque corrupto: no reintentar en este nodo
                raise IOError(
                    f"Bloque corrupto: {block_id} en {datanode_host}:{port} (checksum mismatch)"
                )

            logger.info(
                f"download_block OK: {block_id} ← {datanode_host}:{port} ({len(data):,} bytes)"
            )
            return data

        except IOError:
            # Error de checksum: propagar sin reintentar en el mismo nodo
            raise
        except grpc.RpcError as e:
            logger.error(
                f"download_block RPC (intento {attempt+1}/{MAX_RETRIES}): "
                f"{block_id} ← {datanode_host}:{port}: {e.code()} – {e.details()}"
            )
        except Exception as e:
            logger.error(
                f"download_block error (intento {attempt+1}/{MAX_RETRIES}): "
                f"{block_id}: {e}"
            )
        finally:
            if channel:
                channel.close()

        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_DELAYS[attempt])

    raise IOError(
        f"download_block: bloque {block_id} no disponible en {datanode_host}:{port} "
        f"después de {MAX_RETRIES} intentos"
    )
