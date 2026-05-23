"""
datanode/replication_agent.py
Agente de replicación pipeline: envía bloques a otros DataNodes vía gRPC streaming.
Día 2: implementación completa de _replicate() y enqueue_pipeline().
"""
import threading
import queue
import logging
import sys
import os
import time
import hashlib

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import config

logger = logging.getLogger('datanode.replication_agent')

CHUNK_SIZE  = 1 * 1024 * 1024  # 1 MB
MAX_RETRIES = 3
RETRY_DELAYS = [1, 2, 4]        # backoff exponencial (segundos)


class ReplicationAgent(threading.Thread):
    """
    Procesa tareas de replicación en background.
    Dos tipos de tarea:
      - 'replicate': (block_id, dest_host, dest_port, dest_node_id) – re-replicación por orden del NameNode
      - 'pipeline':  (block_id, data, checksum, next_node_info, remaining_pipeline) – pipeline inline
    """

    def __init__(self, block_store, node_id: str):
        super().__init__(daemon=True, name=f'ReplicationAgent-{node_id}')
        self.block_store = block_store
        self.node_id     = node_id
        self._queue      = queue.Queue()
        self._stop_event = threading.Event()

    def enqueue(self, block_id: str, dest_host: str, dest_port: int, dest_node_id: str):
        """Encola una tarea de re-replicación (order from NameNode)."""
        logger.info(
            f"Replicación encolada: {block_id} → {dest_node_id} ({dest_host}:{dest_port})"
        )
        self._queue.put(('replicate', block_id, dest_host, dest_port, dest_node_id))

    def enqueue_pipeline(self, block_id: str, data: bytes, checksum: str,
                         next_node, remaining_pipeline: list):
        """Encola un paso de replicación pipeline tras recibir un bloque nuevo."""
        logger.info(
            f"Pipeline encolado: {block_id} → {next_node.node_id} "
            f"(restantes: {[n.node_id for n in remaining_pipeline]})"
        )
        self._queue.put(('pipeline', block_id, data, checksum, next_node, remaining_pipeline))

    def run(self):
        logger.info("ReplicationAgent iniciado")
        while not self._stop_event.is_set():
            try:
                task = self._queue.get(timeout=1)
                kind = task[0]
                if kind == 'replicate':
                    _, block_id, dest_host, dest_port, dest_node_id = task
                    self._replicate(block_id, dest_host, dest_port, dest_node_id)
                elif kind == 'pipeline':
                    _, block_id, data, checksum, next_node, remaining = task
                    self._pipeline_forward(block_id, data, checksum, next_node, remaining)
                self._queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error en replicación: {e}", exc_info=True)

    def stop(self):
        self._stop_event.set()

    # ─── Helpers internos ──────────────────────────────────────────────────────

    def _make_stream_for_pipeline(self, block_id: str, data: bytes, checksum: str,
                                   remaining_pipeline: list):
        """Genera el stream StoreBlockRequest para enviar al siguiente nodo."""
        from proto import dfs_pb2
        offset = 0
        chunk_i = 0
        while offset < len(data):
            end   = min(offset + CHUNK_SIZE, len(data))
            piece = data[offset:end]
            req = dfs_pb2.StoreBlockRequest(
                block_id    = block_id,
                data        = piece,
                chunk_index = chunk_i,
                checksum    = checksum if chunk_i == 0 else "",
                pipeline    = remaining_pipeline if chunk_i == 0 else [],
            )
            yield req
            offset  += CHUNK_SIZE
            chunk_i += 1

    def _pipeline_forward(self, block_id: str, data: bytes, checksum: str,
                          next_node, remaining_pipeline: list):
        """Reenvía un bloque al siguiente nodo del pipeline por gRPC streaming."""
        import grpc
        from proto import dfs_pb2_grpc

        for attempt in range(MAX_RETRIES):
            channel = None
            try:
                channel = grpc.insecure_channel(
                    f"{next_node.host}:{next_node.port}",
                    options=[
                        ('grpc.max_send_message_length',    256 * 1024 * 1024),
                        ('grpc.max_receive_message_length', 256 * 1024 * 1024),
                    ]
                )
                stub     = dfs_pb2_grpc.DataNodeServiceStub(channel)
                response = stub.StoreBlock(
                    self._make_stream_for_pipeline(block_id, data, checksum, remaining_pipeline),
                    timeout=120,
                )
                if response.success:
                    logger.info(
                        f"Pipeline: {block_id} replicado exitosamente en {next_node.node_id}"
                    )
                    return
                else:
                    logger.warning(
                        f"Pipeline: {block_id} → {next_node.node_id} falló: {response.message}"
                    )
            except Exception as e:
                logger.error(
                    f"Pipeline (intento {attempt+1}/{MAX_RETRIES}): "
                    f"{block_id} → {next_node.node_id}: {e}"
                )
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAYS[attempt])
            finally:
                if channel:
                    channel.close()

        logger.error(
            f"Pipeline: {block_id} → {next_node.node_id} falló después de {MAX_RETRIES} intentos"
        )

    def _replicate(self, block_id: str, dest_host: str, dest_port: int, dest_node_id: str):
        """
        Re-replicar un bloque por orden del NameNode (HeartbeatResponse).
        Lee el bloque del block_store local y lo envía en streaming al destino.
        """
        import grpc
        from proto import dfs_pb2, dfs_pb2_grpc

        # Leer bloque local
        data, error = self.block_store.retrieve_block(block_id)
        if data is None:
            logger.error(f"_replicate: bloque {block_id} no encontrado localmente: {error}")
            return

        checksum = hashlib.sha256(data).hexdigest()

        for attempt in range(MAX_RETRIES):
            channel = None
            try:
                channel = grpc.insecure_channel(
                    f"{dest_host}:{dest_port}",
                    options=[
                        ('grpc.max_send_message_length',    256 * 1024 * 1024),
                        ('grpc.max_receive_message_length', 256 * 1024 * 1024),
                    ]
                )
                stub     = dfs_pb2_grpc.DataNodeServiceStub(channel)
                response = stub.StoreBlock(
                    self._make_stream_for_pipeline(block_id, data, checksum, []),
                    timeout=120,
                )
                if response.success:
                    logger.info(
                        f"Re-replicación OK: {block_id} → {dest_node_id} ({dest_host}:{dest_port})"
                    )
                    return
                else:
                    logger.warning(
                        f"Re-replicación: {block_id} → {dest_node_id} falló: {response.message}"
                    )
            except Exception as e:
                logger.error(
                    f"Re-replicación (intento {attempt+1}/{MAX_RETRIES}): "
                    f"{block_id} → {dest_node_id}: {e}"
                )
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAYS[attempt])
            finally:
                if channel:
                    channel.close()

        logger.error(
            f"Re-replicación fallida definitivamente: {block_id} → {dest_node_id} "
            f"después de {MAX_RETRIES} intentos"
        )
