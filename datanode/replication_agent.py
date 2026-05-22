"""
datanode/replication_agent.py
Agente de replicación pipeline: envía bloques a otros DataNodes.
Implementación completa en Día 2.
Día 1: esqueleto con cola de tareas funcional.
"""
import threading
import queue
import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

logger = logging.getLogger('datanode.replication_agent')


class ReplicationAgent(threading.Thread):
    """
    Procesa tareas de replicación en background.
    Cada tarea = (block_id, dest_host, dest_port, dest_node_id).
    """

    def __init__(self, block_store, node_id: str):
        super().__init__(daemon=True, name=f'ReplicationAgent-{node_id}')
        self.block_store = block_store
        self.node_id     = node_id
        self._queue      = queue.Queue()
        self._stop_event = threading.Event()

    def enqueue(self, block_id: str, dest_host: str, dest_port: int, dest_node_id: str):
        """Agrega una tarea de replicación a la cola."""
        logger.info(
            f"Replicación encolada: {block_id} → {dest_node_id} ({dest_host}:{dest_port})"
        )
        self._queue.put((block_id, dest_host, dest_port, dest_node_id))

    def run(self):
        logger.info("ReplicationAgent iniciado (Día 2: implementación completa)")
        while not self._stop_event.is_set():
            try:
                task = self._queue.get(timeout=1)
                block_id, dest_host, dest_port, dest_node_id = task
                self._replicate(block_id, dest_host, dest_port, dest_node_id)
                self._queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error en replicación: {e}", exc_info=True)

    def stop(self):
        self._stop_event.set()

    def _replicate(self, block_id: str, dest_host: str, dest_port: int, dest_node_id: str):
        """
        Replicar un bloque a otro DataNode vía gRPC streaming.
        Implementación completa en Día 2.
        """
        logger.info(
            f"[TODO Día 2] Replicar {block_id} → {dest_node_id} ({dest_host}:{dest_port})"
        )
        # Implementación completa en Día 2:
        # 1. Leer bloque de block_store
        # 2. Abrir canal gRPC al DataNode destino
        # 3. Llamar ReplicateBlock en streaming
        # 4. Verificar confirmación
