"""
datanode/heartbeat_agent.py
Thread daemon que envía heartbeats periódicos al NameNode.
También procesa instrucciones de re-replicación y eliminación devueltas en HeartbeatResponse.
"""
import threading
import grpc
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import config
from proto import dfs_pb2, dfs_pb2_grpc

logger = logging.getLogger('datanode.heartbeat_agent')


class HeartbeatAgent(threading.Thread):

    def __init__(self, node_id, host, port, namenode_address, block_store,
                 replication_agent=None):
        super().__init__(daemon=True, name=f'HeartbeatAgent-{node_id}')
        self.node_id            = node_id
        self.host               = host
        self.port               = port
        self.namenode_address   = namenode_address
        self.block_store        = block_store
        self.replication_agent  = replication_agent   # inyectado en Día 2
        self._stop_event        = threading.Event()
        self._stub              = None
        self._channel           = None

    def run(self):
        logger.info(
            f"HeartbeatAgent iniciado → {self.namenode_address} "
            f"(intervalo={config.HEARTBEAT_INTERVAL}s)"
        )
        while not self._stop_event.is_set():
            try:
                self._send()
            except grpc.RpcError as e:
                logger.warning(f"Heartbeat RPC fallido: {e.code()} – {e.details()}")
                self._stub = None
            except Exception as e:
                logger.warning(f"Heartbeat error: {e}")
                self._stub = None
            self._stop_event.wait(config.HEARTBEAT_INTERVAL)

    def stop(self):
        self._stop_event.set()
        if self._channel:
            self._channel.close()

    # ─── Privado ───────────────────────────────────────────────────────────────
    def _get_stub(self):
        if self._stub is None:
            self._channel = grpc.insecure_channel(self.namenode_address)
            self._stub    = dfs_pb2_grpc.NameNodeServiceStub(self._channel)
        return self._stub

    def _send(self):
        available = self.block_store.get_available_space()
        count     = len(self.block_store.list_blocks())   # lista ligera

        response = self._get_stub().Heartbeat(
            dfs_pb2.HeartbeatRequest(
                node_id=self.node_id,
                host=self.host,
                port=self.port,
                available_space=available,
                block_count=count,
            ),
            timeout=5,
        )

        logger.debug(f"Heartbeat ACK: {response.acknowledged}")

        # Procesar eliminaciones
        for block_id in response.blocks_to_delete:
            self.block_store.delete_block(block_id)
            logger.info(f"Bloque eliminado por orden del NameNode: {block_id}")

        # Procesar tareas de replicación (implementado en Día 2)
        for task in response.replication_tasks:
            logger.info(
                f"Tarea de re-replicación recibida: "
                f"{task.block_id} → {task.destination.node_id}"
            )
            if self.replication_agent:
                self.replication_agent.enqueue(
                    task.block_id,
                    task.destination.host,
                    task.destination.port,
                    task.destination.node_id,
                )
