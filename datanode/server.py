"""
datanode/server.py
Servidor gRPC del DataNode.
Implementa DataNodeService: StoreBlock, RetrieveBlock, ReplicateBlock, DeleteBlock.
Día 1: esqueleto operativo con DeleteBlock funcional.
Días 2-3: StoreBlock/RetrieveBlock/ReplicateBlock completos.
"""
import grpc
from concurrent import futures
import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import config

from proto import dfs_pb2, dfs_pb2_grpc
from block_store import BlockStore
from heartbeat_agent import HeartbeatAgent
from block_report_agent import BlockReportAgent
from replication_agent import ReplicationAgent

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s [%(name)-28s] %(levelname)-8s %(message)s',
)
logger = logging.getLogger('datanode.server')


class DataNodeServicer(dfs_pb2_grpc.DataNodeServiceServicer):

    def __init__(self, block_store: BlockStore, replication_agent: ReplicationAgent):
        self.block_store        = block_store
        self.replication_agent  = replication_agent

    # ══════════════════════════════════════════════════════════════════════════
    # StoreBlock – cliente → DataNode (client-streaming)
    # Implementación completa en Día 2
    # ══════════════════════════════════════════════════════════════════════════
    def StoreBlock(self, request_iterator, context):
        """
        Recibe un bloque por streaming desde el cliente.
        Pipeline de replicación: reenvía al siguiente DataNode de la lista.
        Implementación completa en Día 2.
        """
        logger.info("StoreBlock recibido (implementación completa en Día 2)")
        # Consumir el stream para no bloquear
        for _ in request_iterator:
            pass
        return dfs_pb2.StoreBlockResponse(
            success=False,
            message="StoreBlock: implementación completa disponible en Día 2"
        )

    # ══════════════════════════════════════════════════════════════════════════
    # RetrieveBlock – DataNode → cliente (server-streaming)
    # Implementación completa en Día 2
    # ══════════════════════════════════════════════════════════════════════════
    def RetrieveBlock(self, request, context):
        """
        Envía un bloque en chunks al cliente.
        Implementación completa en Día 2.
        """
        logger.info(f"RetrieveBlock {request.block_id} (implementación completa en Día 2)")
        yield dfs_pb2.RetrieveBlockResponse(data=b"", chunk_index=0, is_last=True)

    # ══════════════════════════════════════════════════════════════════════════
    # ReplicateBlock – DataNode → DataNode
    # Implementación completa en Día 2
    # ══════════════════════════════════════════════════════════════════════════
    def ReplicateBlock(self, request, context):
        """
        Ordena al DataNode replicar un bloque a otro destino.
        Implementación completa en Día 2.
        """
        logger.info(
            f"ReplicateBlock {request.block_id} → "
            f"{request.destination.node_id} (Día 2)"
        )
        self.replication_agent.enqueue(
            request.block_id,
            request.destination.host,
            request.destination.port,
            request.destination.node_id,
        )
        return dfs_pb2.ReplicateBlockResponse(
            success=True,
            message="Replicación encolada"
        )

    # ══════════════════════════════════════════════════════════════════════════
    # DeleteBlock – elimina un bloque local (FUNCIONAL desde Día 1)
    # ══════════════════════════════════════════════════════════════════════════
    def DeleteBlock(self, request, context):
        success, message = self.block_store.delete_block(request.block_id)
        logger.info(f"DeleteBlock {request.block_id}: {message}")
        return dfs_pb2.DeleteBlockResponse(success=success, message=message)


# ══════════════════════════════════════════════════════════════════════════════
def serve():
    node_id          = config.NODE_ID
    port             = config.DATANODE_PORT
    host             = config.DATANODE_HOST
    namenode_addr    = f"{config.NAMENODE_HOST}:{config.NAMENODE_PORT}"
    storage_path     = config.DATANODE_STORAGE_PATH

    # Inicializar componentes
    block_store        = BlockStore(storage_path)
    replication_agent  = ReplicationAgent(block_store, node_id)
    heartbeat_agent    = HeartbeatAgent(
        node_id=node_id,
        host=host,
        port=port,
        namenode_address=namenode_addr,
        block_store=block_store,
        replication_agent=replication_agent,
    )
    block_report_agent = BlockReportAgent(
        node_id=node_id,
        namenode_address=namenode_addr,
        block_store=block_store,
    )

    # Servidor gRPC
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ('grpc.max_send_message_length',    256 * 1024 * 1024),
            ('grpc.max_receive_message_length', 256 * 1024 * 1024),
        ]
    )
    dfs_pb2_grpc.add_DataNodeServiceServicer_to_server(
        DataNodeServicer(block_store, replication_agent), server
    )
    server.add_insecure_port(f'[::]:{port}')
    server.start()

    logger.info(
        f"DataNode '{node_id}' escuchando en puerto {port} | "
        f"NameNode: {namenode_addr} | Storage: {storage_path}"
    )

    # Iniciar agentes background
    replication_agent.start()
    heartbeat_agent.start()
    block_report_agent.start()

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info(f"DataNode '{node_id}' cerrando...")
        heartbeat_agent.stop()
        block_report_agent.stop()
        replication_agent.stop()
        server.stop(grace=5)


if __name__ == '__main__':
    serve()
