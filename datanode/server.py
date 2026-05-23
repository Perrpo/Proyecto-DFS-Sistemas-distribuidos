"""
datanode/server.py
Servidor gRPC del DataNode.
Implementa DataNodeService: StoreBlock, RetrieveBlock, ReplicateBlock, DeleteBlock.
Día 2: StoreBlock/RetrieveBlock/ReplicateBlock completos con pipeline de replicación.
"""
import grpc
from concurrent import futures
import logging
import sys
import os
import hashlib

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

CHUNK_SIZE = 1 * 1024 * 1024   # 1 MB por chunk en streaming


class DataNodeServicer(dfs_pb2_grpc.DataNodeServiceServicer):

    def __init__(self, block_store: BlockStore, replication_agent: ReplicationAgent):
        self.block_store        = block_store
        self.replication_agent  = replication_agent

    # ══════════════════════════════════════════════════════════════════════════
    # StoreBlock – cliente → DataNode (client-streaming con pipeline)
    # ══════════════════════════════════════════════════════════════════════════
    def StoreBlock(self, request_iterator, context):
        """
        Recibe un bloque por streaming desde el cliente.
        Pipeline de replicación: al recibir el primer chunk reenvía al siguiente
        DataNode de la lista (si existe), de forma transparente.
        """
        block_id    = None
        checksum    = None
        pipeline    = []
        chunks      = []
        pipeline_stub   = None
        pipeline_channel = None

        try:
            for chunk in request_iterator:
                # Primer chunk: inicializar metadata y pipeline
                if block_id is None:
                    block_id = chunk.block_id
                    checksum = chunk.checksum
                    pipeline = list(chunk.pipeline)     # DataNodeInfo restantes
                    logger.info(
                        f"StoreBlock iniciado: block_id={block_id} "
                        f"pipeline_restante={[n.node_id for n in pipeline]}"
                    )

                    # Abrir canal al siguiente nodo del pipeline si existe
                    if pipeline:
                        next_node = pipeline[0]
                        remaining = pipeline[1:]
                        try:
                            pipeline_channel = grpc.insecure_channel(
                                f"{next_node.host}:{next_node.port}",
                                options=[
                                    ('grpc.max_send_message_length',    256 * 1024 * 1024),
                                    ('grpc.max_receive_message_length', 256 * 1024 * 1024),
                                ]
                            )
                            pipeline_stub = dfs_pb2_grpc.DataNodeServiceStub(pipeline_channel)
                        except Exception as e:
                            logger.error(f"No se pudo conectar al nodo pipeline {next_node.node_id}: {e}")
                            pipeline_stub = None

                chunks.append(chunk.data)

            if block_id is None:
                return dfs_pb2.StoreBlockResponse(
                    success=False, block_id="", message="Stream vacío"
                )

            # Ensamblar datos completos
            full_data = b"".join(chunks)

            # Verificar integridad SHA-256
            actual_checksum = hashlib.sha256(full_data).hexdigest()
            if checksum and actual_checksum != checksum:
                logger.error(
                    f"StoreBlock {block_id}: checksum mismatch "
                    f"esperado={checksum[:12]} actual={actual_checksum[:12]}"
                )
                return dfs_pb2.StoreBlockResponse(
                    success=False, block_id=block_id,
                    message=f"Error: checksum no coincide para bloque {block_id}"
                )

            # Almacenar en disco
            success, message = self.block_store.store_block(block_id, full_data)
            if not success:
                logger.error(f"StoreBlock {block_id}: fallo al guardar – {message}")
                return dfs_pb2.StoreBlockResponse(
                    success=False, block_id=block_id, message=message
                )

            logger.info(
                f"StoreBlock {block_id}: {len(full_data):,} bytes almacenados localmente"
            )

            # Replicar al siguiente nodo del pipeline (asíncrono en background)
            if pipeline and pipeline_stub is not None:
                next_node = pipeline[0]
                remaining = pipeline[1:]
                self.replication_agent.enqueue_pipeline(
                    block_id, full_data, checksum, next_node, remaining
                )

            return dfs_pb2.StoreBlockResponse(
                success=True, block_id=block_id,
                message=f"Bloque {block_id} almacenado correctamente"
            )

        except Exception as e:
            logger.error(f"StoreBlock error: {e}", exc_info=True)
            return dfs_pb2.StoreBlockResponse(
                success=False, block_id=block_id or "",
                message=f"Error interno: {e}"
            )
        finally:
            if pipeline_channel:
                pipeline_channel.close()

    # ══════════════════════════════════════════════════════════════════════════
    # RetrieveBlock – DataNode → cliente (server-streaming)
    # ══════════════════════════════════════════════════════════════════════════
    def RetrieveBlock(self, request, context):
        """
        Envía un bloque en chunks de 1 MB al cliente.
        Verifica integridad SHA-256 antes de enviar.
        """
        block_id = request.block_id
        logger.info(f"RetrieveBlock: solicitado block_id={block_id}")

        data, error = self.block_store.retrieve_block(block_id)
        if data is None:
            logger.error(f"RetrieveBlock {block_id}: no encontrado – {error}")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(error or f"Bloque no encontrado: {block_id}")
            return

        total   = len(data)
        offset  = 0
        chunk_i = 0

        while offset < total:
            end   = min(offset + CHUNK_SIZE, total)
            piece = data[offset:end]
            is_last = (end >= total)

            yield dfs_pb2.RetrieveBlockResponse(
                data=piece,
                chunk_index=chunk_i,
                is_last=is_last,
            )
            offset  += CHUNK_SIZE
            chunk_i += 1

        logger.info(
            f"RetrieveBlock {block_id}: enviado en {chunk_i} chunk(s) ({total:,} bytes)"
        )

    # ══════════════════════════════════════════════════════════════════════════
    # ReplicateBlock – NameNode → DataNode (orden de re-replicación)
    # ══════════════════════════════════════════════════════════════════════════
    def ReplicateBlock(self, request, context):
        """
        El NameNode ordena al DataNode replicar un bloque a otro destino.
        La replicación real se encola en el ReplicationAgent.
        """
        logger.info(
            f"ReplicateBlock {request.block_id} → "
            f"{request.destination.node_id} ({request.destination.host}:{request.destination.port})"
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
