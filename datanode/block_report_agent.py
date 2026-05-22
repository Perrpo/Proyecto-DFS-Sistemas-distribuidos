"""
datanode/block_report_agent.py
Thread daemon que envía el inventario completo de bloques al NameNode
cada BLOCK_REPORT_INTERVAL segundos.
"""
import threading
import grpc
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import config
from proto import dfs_pb2, dfs_pb2_grpc

logger = logging.getLogger('datanode.block_report_agent')


class BlockReportAgent(threading.Thread):

    def __init__(self, node_id: str, namenode_address: str, block_store):
        super().__init__(daemon=True, name=f'BlockReportAgent-{node_id}')
        self.node_id          = node_id
        self.namenode_address = namenode_address
        self.block_store      = block_store
        self._stop_event      = threading.Event()

    def run(self):
        # Esperar un poco para que el NameNode registre el heartbeat inicial
        self._stop_event.wait(10)
        logger.info(
            f"BlockReportAgent iniciado (intervalo={config.BLOCK_REPORT_INTERVAL}s)"
        )
        while not self._stop_event.is_set():
            try:
                self._send_report()
            except grpc.RpcError as e:
                logger.warning(f"BlockReport RPC fallido: {e.code()} – {e.details()}")
            except Exception as e:
                logger.warning(f"BlockReport error: {e}")
            self._stop_event.wait(config.BLOCK_REPORT_INTERVAL)

    def stop(self):
        self._stop_event.set()

    def _send_report(self):
        blocks     = self.block_store.list_blocks()
        block_msgs = [
            dfs_pb2.BlockInfo(
                block_id=b['block_id'],
                block_size=b['block_size'],
                checksum=b['checksum'],
            ) for b in blocks
        ]

        channel  = grpc.insecure_channel(self.namenode_address)
        stub     = dfs_pb2_grpc.NameNodeServiceStub(channel)
        response = stub.BlockReport(
            dfs_pb2.BlockReportRequest(node_id=self.node_id, blocks=block_msgs),
            timeout=30,
        )
        channel.close()

        logger.info(f"BlockReport enviado: {len(blocks)} bloques")

        # Procesar eliminaciones ordenadas por NameNode
        for block_id in response.blocks_to_delete:
            self.block_store.delete_block(block_id)
            logger.info(f"Bloque eliminado (BlockReport): {block_id}")
