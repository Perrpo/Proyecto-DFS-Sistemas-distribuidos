"""
namenode/server.py
Servidor gRPC del NameNode – punto de entrada principal.
Implementa todos los RPCs de NameNodeService.
"""
import grpc
from concurrent import futures
import logging
import uuid
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import config

from proto import dfs_pb2, dfs_pb2_grpc
from metadata_store import MetadataStore
from auth_service import AuthService
from namespace_manager import NamespaceManager
from block_manager import BlockManager
from heartbeat_monitor import HeartbeatMonitor

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s [%(name)-28s] %(levelname)-8s %(message)s',
)
logger = logging.getLogger('namenode.server')


class NameNodeServicer(dfs_pb2_grpc.NameNodeServiceServicer):

    def __init__(self):
        self.store     = MetadataStore(config.NAMENODE_DB_PATH)
        self.auth      = AuthService(self.store)
        self.namespace = NamespaceManager(self.store)
        self.blocks    = BlockManager(self.store, config.REPLICATION_FACTOR)
        self.monitor   = HeartbeatMonitor(self.store, self.blocks)
        self.monitor.start()
        logger.info("NameNode inicializado y listo.")

    # ─── Utilidad privada ──────────────────────────────────────────────────────
    def _validate(self, token):
        return self.auth.validate_token(token)

    # ══════════════════════════════════════════════════════════════════════════
    # Auth
    # ══════════════════════════════════════════════════════════════════════════
    def Register(self, request, context):
        success, message = self.auth.register_user(request.username, request.password)
        logger.info(f"Register {request.username}: {'OK' if success else message}")
        return dfs_pb2.RegisterResponse(success=success, message=message)

    def Login(self, request, context):
        success, result = self.auth.authenticate(request.username, request.password)
        logger.info(f"Login {request.username}: {'OK' if success else 'FAIL'}")
        return dfs_pb2.LoginResponse(
            success=success,
            token=result if success else "",
            message="" if success else result,
        )

    # ══════════════════════════════════════════════════════════════════════════
    # Operaciones de archivos
    # ══════════════════════════════════════════════════════════════════════════
    def PutFile(self, request, context):
        valid, user_id = self._validate(request.token)
        if not valid:
            return dfs_pb2.PutFileResponse(success=False, message=user_id)

        # Validar path
        ok, filepath = self.namespace.validate_filepath(user_id, request.filepath)
        if not ok:
            return dfs_pb2.PutFileResponse(success=False, message=filepath)

        # ¿Ya existe?
        if self.store.get_file(user_id, filepath):
            return dfs_pb2.PutFileResponse(
                success=False,
                message=f"Error: archivo ya existe: {filepath}"
            )

        # Asignar DataNodes
        try:
            node_assignments = self.blocks.assign_datanodes_for_upload(request.block_count)
        except ValueError as e:
            return dfs_pb2.PutFileResponse(success=False, message=str(e))

        # Crear registro de archivo
        file_id = str(uuid.uuid4())
        self.store.create_file(
            file_id, user_id, filepath,
            request.filesize, request.block_count, config.BLOCK_SIZE
        )

        # Crear asignaciones de bloques (con checksum pendiente)
        assignments = []
        for i, nodes in enumerate(node_assignments):
            block_id = str(uuid.uuid4())
            self.store.create_block(block_id, file_id, i, 0, 'pending')

            assignments.append(dfs_pb2.BlockAssignment(
                block_id=block_id,
                block_index=i,
                block_size=config.BLOCK_SIZE,
                datanodes=[
                    dfs_pb2.DataNodeInfo(
                        node_id=n['node_id'], host=n['host'], port=n['port']
                    ) for n in nodes
                ],
            ))

        logger.info(f"PutFile: {filepath} ({request.block_count} bloques) usuario={user_id}")
        return dfs_pb2.PutFileResponse(
            success=True, file_id=file_id, assignments=assignments
        )

    def GetFile(self, request, context):
        valid, user_id = self._validate(request.token)
        if not valid:
            return dfs_pb2.GetFileResponse(success=False, message=user_id)

        file_info = self.store.get_file(user_id, request.filepath)
        if not file_info:
            return dfs_pb2.GetFileResponse(
                success=False,
                message=f"Error: archivo no existe: {request.filepath}"
            )

        locations_data = self.blocks.get_block_locations_for_download(file_info['file_id'])
        block_locations = []

        for item in locations_data:
            block = item['block']
            locs  = item['locations']
            if not locs:
                return dfs_pb2.GetFileResponse(
                    success=False,
                    message=f"Error: bloque no disponible en ningún nodo: {block['block_id']}"
                )
            block_locations.append(dfs_pb2.BlockLocation(
                block_id=block['block_id'],
                block_index=block['block_index'],
                block_size=block['block_size'],
                checksum=block['checksum'],
                datanodes=[
                    dfs_pb2.DataNodeInfo(
                        node_id=loc['node_id'], host=loc['host'], port=loc['port']
                    ) for loc in locs
                ],
            ))

        logger.info(f"GetFile: {request.filepath} usuario={user_id}")
        return dfs_pb2.GetFileResponse(
            success=True,
            file_id=file_info['file_id'],
            filesize=file_info['filesize'],
            locations=block_locations,
        )

    def DeleteFile(self, request, context):
        valid, user_id = self._validate(request.token)
        if not valid:
            return dfs_pb2.DeleteFileResponse(success=False, message=user_id)

        file_info = self.store.get_file(user_id, request.filepath)
        if not file_info:
            return dfs_pb2.DeleteFileResponse(
                success=False,
                message=f"Error: archivo no encontrado: {request.filepath}"
            )

        self.store.mark_file_deleted(user_id, request.filepath)
        self.store.delete_blocks_for_file(file_info['file_id'])
        logger.info(f"DeleteFile: {request.filepath} usuario={user_id}")
        return dfs_pb2.DeleteFileResponse(success=True, message="Archivo eliminado exitosamente")

    def ListFiles(self, request, context):
        valid, user_id = self._validate(request.token)
        if not valid:
            return dfs_pb2.ListFilesResponse(success=False, message=user_id)

        directory = request.directory if request.directory else '/'
        success, files, dirs, message = self.namespace.list_dir(user_id, directory)
        if not success:
            return dfs_pb2.ListFilesResponse(success=False, message=message)

        return dfs_pb2.ListFilesResponse(
            success=True,
            files=[
                dfs_pb2.FileInfo(
                    filepath=f['filepath'], filesize=f['filesize'],
                    block_count=f['block_count'], created_at=f['created_at'],
                    status=f['status'],
                ) for f in files
            ],
            directories=[d['path'] for d in dirs],
        )

    # ══════════════════════════════════════════════════════════════════════════
    # Namespace
    # ══════════════════════════════════════════════════════════════════════════
    def MakeDir(self, request, context):
        valid, user_id = self._validate(request.token)
        if not valid:
            return dfs_pb2.MakeDirResponse(success=False, message=user_id)
        success, msg = self.namespace.make_dir(user_id, request.path)
        return dfs_pb2.MakeDirResponse(success=success, message=msg)

    def RemoveDir(self, request, context):
        valid, user_id = self._validate(request.token)
        if not valid:
            return dfs_pb2.RemoveDirResponse(success=False, message=user_id)
        success, msg = self.namespace.remove_dir(user_id, request.path, request.recursive)
        return dfs_pb2.RemoveDirResponse(success=success, message=msg)

    def GetClusterStatus(self, request, context):
        valid, user_id = self._validate(request.token)
        if not valid:
            return dfs_pb2.ClusterStatusResponse(success=False, message=user_id)

        datanodes = self.store.get_all_datanodes()
        total_blocks = self.store.get_total_block_count()
        total_files  = self.store.get_total_file_count()

        dn_statuses = [
            dfs_pb2.DataNodeStatus(
                node_id         = dn['node_id'],
                host            = dn['host'],
                port            = dn['port'],
                status          = dn['status'],
                block_count     = dn['block_count'],
                available_space = dn['available_space'],
                last_heartbeat  = dn['last_heartbeat'],
            )
            for dn in datanodes
        ]

        logger.info(f"GetClusterStatus: {len(datanodes)} DataNodes, {total_blocks} bloques, {total_files} archivos")
        return dfs_pb2.ClusterStatusResponse(
            success      = True,
            datanodes    = dn_statuses,
            total_blocks = total_blocks,
            total_files  = total_files,
        )

    # ══════════════════════════════════════════════════════════════════════════
    # DataNode → NameNode
    # ══════════════════════════════════════════════════════════════════════════
    def Heartbeat(self, request, context):
        self.store.upsert_datanode(
            request.node_id, request.host, request.port,
            request.available_space, request.block_count,
        )
        logger.debug(
            f"Heartbeat {request.node_id}: {request.available_space // (1024**3):.1f} GB libres"
        )

        # Entregar tareas de re-replicación pendientes
        tasks = self.monitor.pop_tasks_for_node(request.node_id)
        replication_tasks = [
            dfs_pb2.ReplicationTask(
                block_id=t['block_id'],
                source=dfs_pb2.DataNodeInfo(
                    node_id=t['source']['node_id'],
                    host=t['source']['host'],
                    port=t['source']['port'],
                ),
                destination=dfs_pb2.DataNodeInfo(
                    node_id=t['destination']['node_id'],
                    host=t['destination']['host'],
                    port=t['destination']['port'],
                ),
            ) for t in tasks
        ]

        return dfs_pb2.HeartbeatResponse(
            acknowledged=True,
            blocks_to_delete=[],
            replication_tasks=replication_tasks,
        )

    def BlockReport(self, request, context):
        blocks = [
            {'block_id': b.block_id, 'block_size': b.block_size, 'checksum': b.checksum}
            for b in request.blocks
        ]
        self.store.reconcile_block_report(request.node_id, blocks)
        logger.info(f"BlockReport de {request.node_id}: {len(blocks)} bloques")
        return dfs_pb2.BlockReportResponse(acknowledged=True, blocks_to_delete=[])

    # ══════════════════════════════════════════════════════════════════════════
    # Confirmación y reporte de corrupción
    # ══════════════════════════════════════════════════════════════════════════
    def ConfirmBlock(self, request, context):
        valid, user_id = self._validate(request.token)
        if not valid:
            return dfs_pb2.ConfirmBlockResponse(success=False, message=user_id)

        self.blocks.confirm_block(
            request.block_id, request.block_size,
            request.checksum, list(request.node_ids),
        )
        logger.info(
            f"ConfirmBlock: {request.block_id} "
            f"({request.block_size} bytes) en {list(request.node_ids)}"
        )
        return dfs_pb2.ConfirmBlockResponse(success=True, message="Bloque confirmado")

    def ReportCorruptBlock(self, request, context):
        valid, user_id = self._validate(request.token)
        if not valid:
            return dfs_pb2.ReportCorruptBlockResponse(success=False, message=user_id)

        self.store.remove_block_location(request.block_id, request.node_id)
        logger.warning(
            f"Bloque corrupto reportado: {request.block_id} en {request.node_id}. "
            f"Ubicación eliminada; se activará re-replicación."
        )
        return dfs_pb2.ReportCorruptBlockResponse(
            success=True, message="Bloque corrupto registrado"
        )


# ══════════════════════════════════════════════════════════════════════════════
def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=20),
        options=[
            ('grpc.max_send_message_length',    256 * 1024 * 1024),
            ('grpc.max_receive_message_length', 256 * 1024 * 1024),
        ]
    )
    dfs_pb2_grpc.add_NameNodeServiceServicer_to_server(NameNodeServicer(), server)
    server.add_insecure_port(f'[::]:{config.NAMENODE_PORT}')
    server.start()
    logger.info(f"NameNode escuchando en puerto {config.NAMENODE_PORT}")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("NameNode cerrando...")
        server.stop(grace=5)


if __name__ == '__main__':
    serve()
