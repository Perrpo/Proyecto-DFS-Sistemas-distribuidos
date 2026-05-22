"""
namenode/block_manager.py
Gestiona asignación de DataNodes para bloques (Round-Robin con balanceo de espacio)
y detecta bloques sub-replicados para re-replicación.
"""
import uuid
import logging

logger = logging.getLogger(__name__)


class BlockManager:
    """Coordinador de bloques del NameNode."""

    def __init__(self, metadata_store, replication_factor: int = 3):
        self.store = metadata_store
        self.replication_factor = replication_factor
        self._rr_index = 0   # índice Round-Robin global

    # ─── Asignación para PUT ───────────────────────────────────────────────────
    def assign_datanodes_for_upload(self, block_count: int) -> list:
        """
        Retorna una lista de listas con los DataNodes asignados a cada bloque.
        Estrategia: Round-Robin partiendo del nodo con más espacio disponible.
        Lanza ValueError si no hay suficientes DataNodes activos.
        """
        active = self.store.get_active_datanodes()   # ordenados por available_space DESC

        if len(active) < self.replication_factor:
            raise ValueError(
                f"Error: no hay DataNodes suficientes. "
                f"Se necesitan {self.replication_factor}, disponibles: {len(active)}"
            )

        assignments = []
        n = len(active)
        for i in range(block_count):
            start = (self._rr_index + i) % n
            pipeline = []
            for j in range(self.replication_factor):
                pipeline.append(active[(start + j) % n])
            assignments.append(pipeline)

        self._rr_index = (self._rr_index + block_count) % n
        return assignments

    # ─── Ubicaciones para GET ──────────────────────────────────────────────────
    def get_block_locations_for_download(self, file_id: str) -> list:
        """
        Retorna lista de dicts {block, locations} para reconstruir un archivo.
        """
        blocks = self.store.get_blocks_for_file(file_id)
        result = []
        for block in blocks:
            locations = self.store.get_block_locations(block['block_id'])
            result.append({'block': block, 'locations': locations})
        return result

    # ─── Registro de bloques confirmados ──────────────────────────────────────
    def confirm_block(self, block_id: str, block_size: int, checksum: str, node_ids: list):
        """Actualiza metadatos tras confirmación del cliente de un bloque subido."""
        self.store.update_block(block_id, block_size, checksum)
        for node_id in node_ids:
            self.store.add_block_location(block_id, node_id)
        logger.info(f"Bloque confirmado: {block_id} en nodos {node_ids}")

    # ─── Detección de sub-replicación ─────────────────────────────────────────
    def get_underreplicated_blocks(self) -> list:
        """
        Devuelve bloques con réplicas activas < REPLICATION_FACTOR.
        """
        counts = self.store.get_all_block_replica_counts()
        under  = []
        for item in counts:
            if item['replica_count'] < self.replication_factor:
                block_info = self.store.get_block(item['block_id'])
                if block_info and block_info.get('checksum') == 'pending':
                    continue   # bloque aún no confirmado, ignorar
                locations  = self.store.get_block_locations(item['block_id'])
                under.append({
                    'block_id':        item['block_id'],
                    'block_info':      block_info,
                    'current_replicas': item['replica_count'],
                    'locations':       locations,
                })
        return under
