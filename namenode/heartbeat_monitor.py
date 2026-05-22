"""
namenode/heartbeat_monitor.py
Daemon de monitoreo de DataNodes.
- Detecta nodos caídos (sin heartbeat > HEARTBEAT_TIMEOUT)
- Detecta bloques sub-replicados y genera tareas de re-replicación
- Las tareas son entregadas a los DataNodes en la respuesta del siguiente Heartbeat
"""
import threading
import logging
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import config

logger = logging.getLogger('namenode.heartbeat_monitor')


class HeartbeatMonitor(threading.Thread):
    """
    Thread daemon que verifica la salud de los DataNodes cada HEARTBEAT_INTERVAL segundos.
    """

    def __init__(self, metadata_store, block_manager):
        super().__init__(daemon=True, name='HeartbeatMonitor')
        self.store         = metadata_store
        self.block_manager = block_manager
        self._stop_event   = threading.Event()
        # Mapa: node_id → [(block_id, {source, destination})]
        self._pending_tasks: dict = {}
        self._lock = threading.Lock()

    def run(self):
        logger.info(
            f"HeartbeatMonitor iniciado "
            f"(interval={config.HEARTBEAT_INTERVAL}s, timeout={config.HEARTBEAT_TIMEOUT}s)"
        )
        while not self._stop_event.is_set():
            try:
                self._check_datanodes()
                self._schedule_replication()
            except Exception as e:
                logger.error(f"HeartbeatMonitor error: {e}", exc_info=True)
            self._stop_event.wait(config.HEARTBEAT_INTERVAL)

    def stop(self):
        self._stop_event.set()

    # ─── Detección de nodos caídos ─────────────────────────────────────────────
    def _check_datanodes(self):
        threshold = datetime.utcnow() - timedelta(seconds=config.HEARTBEAT_TIMEOUT)
        for node in self.store.get_all_datanodes():
            if node['status'] == 'inactive':
                continue
            last_hb = datetime.fromisoformat(node['last_heartbeat'])
            if last_hb < threshold:
                logger.warning(
                    f"DataNode {node['node_id']} inactivo "
                    f"(último heartbeat: {node['last_heartbeat']})"
                )
                self.store.mark_datanode_inactive(node['node_id'])

    # ─── Programación de re-replicación ────────────────────────────────────────
    def _schedule_replication(self):
        under_replicated = self.block_manager.get_underreplicated_blocks()
        if not under_replicated:
            return

        active_nodes = self.store.get_active_datanodes()
        if len(active_nodes) < 2:
            logger.warning("No se puede re-replicar: menos de 2 DataNodes activos")
            return

        for item in under_replicated:
            block_id  = item['block_id']
            locations = item['locations']

            if not locations:
                logger.error(f"¡Bloque {block_id} sin réplicas disponibles! Pérdida de datos.")
                continue

            existing_node_ids = {loc['node_id'] for loc in locations}
            source      = locations[0]
            destination = next(
                (n for n in active_nodes if n['node_id'] not in existing_node_ids),
                None
            )

            if destination is None:
                logger.warning(f"Sin destino disponible para re-replicar {block_id}")
                continue

            src_node_id = source['node_id']
            task = {'block_id': block_id, 'source': source, 'destination': destination}

            with self._lock:
                tasks = self._pending_tasks.setdefault(src_node_id, [])
                # Evitar duplicados
                if not any(t['block_id'] == block_id for t in tasks):
                    tasks.append(task)
                    logger.info(
                        f"Re-replicación programada: {block_id} "
                        f"{src_node_id} → {destination['node_id']}"
                    )

    # ─── Entrega de tareas a DataNodes (llamado por server.py en Heartbeat) ────
    def pop_tasks_for_node(self, node_id: str) -> list:
        """Devuelve y elimina las tareas pendientes para un DataNode."""
        with self._lock:
            return self._pending_tasks.pop(node_id, [])
