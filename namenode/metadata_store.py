"""
namenode/metadata_store.py
Capa de acceso a datos SQLite para el NameNode.
Gestiona: usuarios, directorios, archivos, bloques, ubicaciones y DataNodes.
"""
import sqlite3
import os
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class MetadataStore:
    """Capa de persistencia SQLite del NameNode."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self._init_db()

    # ─── Conexión ──────────────────────────────────────────────────────────────
    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self):
        with self._get_conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id      TEXT PRIMARY KEY,
                    username     TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    created_at   TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS directories (
                    dir_id     TEXT PRIMARY KEY,
                    user_id    TEXT NOT NULL,
                    path       TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(user_id, path),
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                );

                CREATE TABLE IF NOT EXISTS files (
                    file_id     TEXT PRIMARY KEY,
                    user_id     TEXT NOT NULL,
                    filepath    TEXT NOT NULL,
                    filesize    INTEGER NOT NULL,
                    block_count INTEGER NOT NULL,
                    block_size  INTEGER NOT NULL,
                    created_at  TEXT NOT NULL,
                    status      TEXT NOT NULL DEFAULT 'active',
                    UNIQUE(user_id, filepath),
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                );

                CREATE TABLE IF NOT EXISTS blocks (
                    block_id    TEXT PRIMARY KEY,
                    file_id     TEXT NOT NULL,
                    block_index INTEGER NOT NULL,
                    block_size  INTEGER NOT NULL DEFAULT 0,
                    checksum    TEXT NOT NULL DEFAULT 'pending',
                    FOREIGN KEY (file_id) REFERENCES files(file_id)
                );

                CREATE TABLE IF NOT EXISTS block_locations (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    block_id   TEXT NOT NULL,
                    node_id    TEXT NOT NULL,
                    status     TEXT NOT NULL DEFAULT 'active',
                    updated_at TEXT NOT NULL,
                    UNIQUE(block_id, node_id),
                    FOREIGN KEY (block_id) REFERENCES blocks(block_id)
                );

                CREATE TABLE IF NOT EXISTS datanodes (
                    node_id         TEXT PRIMARY KEY,
                    host            TEXT NOT NULL,
                    port            INTEGER NOT NULL,
                    available_space INTEGER NOT NULL DEFAULT 0,
                    block_count     INTEGER NOT NULL DEFAULT 0,
                    status          TEXT NOT NULL DEFAULT 'active',
                    last_heartbeat  TEXT NOT NULL
                );
            """)
            conn.commit()
        logger.info(f"Base de datos inicializada en {self.db_path}")

    # ─── Usuarios ──────────────────────────────────────────────────────────────
    def create_user(self, user_id: str, username: str, password_hash: str):
        with self._get_conn() as conn:
            conn.execute(
                "INSERT INTO users (user_id, username, password_hash, created_at) VALUES (?,?,?,?)",
                (user_id, username, password_hash, datetime.utcnow().isoformat())
            )
            conn.commit()

    def get_user_by_username(self, username: str):
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE username=?", (username,)
            ).fetchone()
            return dict(row) if row else None

    def get_user_by_id(self, user_id: str):
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE user_id=?", (user_id,)
            ).fetchone()
            return dict(row) if row else None

    # ─── DataNodes ─────────────────────────────────────────────────────────────
    def upsert_datanode(self, node_id, host, port, available_space, block_count, status='active'):
        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO datanodes
                    (node_id, host, port, available_space, block_count, status, last_heartbeat)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(node_id) DO UPDATE SET
                    host=excluded.host,
                    port=excluded.port,
                    available_space=excluded.available_space,
                    block_count=excluded.block_count,
                    status=excluded.status,
                    last_heartbeat=excluded.last_heartbeat
            """, (node_id, host, port, available_space, block_count, status,
                  datetime.utcnow().isoformat()))
            conn.commit()

    def get_active_datanodes(self):
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM datanodes WHERE status='active' ORDER BY available_space DESC"
            ).fetchall()
            return [dict(r) for r in rows]

    def get_all_datanodes(self):
        with self._get_conn() as conn:
            rows = conn.execute("SELECT * FROM datanodes").fetchall()
            return [dict(r) for r in rows]

    def mark_datanode_inactive(self, node_id: str):
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE datanodes SET status='inactive' WHERE node_id=?", (node_id,)
            )
            conn.commit()

    def get_datanode(self, node_id: str):
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM datanodes WHERE node_id=?", (node_id,)
            ).fetchone()
            return dict(row) if row else None

    # ─── Archivos ──────────────────────────────────────────────────────────────
    def create_file(self, file_id, user_id, filepath, filesize, block_count, block_size):
        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO files
                    (file_id, user_id, filepath, filesize, block_count, block_size, created_at, status)
                VALUES (?,?,?,?,?,?,?,'active')
            """, (file_id, user_id, filepath, filesize, block_count, block_size,
                  datetime.utcnow().isoformat()))
            conn.commit()

    def get_file(self, user_id: str, filepath: str):
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM files WHERE user_id=? AND filepath=? AND status='active'",
                (user_id, filepath)
            ).fetchone()
            return dict(row) if row else None

    def get_file_by_id(self, file_id: str):
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM files WHERE file_id=?", (file_id,)
            ).fetchone()
            return dict(row) if row else None

    def list_files(self, user_id: str, directory: str):
        with self._get_conn() as conn:
            if directory == '/':
                rows = conn.execute(
                    "SELECT * FROM files WHERE user_id=? AND status='active'",
                    (user_id,)
                ).fetchall()
            else:
                prefix = directory.rstrip('/') + '/'
                rows = conn.execute(
                    "SELECT * FROM files WHERE user_id=? AND status='active' AND filepath LIKE ?",
                    (user_id, prefix + '%')
                ).fetchall()
            return [dict(r) for r in rows]

    def mark_file_deleted(self, user_id: str, filepath: str):
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE files SET status='deleted' WHERE user_id=? AND filepath=?",
                (user_id, filepath)
            )
            conn.commit()

    # ─── Bloques ───────────────────────────────────────────────────────────────
    def create_block(self, block_id, file_id, block_index, block_size, checksum):
        with self._get_conn() as conn:
            conn.execute(
                "INSERT INTO blocks (block_id, file_id, block_index, block_size, checksum) VALUES (?,?,?,?,?)",
                (block_id, file_id, block_index, block_size, checksum)
            )
            conn.commit()

    def update_block(self, block_id: str, block_size: int, checksum: str):
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE blocks SET block_size=?, checksum=? WHERE block_id=?",
                (block_size, checksum, block_id)
            )
            conn.commit()

    def get_block(self, block_id: str):
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM blocks WHERE block_id=?", (block_id,)
            ).fetchone()
            return dict(row) if row else None

    def get_blocks_for_file(self, file_id: str):
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM blocks WHERE file_id=? ORDER BY block_index",
                (file_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    def delete_blocks_for_file(self, file_id: str):
        with self._get_conn() as conn:
            conn.execute("""
                DELETE FROM block_locations
                WHERE block_id IN (SELECT block_id FROM blocks WHERE file_id=?)
            """, (file_id,))
            conn.execute("DELETE FROM blocks WHERE file_id=?", (file_id,))
            conn.commit()

    # ─── Ubicaciones de Bloque ─────────────────────────────────────────────────
    def add_block_location(self, block_id: str, node_id: str):
        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO block_locations (block_id, node_id, status, updated_at)
                VALUES (?,?,'active',?)
            """, (block_id, node_id, datetime.utcnow().isoformat()))
            conn.commit()

    def get_block_locations(self, block_id: str):
        """Devuelve ubicaciones activas de un bloque, uniendo con datos del DataNode."""
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT bl.block_id, bl.node_id, bl.status, dn.host, dn.port
                FROM block_locations bl
                JOIN datanodes dn ON bl.node_id = dn.node_id
                WHERE bl.block_id=? AND bl.status='active' AND dn.status='active'
            """, (block_id,)).fetchall()
            return [dict(r) for r in rows]

    def remove_block_location(self, block_id: str, node_id: str):
        with self._get_conn() as conn:
            conn.execute("""
                UPDATE block_locations
                SET status='deleted', updated_at=?
                WHERE block_id=? AND node_id=?
            """, (datetime.utcnow().isoformat(), block_id, node_id))
            conn.commit()

    def get_blocks_for_node(self, node_id: str):
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT block_id FROM block_locations WHERE node_id=? AND status='active'",
                (node_id,)
            ).fetchall()
            return [r['block_id'] for r in rows]

    def get_all_block_replica_counts(self):
        """Devuelve conteo de réplicas activas por bloque (para detectar sub-replicación)."""
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT block_id, COUNT(*) as replica_count
                FROM block_locations
                WHERE status='active'
                GROUP BY block_id
            """).fetchall()
            return [dict(r) for r in rows]

    def reconcile_block_report(self, node_id: str, reported_blocks: list):
        """Reconcilia el inventario de bloques reportado vs el esperado."""
        with self._get_conn() as conn:
            known = set(r['block_id'] for r in conn.execute(
                "SELECT block_id FROM block_locations WHERE node_id=? AND status='active'",
                (node_id,)
            ).fetchall())
            reported = set(b['block_id'] for b in reported_blocks)

            for block_id in reported - known:
                conn.execute("""
                    INSERT OR REPLACE INTO block_locations (block_id, node_id, status, updated_at)
                    VALUES (?,?,'active',?)
                """, (block_id, node_id, datetime.utcnow().isoformat()))

            for block_id in known - reported:
                conn.execute("""
                    UPDATE block_locations SET status='missing', updated_at=?
                    WHERE block_id=? AND node_id=?
                """, (datetime.utcnow().isoformat(), block_id, node_id))

            conn.commit()
        logger.debug(f"BlockReport reconciliado para {node_id}: +{len(reported-known)} -{len(known-reported)}")

    # ─── Directorios ───────────────────────────────────────────────────────────
    def create_directory(self, dir_id: str, user_id: str, path: str):
        with self._get_conn() as conn:
            conn.execute(
                "INSERT INTO directories (dir_id, user_id, path, created_at) VALUES (?,?,?,?)",
                (dir_id, user_id, path, datetime.utcnow().isoformat())
            )
            conn.commit()

    def get_directory(self, user_id: str, path: str):
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM directories WHERE user_id=? AND path=?",
                (user_id, path)
            ).fetchone()
            return dict(row) if row else None

    def list_directories(self, user_id: str, parent_path: str):
        with self._get_conn() as conn:
            if parent_path == '/':
                rows = conn.execute(
                    "SELECT * FROM directories WHERE user_id=? AND path != '/'",
                    (user_id,)
                ).fetchall()
            else:
                prefix = parent_path.rstrip('/') + '/'
                rows = conn.execute(
                    "SELECT * FROM directories WHERE user_id=? AND path LIKE ? AND path != ?",
                    (user_id, prefix + '%', parent_path)
                ).fetchall()
            return [dict(r) for r in rows]

    def delete_directory(self, user_id: str, path: str):
        with self._get_conn() as conn:
            conn.execute(
                "DELETE FROM directories WHERE user_id=? AND path=?",
                (user_id, path)
            )
            conn.commit()

    def directory_is_empty(self, user_id: str, path: str) -> bool:
        with self._get_conn() as conn:
            prefix = path.rstrip('/') + '/'
            file_count = conn.execute(
                "SELECT COUNT(*) FROM files WHERE user_id=? AND filepath LIKE ? AND status='active'",
                (user_id, prefix + '%')
            ).fetchone()[0]
            dir_count = conn.execute(
                "SELECT COUNT(*) FROM directories WHERE user_id=? AND path LIKE ?",
                (user_id, prefix + '%')
            ).fetchone()[0]
            return file_count == 0 and dir_count == 0

    def list_all_files_under(self, user_id: str, path: str):
        """Lista todos los archivos activos bajo path (recursivo)."""
        with self._get_conn() as conn:
            prefix = path.rstrip('/') + '/'
            rows = conn.execute(
                "SELECT * FROM files WHERE user_id=? AND (filepath LIKE ? OR filepath=?) AND status='active'",
                (user_id, prefix + '%', path)
            ).fetchall()
            return [dict(r) for r in rows]

    def list_all_dirs_under(self, user_id: str, path: str):
        """Lista todos los subdirectorios bajo path (recursivo, excluye path mismo)."""
        with self._get_conn() as conn:
            prefix = path.rstrip('/') + '/'
            rows = conn.execute(
                "SELECT * FROM directories WHERE user_id=? AND path LIKE ?",
                (user_id, prefix + '%')
            ).fetchall()
            return [dict(r) for r in rows]

    def get_total_file_count(self) -> int:
        """Total de archivos activos en el sistema."""
        with self._get_conn() as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM files WHERE status='active'"
            ).fetchone()[0]

    def get_total_block_count(self) -> int:
        """Total de bloques únicos confirmados en el sistema."""
        with self._get_conn() as conn:
            return conn.execute(
                "SELECT COUNT(DISTINCT block_id) FROM block_locations WHERE status='active'"
            ).fetchone()[0]
