"""
config.py – Configuración global de MiniDFS
Todos los valores son sobrescribibles via variables de entorno.
"""
import os

# ── Bloques ───────────────────────────────────────────────────────────────────
BLOCK_SIZE = int(os.getenv("BLOCK_SIZE", str(64 * 1024 * 1024)))   # 64 MB
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", "3"))
CHUNK_SIZE = 4 * 1024 * 1024                                        # 4 MB por chunk gRPC

# ── Heartbeat ─────────────────────────────────────────────────────────────────
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "10"))    # segundos
HEARTBEAT_TIMEOUT = int(os.getenv("HEARTBEAT_TIMEOUT", "30"))      # segundos
BLOCK_REPORT_INTERVAL = int(os.getenv("BLOCK_REPORT_INTERVAL", "60"))

# ── NameNode ──────────────────────────────────────────────────────────────────
NAMENODE_HOST = os.getenv("NAMENODE_HOST", "localhost")
NAMENODE_PORT = int(os.getenv("NAMENODE_PORT", "50051"))
NAMENODE_DB_PATH = os.getenv("NAMENODE_DB_PATH", "./namenode.db")

# ── DataNode ──────────────────────────────────────────────────────────────────
NODE_ID = os.getenv("NODE_ID", "dn1")
DATANODE_HOST = os.getenv("DATANODE_HOST", "localhost")
DATANODE_PORT = int(os.getenv("DATANODE_PORT", "50061"))
DATANODE_STORAGE_PATH = os.getenv("DATANODE_STORAGE_PATH", "./blocks")

# ── Auth ──────────────────────────────────────────────────────────────────────
JWT_SECRET = os.getenv("JWT_SECRET", "minidfs-secret-key-change-in-production")
JWT_EXPIRY_HOURS = int(os.getenv("JWT_EXPIRY_HOURS", "24"))

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
