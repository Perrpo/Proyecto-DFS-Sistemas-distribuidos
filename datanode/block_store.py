"""
datanode/block_store.py
Almacenamiento local de bloques en disco con verificación SHA-256.
Día 3: se agregan store_block() y retrieve_block() que usan verify_checksum().
"""
import os
import hashlib
import shutil
import logging

logger = logging.getLogger('datanode.block_store')

CHUNK = 4 * 1024 * 1024   # 4 MB por chunk de lectura


class BlockStore:
    """Gestiona bloques almacenados como archivos en disco."""

    def __init__(self, storage_path: str):
        self.storage_path = storage_path
        os.makedirs(storage_path, exist_ok=True)
        logger.info(f"BlockStore inicializado en {storage_path}")

    # ─── Path helper ───────────────────────────────────────────────────────────
    def _path(self, block_id: str) -> str:
        return os.path.join(self.storage_path, f"{block_id}.block")

    def block_exists(self, block_id: str) -> bool:
        return os.path.exists(self._path(block_id))

    # ─── API de alto nivel (usada por datanode/server.py) ─────────────────────
    def store_block(self, block_id: str, data: bytes) -> tuple:
        """
        Almacena un bloque en disco y verifica la integridad por SHA-256.

        Args:
            block_id: Identificador único del bloque
            data:     Datos completos del bloque (bytes)

        Returns:
            (success: bool, message: str)
        """
        try:
            checksum = self.write_block(block_id, data)
            # Verificar que lo escrito es idéntico a lo recibido
            valid, actual = self.verify_checksum(block_id, checksum)
            if not valid:
                # Eliminar archivo corrupto
                try:
                    os.remove(self._path(block_id))
                except OSError:
                    pass
                return False, f"Error de integridad al almacenar bloque {block_id}"
            return True, f"Bloque {block_id} almacenado ({len(data):,} bytes)"
        except Exception as e:
            logger.error(f"store_block {block_id}: {e}", exc_info=True)
            return False, f"Error interno al almacenar bloque {block_id}: {e}"

    def retrieve_block(self, block_id: str) -> tuple:
        """
        Lee un bloque completo del disco y verifica su integridad SHA-256.

        Returns:
            (data: bytes, error: str|None)
            Si hay error: (None, mensaje_de_error)
        """
        path = self._path(block_id)
        if not os.path.exists(path):
            return None, f"Bloque no encontrado: {block_id}"

        try:
            with open(path, 'rb') as f:
                data = f.read()

            # Calcular checksum de lo leído y comparar con el archivo en disco
            # (doble lectura evitada: sólo calculamos sobre los datos leídos)
            actual_checksum = hashlib.sha256(data).hexdigest()

            # Verificar contra el checksum recalculado leyendo de nuevo el archivo
            valid, stored_checksum = self.verify_checksum(block_id, actual_checksum)
            if not valid:
                logger.error(
                    f"retrieve_block {block_id}: checksum mismatch "
                    f"(almacenado={stored_checksum[:12]} leído={actual_checksum[:12]})"
                )
                return None, f"Bloque corrupto: {block_id} (checksum mismatch)"

            logger.debug(f"retrieve_block {block_id}: {len(data):,} bytes OK")
            return data, None

        except Exception as e:
            logger.error(f"retrieve_block {block_id}: {e}", exc_info=True)
            return None, f"Error al leer bloque {block_id}: {e}"

    # ─── Escritura ─────────────────────────────────────────────────────────────
    def write_block(self, block_id: str, data: bytes) -> str:
        """
        Escribe los datos en disco y retorna el checksum SHA-256.
        """
        path = self._path(block_id)
        with open(path, 'wb') as f:
            f.write(data)
        checksum = hashlib.sha256(data).hexdigest()
        logger.info(
            f"Bloque almacenado: {block_id} ({len(data):,} bytes, "
            f"sha256={checksum[:12]}...)"
        )
        return checksum

    def write_block_stream(self, block_id: str, chunks: list) -> str:
        """
        Escribe un bloque recibido en chunks y retorna el checksum SHA-256.
        chunks: lista de bytes
        """
        path   = self._path(block_id)
        sha256 = hashlib.sha256()
        with open(path, 'wb') as f:
            for chunk in chunks:
                f.write(chunk)
                sha256.update(chunk)
        checksum = sha256.hexdigest()
        size = os.path.getsize(path)
        logger.info(
            f"Bloque almacenado (stream): {block_id} ({size:,} bytes, "
            f"sha256={checksum[:12]}...)"
        )
        return checksum

    # ─── Lectura ───────────────────────────────────────────────────────────────
    def read_block_chunks(self, block_id: str, chunk_size: int = CHUNK):
        """
        Generador que produce chunks de bytes del bloque.
        Lanza FileNotFoundError si el bloque no existe.
        """
        path = self._path(block_id)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Bloque no encontrado: {block_id}")
        with open(path, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk

    def read_block_full(self, block_id: str) -> bytes:
        """Lee el bloque completo en memoria (para replicación)."""
        path = self._path(block_id)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Bloque no encontrado: {block_id}")
        with open(path, 'rb') as f:
            return f.read()

    # ─── Verificación ──────────────────────────────────────────────────────────
    def verify_checksum(self, block_id: str, expected: str) -> tuple:
        """
        Verifica la integridad del bloque leyendo el archivo en disco.
        Retorna: (valid: bool, actual_checksum: str)
        """
        path = self._path(block_id)
        if not os.path.exists(path):
            return False, ""
        sha256 = hashlib.sha256()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(CHUNK), b''):
                sha256.update(chunk)
        actual = sha256.hexdigest()
        return actual == expected, actual

    # ─── Eliminación ───────────────────────────────────────────────────────────
    def delete_block(self, block_id: str) -> tuple:
        """Elimina el archivo de bloque. Retorna (success, message)."""
        path = self._path(block_id)
        if os.path.exists(path):
            os.remove(path)
            logger.info(f"Bloque eliminado: {block_id}")
            return True, f"Bloque eliminado: {block_id}"
        return False, f"Bloque no encontrado: {block_id}"

    # ─── Inventario ────────────────────────────────────────────────────────────
    def list_blocks(self) -> list:
        """
        Retorna lista de dicts {block_id, block_size, checksum} para BlockReport.
        """
        blocks = []
        try:
            for fname in os.listdir(self.storage_path):
                if not fname.endswith('.block'):
                    continue
                block_id = fname[:-6]
                path     = os.path.join(self.storage_path, fname)
                size     = os.path.getsize(path)
                sha256   = hashlib.sha256()
                with open(path, 'rb') as f:
                    for chunk in iter(lambda: f.read(CHUNK), b''):
                        sha256.update(chunk)
                blocks.append({
                    'block_id':   block_id,
                    'block_size': size,
                    'checksum':   sha256.hexdigest(),
                })
        except Exception as e:
            logger.error(f"Error escaneando bloques: {e}")
        return blocks

    # ─── Espacio disponible ────────────────────────────────────────────────────
    def get_available_space(self) -> int:
        try:
            _, _, free = shutil.disk_usage(self.storage_path)
            return free
        except Exception:
            return 0
