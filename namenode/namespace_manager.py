"""
namenode/namespace_manager.py
Gestiona el espacio de nombres virtual (directorios) por usuario.
"""
import uuid
import logging

logger = logging.getLogger(__name__)


class NamespaceManager:
    """Operaciones sobre el namespace de directorios virtuales."""

    def __init__(self, metadata_store):
        self.store = metadata_store

    # ─── mkdir ─────────────────────────────────────────────────────────────────
    def make_dir(self, user_id: str, path: str):
        path = _normalize(path)

        if self.store.get_directory(user_id, path):
            return False, f"Error: directorio ya existe: {path}"

        parent = _parent(path)
        if parent != path and not self.store.get_directory(user_id, parent):
            return False, f"Error: directorio padre no existe: {parent}"

        try:
            self.store.create_directory(str(uuid.uuid4()), user_id, path)
            logger.info(f"mkdir: {path} → usuario {user_id}")
            return True, "Directorio creado"
        except Exception as e:
            logger.error(f"make_dir error: {e}")
            return False, f"Error al crear directorio: {e}"

    # ─── rmdir ─────────────────────────────────────────────────────────────────
    def remove_dir(self, user_id: str, path: str, recursive: bool = False):
        path = _normalize(path)

        if path == '/':
            return False, "Error: no se puede eliminar el directorio raíz"

        if not self.store.get_directory(user_id, path):
            return False, f"Error: directorio no existe: {path}"

        if recursive:
            return self._remove_dir_recursive(user_id, path)

        if not self.store.directory_is_empty(user_id, path):
            return False, (
                f"Error: directorio no está vacío: {path}. "
                f"Use 'rmdir -r' para eliminar recursivamente."
            )

        try:
            self.store.delete_directory(user_id, path)
            logger.info(f"rmdir: {path} → usuario {user_id}")
            return True, "Directorio eliminado"
        except Exception as e:
            logger.error(f"remove_dir error: {e}")
            return False, f"Error al eliminar directorio: {e}"

    def _remove_dir_recursive(self, user_id: str, path: str):
        """Elimina recursivamente todos los archivos y subdirectorios bajo path."""
        try:
            # 1. Eliminar todos los archivos directamente bajo este directorio y subdirectorios
            all_files = self.store.list_all_files_under(user_id, path)
            for f in all_files:
                self.store.mark_file_deleted(user_id, f['filepath'])
                self.store.delete_blocks_for_file(f['file_id'])

            # 2. Eliminar subdirectorios (de los más profundos hacia arriba)
            all_dirs = self.store.list_all_dirs_under(user_id, path)
            all_dirs_sorted = sorted(all_dirs, key=lambda d: d['path'].count('/'), reverse=True)
            for d in all_dirs_sorted:
                self.store.delete_directory(user_id, d['path'])

            # 3. Eliminar el directorio raíz de la operación
            self.store.delete_directory(user_id, path)
            logger.info(f"rmdir -r: {path} ({len(all_files)} archivos, {len(all_dirs)} subdirs) → usuario {user_id}")
            return True, f"Directorio eliminado recursivamente: {path} ({len(all_files)} archivos eliminados)"
        except Exception as e:
            logger.error(f"remove_dir_recursive error: {e}")
            return False, f"Error al eliminar directorio: {e}"


    # ─── ls ────────────────────────────────────────────────────────────────────
    def list_dir(self, user_id: str, directory: str):
        """
        Lista archivos y subdirectorios.
        Retorna: (success, files, dirs, message)
        """
        directory = _normalize(directory)

        if not self.store.get_directory(user_id, directory):
            return False, [], [], f"Error: directorio no existe: {directory}"

        files = self.store.list_files(user_id, directory)
        dirs  = self.store.list_directories(user_id, directory)
        return True, files, dirs, "OK"

    # ─── Validaciones ──────────────────────────────────────────────────────────
    def validate_filepath(self, user_id: str, filepath: str):
        """Verifica que el directorio padre del archivo exista."""
        filepath = _normalize(filepath)
        parent   = _parent(filepath)
        if not self.store.get_directory(user_id, parent):
            return False, f"Error: directorio no existe: {parent}"
        return True, filepath

    def ensure_root(self, user_id: str):
        """Garantiza que el usuario tenga directorio raíz."""
        if not self.store.get_directory(user_id, '/'):
            self.store.create_directory(str(uuid.uuid4()), user_id, '/')


# ─── Helpers ───────────────────────────────────────────────────────────────────
def _normalize(path: str) -> str:
    if not path.startswith('/'):
        path = '/' + path
    if path != '/' and path.endswith('/'):
        path = path.rstrip('/')
    return path


def _parent(path: str) -> str:
    if path == '/':
        return '/'
    parts = path.rsplit('/', 1)
    return parts[0] if parts[0] else '/'
