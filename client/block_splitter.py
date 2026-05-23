"""
client/block_splitter.py
Divide archivos en bloques de tamaño fijo para distribución en DataNodes.
Día 2: implementación completa.
"""
import hashlib
import math
import os


def split_file(filepath: str, block_size: int):
    """
    Generador que produce (block_index, block_data, checksum_sha256) por cada bloque.

    Args:
        filepath:   Ruta local del archivo a dividir
        block_size: Tamaño máximo de cada bloque en bytes (ej. 64*1024*1024)

    Yields:
        (int, bytes, str): índice 0-based, datos del bloque, sha256 hex del bloque

    Raises:
        FileNotFoundError: si el archivo no existe
        IOError: si hay un error de lectura
    """
    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"Archivo no encontrado: {filepath}")

    block_index = 0
    with open(filepath, 'rb') as f:
        while True:
            data = f.read(block_size)
            if not data:
                break
            checksum = hashlib.sha256(data).hexdigest()
            yield block_index, data, checksum
            block_index += 1


def get_block_count(filesize: int, block_size: int) -> int:
    """Calcula el número de bloques necesarios para un archivo."""
    if filesize == 0:
        return 1
    return math.ceil(filesize / block_size)
