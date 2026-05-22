"""
client/block_splitter.py
Divide archivos en bloques de tamaño fijo para distribución en DataNodes.
Implementación completa en Día 2.
Día 1: esqueleto con la firma correcta.
"""
import hashlib
import os


def split_file(filepath: str, block_size: int):
    """
    Generador que produce (block_index, block_data, checksum_sha256) por cada bloque.

    Args:
        filepath:   Ruta local del archivo a dividir
        block_size: Tamaño máximo de cada bloque en bytes (ej. 64*1024*1024)

    Yields:
        (int, bytes, str): índice, datos, sha256 del bloque

    Ejemplo de uso (Día 2):
        for idx, data, checksum in split_file("video.mp4", 64*1024*1024):
            print(f"Bloque {idx}: {len(data)} bytes, sha256={checksum[:12]}")
    """
    # Implementación completa en Día 2
    raise NotImplementedError("split_file: implementación completa en Día 2")


def get_block_count(filesize: int, block_size: int) -> int:
    """Calcula el número de bloques necesarios para un archivo."""
    import math
    return math.ceil(filesize / block_size)
