"""
client/block_assembler.py
Reconstruye un archivo a partir de bloques descargados de los DataNodes.
Implementación completa en Día 2.
Día 1: esqueleto con firma correcta.
"""


def assemble_blocks(ordered_blocks: list, output_path: str):
    """
    Reconstruye el archivo original a partir de bloques en orden.

    Args:
        ordered_blocks: Lista de bytes ordenada por block_index
                        [(block_index, block_data), ...]
        output_path:    Ruta donde guardar el archivo reconstruido

    Returns:
        int: tamaño del archivo reconstruido en bytes

    Ejemplo de uso (Día 2):
        size = assemble_blocks([(0, data0), (1, data1), (2, data2)], "output.mp4")
        print(f"Archivo reconstruido: {size} bytes")
    """
    # Implementación completa en Día 2
    raise NotImplementedError("assemble_blocks: implementación completa en Día 2")
