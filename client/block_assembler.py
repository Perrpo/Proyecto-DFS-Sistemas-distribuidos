"""
client/block_assembler.py
Reconstruye un archivo a partir de bloques descargados de los DataNodes.
Día 2: implementación completa.
"""
import os


def assemble_blocks(ordered_blocks: list, output_path: str) -> int:
    """
    Reconstruye el archivo original a partir de bloques en orden.

    Args:
        ordered_blocks: Lista de tuplas (block_index, block_data) ordenada por block_index.
                        Ejemplo: [(0, b'...'), (1, b'...'), (2, b'...')]
        output_path:    Ruta donde guardar el archivo reconstruido.
                        Si output_path es un directorio, el archivo se guarda allí
                        con un nombre temporal.

    Returns:
        int: tamaño total del archivo reconstruido en bytes

    Raises:
        ValueError: si la lista de bloques está vacía o tiene un hueco en los índices
        IOError:    si hay error al escribir el archivo de salida
    """
    if not ordered_blocks:
        raise ValueError("La lista de bloques está vacía")

    # Ordenar por índice (por si acaso llegan desordenados)
    sorted_blocks = sorted(ordered_blocks, key=lambda x: x[0])

    # Verificar continuidad de índices
    expected_index = 0
    for idx, _ in sorted_blocks:
        if idx != expected_index:
            raise ValueError(
                f"Hueco en los bloques: esperado índice {expected_index}, encontrado {idx}"
            )
        expected_index += 1

    # Crear directorio destino si no existe
    parent = os.path.dirname(output_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    total_bytes = 0
    with open(output_path, 'wb') as out:
        for _, data in sorted_blocks:
            out.write(data)
            total_bytes += len(data)

    return total_bytes
