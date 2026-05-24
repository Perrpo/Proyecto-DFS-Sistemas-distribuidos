#!/usr/bin/env python3
"""
client/cli.py
Interfaz de línea de comandos de MiniDFS.
Comandos: register, login, logout, put, get, ls, rm, mkdir, rmdir, status
"""
import argparse
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import config

from auth_client import AuthClient
from block_splitter import split_file, get_block_count
from block_assembler import assemble_blocks
from block_transfer import upload_block, download_block
from proto import dfs_pb2, dfs_pb2_grpc
import grpc

# ─── Conexión al NameNode ──────────────────────────────────────────────────────
def _nn_stub():
    channel = grpc.insecure_channel(
        f"{config.NAMENODE_HOST}:{config.NAMENODE_PORT}",
        options=[('grpc.max_receive_message_length', 256 * 1024 * 1024)]
    )
    return dfs_pb2_grpc.NameNodeServiceStub(channel), channel


def _require_auth():
    token = AuthClient.get_token()
    if not token:
        print("Error: sesión no válida, por favor inicie sesión nuevamente")
        sys.exit(1)
    return token


def _human_size(n: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


# ══════════════════════════════════════════════════════════════════════════════
# Comandos
# ══════════════════════════════════════════════════════════════════════════════

def cmd_register(args):
    stub, ch = _nn_stub()
    try:
        r = stub.Register(
            dfs_pb2.RegisterRequest(username=args.username, password=args.password),
            timeout=10
        )
        print(r.message)
        if not r.success:
            sys.exit(1)
    except grpc.RpcError as e:
        print(f"Error de conexión: {e.details()}")
        sys.exit(1)
    finally:
        ch.close()


def cmd_login(args):
    stub, ch = _nn_stub()
    try:
        r = stub.Login(
            dfs_pb2.LoginRequest(username=args.username, password=args.password),
            timeout=10
        )
        if r.success:
            AuthClient.save_token(r.token)
            print("Autenticación exitosa")
        else:
            print(r.message or "Error: credenciales inválidas")
            sys.exit(1)
    except grpc.RpcError as e:
        print(f"Error de conexión: {e.details()}")
        sys.exit(1)
    finally:
        ch.close()


def cmd_logout(args):
    AuthClient.clear_token()
    print("Sesión cerrada")


def cmd_ls(args):
    token = _require_auth()
    directory = args.path or '/'
    stub, ch = _nn_stub()
    try:
        r = stub.ListFiles(
            dfs_pb2.ListFilesRequest(token=token, directory=directory),
            timeout=10
        )
        if not r.success:
            print(r.message)
            sys.exit(1)

        if not r.directories and not r.files:
            print(f"(directorio vacío: {directory})")
            return

        for d in sorted(r.directories):
            print(f"d  {d}/")
        for f in sorted(r.files, key=lambda x: x.filepath):
            print(
                f"-  {f.filepath:<55} {_human_size(f.filesize):>10}  "
                f"{f.created_at[:10]}  [{f.block_count} bloques]"
            )
    except grpc.RpcError as e:
        print(f"Error de conexión: {e.details()}")
        sys.exit(1)
    finally:
        ch.close()


def cmd_mkdir(args):
    token = _require_auth()
    stub, ch = _nn_stub()
    try:
        r = stub.MakeDir(
            dfs_pb2.MakeDirRequest(token=token, path=args.path), timeout=10
        )
        print(r.message)
        if not r.success:
            sys.exit(1)
    except grpc.RpcError as e:
        print(f"Error de conexión: {e.details()}")
        sys.exit(1)
    finally:
        ch.close()


def cmd_rmdir(args):
    token = _require_auth()
    stub, ch = _nn_stub()
    try:
        r = stub.RemoveDir(
            dfs_pb2.RemoveDirRequest(
                token=token,
                path=args.path,
                recursive=getattr(args, 'recursive', False),
            ),
            timeout=15,
        )
        print(r.message)
        if not r.success:
            sys.exit(1)
    except grpc.RpcError as e:
        print(f"Error de conexión: {e.details()}")
        sys.exit(1)
    finally:
        ch.close()


def cmd_rm(args):
    token = _require_auth()
    stub, ch = _nn_stub()
    try:
        r = stub.DeleteFile(
            dfs_pb2.DeleteFileRequest(token=token, filepath=args.filepath), timeout=10
        )
        print(r.message)
        if not r.success:
            sys.exit(1)
    except grpc.RpcError as e:
        print(f"Error de conexión: {e.details()}")
        sys.exit(1)
    finally:
        ch.close()


def cmd_put(args):
    """
    Subir un archivo local al DFS.
    Flujo:
      1. Calcular tamaño y número de bloques
      2. Solicitar asignación al NameNode (PutFile)
      3. Para cada bloque: dividir, subir con pipeline de replicación, confirmar
    """
    token      = _require_auth()
    local_path = args.local_path
    remote_path = args.remote_path

    if not os.path.isfile(local_path):
        print(f"Error: archivo no encontrado: {local_path}")
        sys.exit(1)

    filesize    = os.path.getsize(local_path)
    block_count = get_block_count(filesize, config.BLOCK_SIZE)

    print(f"Subiendo: {local_path} ({_human_size(filesize)}, {block_count} bloque(s))")
    print(f"  → {remote_path}")

    stub, ch = _nn_stub()
    try:
        # Solicitar asignación de bloques al NameNode
        put_resp = stub.PutFile(
            dfs_pb2.PutFileRequest(
                token=token,
                filepath=remote_path,
                filesize=filesize,
                block_count=block_count,
            ),
            timeout=30,
        )
    except grpc.RpcError as e:
        print(f"Error de conexión al NameNode: {e.details()}")
        sys.exit(1)
    finally:
        ch.close()

    if not put_resp.success:
        print(f"Error: {put_resp.message}")
        sys.exit(1)

    file_id     = put_resp.file_id
    assignments = put_resp.assignments   # lista de BlockAssignment

    # Subir cada bloque
    confirmed = 0
    try:
        for block_index, block_data, checksum in split_file(local_path, config.BLOCK_SIZE):
            assignment = assignments[block_index]
            block_id   = assignment.block_id
            nodes      = list(assignment.datanodes)  # pipeline: nodo[0] = primario

            if not nodes:
                print(f"Error: no hay DataNodes asignados para bloque {block_index}")
                sys.exit(1)

            primary  = nodes[0]
            pipeline = nodes[1:]    # nodos para replicación pipeline

            print(
                f"  Bloque {block_index+1}/{block_count}: {_human_size(len(block_data))} "
                f"→ {primary.node_id} (pipeline: {[n.node_id for n in pipeline]})",
                end=' ... ', flush=True
            )

            # Subir con reintentos y pipeline
            try:
                upload_block(
                    primary.host, primary.port,
                    block_id, block_data, checksum,
                    pipeline_nodes=pipeline,
                )
            except IOError as e:
                print(f"\nError: {e}")
                sys.exit(1)

            # Confirmar bloque al NameNode
            stub2, ch2 = _nn_stub()
            try:
                confirm_resp = stub2.ConfirmBlock(
                    dfs_pb2.ConfirmBlockRequest(
                        token=token,
                        file_id=file_id,
                        block_id=block_id,
                        block_index=block_index,
                        block_size=len(block_data),
                        checksum=checksum,
                        node_ids=[n.node_id for n in nodes],
                    ),
                    timeout=10,
                )
                if not confirm_resp.success:
                    print(f"\nAdvertencia: confirmación fallida para bloque {block_id}: {confirm_resp.message}")
                else:
                    confirmed += 1
                    print("OK")
            except grpc.RpcError as e:
                print(f"\nError al confirmar bloque {block_id}: {e.details()}")
            finally:
                ch2.close()

    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)

    print(f"\n✓ Archivo subido exitosamente: {remote_path}")
    print(f"  {confirmed}/{block_count} bloques confirmados | file_id={file_id}")


def cmd_get(args):
    """
    Descargar un archivo del DFS a una ruta local.
    Flujo:
      1. Solicitar ubicaciones de bloques al NameNode (GetFile)
      2. Para cada bloque: descargar desde primera réplica disponible (fallback a otras)
      3. Verificar SHA-256 de cada bloque
      4. Ensamblar bloques en archivo local
    """
    token       = _require_auth()
    remote_path = args.remote_path
    local_path  = args.local_path

    # Si es un directorio, usar el nombre del archivo remoto
    if os.path.isdir(local_path):
        filename   = os.path.basename(remote_path)
        local_path = os.path.join(local_path, filename)

    stub, ch = _nn_stub()
    try:
        get_resp = stub.GetFile(
            dfs_pb2.GetFileRequest(token=token, filepath=remote_path),
            timeout=30,
        )
    except grpc.RpcError as e:
        print(f"Error de conexión al NameNode: {e.details()}")
        sys.exit(1)
    finally:
        ch.close()

    if not get_resp.success:
        print(f"Error: {get_resp.message}")
        sys.exit(1)

    filesize    = get_resp.filesize
    locations   = get_resp.locations    # lista de BlockLocation
    block_count = len(locations)

    print(f"Descargando: {remote_path} ({_human_size(filesize)}, {block_count} bloque(s))")
    print(f"  → {local_path}")

    downloaded_blocks = []   # lista de (block_index, data)

    for loc in sorted(locations, key=lambda x: x.block_index):
        block_id  = loc.block_id
        block_idx = loc.block_index
        checksum  = loc.checksum
        nodes     = list(loc.datanodes)

        print(
            f"  Bloque {block_idx+1}/{block_count}: "
            f"réplicas={[n.node_id for n in nodes]}",
            end=' ... ', flush=True
        )

        data = None
        last_error = None

        # Intentar cada réplica hasta obtener el bloque
        for node in nodes:
            try:
                data = download_block(node.host, node.port, block_id, checksum)

                # Reportar bloque corrupto al NameNode si es necesario (manejado por IOError de checksum)
                break
            except IOError as e:
                last_error = e
                err_str = str(e)
                if 'corrupto' in err_str or 'checksum' in err_str:
                    # Reportar corrupción al NameNode
                    try:
                        stub3, ch3 = _nn_stub()
                        token_curr = AuthClient.get_token() or token
                        stub3.ReportCorruptBlock(
                            dfs_pb2.ReportCorruptBlockRequest(
                                token=token_curr,
                                block_id=block_id,
                                node_id=node.node_id,
                            ),
                            timeout=5,
                        )
                        ch3.close()
                    except Exception:
                        pass
                    print(f"\n  Advertencia: bloque corrupto en {node.node_id}, probando réplica...")
                else:
                    print(f"\n  Advertencia: nodo {node.node_id} no disponible, probando réplica...")
                continue

        if data is None:
            print(f"\nError: no se pudo descargar bloque {block_id}: {last_error}")
            sys.exit(1)

        downloaded_blocks.append((block_idx, data))
        print("OK")

    # Ensamblar archivo
    try:
        total_bytes = assemble_blocks(downloaded_blocks, local_path)
        print(f"\n✓ Archivo descargado exitosamente: {local_path} ({_human_size(total_bytes)})")
    except Exception as e:
        print(f"Error al ensamblar bloques: {e}")
        sys.exit(1)


# ══════════════════════════════════════════════════════════════════════════════
# Status del clúster
# ══════════════════════════════════════════════════════════════════════════════
def cmd_status(args):
    """
    Muestra el estado del clúster MiniDFS:
    - DataNodes activos/inactivos con bloques y espacio libre
    - Total de archivos y bloques en el sistema
    """
    token = _require_auth()
    stub, ch = _nn_stub()
    try:
        r = stub.GetClusterStatus(
            dfs_pb2.ClusterStatusRequest(token=token), timeout=10
        )
    except grpc.RpcError as e:
        print(f"Error de conexión al NameNode: {e.details()}")
        sys.exit(1)
    finally:
        ch.close()

    if not r.success:
        print(f"Error: {r.message}")
        sys.exit(1)

    print()
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║            MiniDFS – Estado del Clúster                     ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print()
    print(f"  Archivos activos : {r.total_files}")
    print(f"  Bloques únicos   : {r.total_blocks}")
    print(f"  DataNodes        : {len(r.datanodes)}")
    print()
    print(f"  {'ID':<8} {'Host':<18} {'Puerto':>6}  {'Estado':<10} {'Bloques':>8}  {'Espacio libre':>14}  Último HB")
    print("  " + "─" * 78)
    for dn in r.datanodes:
        estado = "✅ activo" if dn.status == 'active' else "❌ inactivo"
        espacio = _human_size(dn.available_space) if dn.available_space > 0 else "N/A"
        hb = dn.last_heartbeat[:19].replace('T', ' ') if dn.last_heartbeat else 'N/A'
        print(
            f"  {dn.node_id:<8} {dn.host:<18} {dn.port:>6}  {estado:<14} {dn.block_count:>8}  "
            f"{espacio:>14}  {hb}"
        )
    print()


# ══════════════════════════════════════════════════════════════════════════════
# Parser principal
# ══════════════════════════════════════════════════════════════════════════════
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='minidfs',
        description='MiniDFS – Sistema de Archivos Distribuidos por Bloques',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python cli.py register andres pass123
  python cli.py login    andres pass123
  python cli.py mkdir    /datos
  python cli.py put      ./video.mp4   /datos/video.mp4
  python cli.py ls       /datos
  python cli.py get      /datos/video.mp4  ./descargado.mp4
  python cli.py rm       /datos/video.mp4
  python cli.py rmdir    /datos
  python cli.py rmdir -r /datos          # elimina recursivamente
  python cli.py status                   # estado del clúster
  python cli.py logout
        """
    )
    sub = parser.add_subparsers(dest='command', metavar='COMANDO')
    sub.required = True

    # register
    p = sub.add_parser('register', help='Registrar nuevo usuario')
    p.add_argument('username', help='Nombre de usuario')
    p.add_argument('password', help='Contraseña')
    p.set_defaults(func=cmd_register)

    # login
    p = sub.add_parser('login', help='Iniciar sesión')
    p.add_argument('username', help='Nombre de usuario')
    p.add_argument('password', help='Contraseña')
    p.set_defaults(func=cmd_login)

    # logout
    p = sub.add_parser('logout', help='Cerrar sesión')
    p.set_defaults(func=cmd_logout)

    # put
    p = sub.add_parser('put', help='Subir archivo al DFS')
    p.add_argument('local_path',  help='Ruta local del archivo')
    p.add_argument('remote_path', help='Ruta remota destino en el DFS')
    p.set_defaults(func=cmd_put)

    # get
    p = sub.add_parser('get', help='Descargar archivo del DFS')
    p.add_argument('remote_path', help='Ruta remota del archivo en el DFS')
    p.add_argument('local_path',  nargs='?', default='.', help='Ruta local destino')
    p.set_defaults(func=cmd_get)

    # ls
    p = sub.add_parser('ls', help='Listar archivos y directorios')
    p.add_argument('path', nargs='?', default='/', help='Directorio a listar')
    p.set_defaults(func=cmd_ls)

    # rm
    p = sub.add_parser('rm', help='Eliminar archivo del DFS')
    p.add_argument('filepath', help='Ruta del archivo a eliminar')
    p.set_defaults(func=cmd_rm)

    # mkdir
    p = sub.add_parser('mkdir', help='Crear directorio')
    p.add_argument('path', help='Ruta del directorio')
    p.set_defaults(func=cmd_mkdir)

    # rmdir
    p = sub.add_parser('rmdir', help='Eliminar directorio (use -r para recursivo)')
    p.add_argument('path', help='Ruta del directorio')
    p.add_argument('-r', '--recursive', action='store_true',
                   help='Eliminar recursivamente el directorio y su contenido')
    p.set_defaults(func=cmd_rmdir)

    # status
    p = sub.add_parser('status', help='Mostrar estado del clúster MiniDFS')
    p.set_defaults(func=cmd_status)

    return parser


def main():
    parser = build_parser()
    args   = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
