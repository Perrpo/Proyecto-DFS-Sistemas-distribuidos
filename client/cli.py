#!/usr/bin/env python3
"""
client/cli.py
Interfaz de línea de comandos de MiniDFS.
Comandos: register, login, logout, put, get, ls, rm, mkdir, rmdir
"""
import argparse
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import config

from auth_client import AuthClient
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
            dfs_pb2.RemoveDirRequest(token=token, path=args.path), timeout=10
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
    """Subir archivo al DFS – implementación completa en Día 2."""
    _require_auth()
    if not os.path.isfile(args.local_path):
        print(f"Error: archivo no encontrado: {args.local_path}")
        sys.exit(1)
    print("Error: comando 'put' disponible a partir del Día 2 (Viernes 22 Mayo)")
    sys.exit(1)


def cmd_get(args):
    """Descargar archivo del DFS – implementación completa en Día 2."""
    _require_auth()
    print("Error: comando 'get' disponible a partir del Día 2 (Viernes 22 Mayo)")
    sys.exit(1)


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
    p = sub.add_parser('rmdir', help='Eliminar directorio vacío')
    p.add_argument('path', help='Ruta del directorio')
    p.set_defaults(func=cmd_rmdir)

    return parser


def main():
    parser = build_parser()
    args   = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
