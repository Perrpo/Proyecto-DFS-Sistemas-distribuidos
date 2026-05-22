"""
client/auth_client.py
Gestión local del token JWT del usuario autenticado.
El token se almacena en ~/.minidfs_token
"""
import os
import json

TOKEN_FILE = os.path.expanduser("~/.minidfs_token")


class AuthClient:

    @staticmethod
    def save_token(token: str):
        with open(TOKEN_FILE, 'w') as f:
            json.dump({'token': token}, f)

    @staticmethod
    def get_token() -> str | None:
        if not os.path.exists(TOKEN_FILE):
            return None
        try:
            with open(TOKEN_FILE, 'r') as f:
                return json.load(f).get('token')
        except Exception:
            return None

    @staticmethod
    def clear_token():
        if os.path.exists(TOKEN_FILE):
            os.remove(TOKEN_FILE)

    @staticmethod
    def is_logged_in() -> bool:
        return AuthClient.get_token() is not None
