"""
namenode/auth_service.py
Servicio de autenticación JWT para el NameNode.
Soporta registro y login de usuarios con contraseñas hasheadas con bcrypt.
"""
import bcrypt
import jwt
import uuid
from datetime import datetime, timedelta
import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import config

logger = logging.getLogger(__name__)


class AuthService:
    """Gestiona registro, login y validación de tokens JWT."""

    def __init__(self, metadata_store):
        self.store = metadata_store

    # ─── Registro ──────────────────────────────────────────────────────────────
    def register_user(self, username: str, password: str):
        """
        Registra un nuevo usuario.
        Retorna: (success: bool, message: str)
        """
        if not username or not password:
            return False, "Error: usuario y contraseña son requeridos"

        if len(password) < 4:
            return False, "Error: contraseña debe tener al menos 4 caracteres"

        existing = self.store.get_user_by_username(username)
        if existing:
            return False, f"Error: usuario '{username}' ya existe"

        password_hash = bcrypt.hashpw(
            password.encode('utf-8'), bcrypt.gensalt()
        ).decode('utf-8')

        user_id = str(uuid.uuid4())
        try:
            self.store.create_user(user_id, username, password_hash)
            # Crear directorio raíz del usuario
            self.store.create_directory(str(uuid.uuid4()), user_id, '/')
            logger.info(f"Usuario registrado: {username} (id={user_id})")
            return True, "Usuario registrado exitosamente"
        except Exception as e:
            logger.error(f"Error registrando usuario {username}: {e}")
            return False, f"Error al registrar usuario: {str(e)}"

    # ─── Login ─────────────────────────────────────────────────────────────────
    def authenticate(self, username: str, password: str):
        """
        Autentica un usuario.
        Retorna: (success: bool, token_o_error: str)
        """
        user = self.store.get_user_by_username(username)
        if not user:
            logger.warning(f"Login fallido: usuario no encontrado: {username}")
            return False, "Error: credenciales inválidas"

        try:
            valid = bcrypt.checkpw(
                password.encode('utf-8'),
                user['password_hash'].encode('utf-8')
            )
        except Exception:
            return False, "Error: credenciales inválidas"

        if not valid:
            logger.warning(f"Login fallido: contraseña incorrecta para {username}")
            return False, "Error: credenciales inválidas"

        token = self._generate_token(user['user_id'], username)
        logger.info(f"Login exitoso: {username}")
        return True, token

    # ─── Validación ────────────────────────────────────────────────────────────
    def validate_token(self, token: str):
        """
        Valida un JWT token.
        Retorna: (valid: bool, user_id_o_error: str)
        """
        if not token:
            return False, "Error: sesión no válida, por favor inicie sesión nuevamente"
        try:
            payload = jwt.decode(token, config.JWT_SECRET, algorithms=['HS256'])
            user_id = payload.get('user_id')
            if not user_id:
                return False, "Error: token inválido"

            user = self.store.get_user_by_id(user_id)
            if not user:
                return False, "Error: usuario no encontrado"

            return True, user_id

        except jwt.ExpiredSignatureError:
            return False, "Error: sesión no válida, por favor inicie sesión nuevamente"
        except jwt.InvalidTokenError:
            return False, "Error: token inválido"

    # ─── Privado ───────────────────────────────────────────────────────────────
    def _generate_token(self, user_id: str, username: str) -> str:
        payload = {
            'user_id':  user_id,
            'username': username,
            'exp': datetime.utcnow() + timedelta(hours=config.JWT_EXPIRY_HOURS),
            'iat': datetime.utcnow(),
        }
        return jwt.encode(payload, config.JWT_SECRET, algorithm='HS256')
