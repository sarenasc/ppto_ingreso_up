"""
routes/auth.py — Autenticación y gestión de usuarios.

Endpoints:
  GET/POST /login              → pantalla de login
  GET      /logout             → cierra sesión
  GET      /api/usuarios       → lista usuarios (sin hash)
  POST     /api/usuarios       → crea usuario
  DELETE   /api/usuarios/<id>  → elimina usuario
  PUT      /api/usuarios/<id>/password → cambia contraseña
"""

from functools import wraps
from flask     import (Blueprint, render_template, request, redirect,
                       url_for, session, jsonify, flash)
from werkzeug.security import generate_password_hash, check_password_hash
from datetime  import datetime

from config   import CFG
from database import get_db

bp = Blueprint('auth', __name__)


# ── Decorador de protección ───────────────────────────────────────────
def login_required(f):
    """Redirige a /login si el usuario no tiene sesión activa."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('user_id'):
            # Guarda la URL que quería visitar para redirigir después del login
            session['next'] = request.url
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated


# ── Login / Logout ────────────────────────────────────────────────────
@bp.route('/login', methods=['GET', 'POST'])
def login():
    # Si ya tiene sesión, ir al inicio
    if session.get('user_id'):
        return redirect(url_for('index'))

    error = None
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password =  request.form.get('password') or ''

        if not username or not password:
            error = 'Ingresa usuario y contraseña.'
        else:
            user = _get_user_by_username(username)
            if user and user.get('activo') and check_password_hash(user['password_hash'], password):
                session.clear()
                session['user_id']   = user['id']
                session['username']  = user['username']
                session.permanent    = True
                # Redirigir a donde quería ir antes del login
                next_url = session.pop('next', None)
                return redirect(next_url or url_for('index'))
            else:
                error = 'Usuario o contraseña incorrectos.'

    return render_template('login.html', error=error)


@bp.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('auth.login'))


# ── API: Gestión de usuarios ──────────────────────────────────────────


@bp.route('/api/me')
def me():
    """Devuelve el usuario de sesión actual (para el topbar del frontend)."""
    if not session.get('user_id'):
        return jsonify({'username': None}), 401
    return jsonify({'username': session.get('username', '')})

@bp.route('/api/usuarios', methods=['GET'])
@login_required
def get_usuarios():
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT id, username, activo,
                   CAST(creado_en AS VARCHAR(16)) AS creado_en
            FROM {schema}.ppto_usuarios
            ORDER BY username
        """)
        rows = db.fetchall_dicts(cur)
    for r in rows:
        if r.get('creado_en') and not isinstance(r['creado_en'], str):
            r['creado_en'] = str(r['creado_en'])[:16]
    return jsonify(rows)


@bp.route('/api/usuarios', methods=['POST'])
@login_required
def create_usuario():
    data     = request.json or {}
    username = (data.get('username') or '').strip().lower()
    password =  data.get('password') or ''

    if not username:
        return jsonify({'error': 'El nombre de usuario es requerido.'}), 400
    if len(password) < 4:
        return jsonify({'error': 'La contraseña debe tener al menos 4 caracteres.'}), 400
    if not username.replace('_','').replace('.','').isalnum():
        return jsonify({'error': 'El usuario solo puede contener letras, números, _ y .'}), 400

    schema = CFG['DB_SCHEMA']
    db     = get_db()
    now    = datetime.now()
    phash  = generate_password_hash(password)

    try:
        with db.get_conn() as conn:
            cur = conn.cursor()
            # Verificar que no exista
            cur.execute(db.norm(
                f"SELECT id FROM {schema}.ppto_usuarios WHERE username=?"
            ), (username,))
            if cur.fetchone():
                return jsonify({'error': f"El usuario '{username}' ya existe."}), 409

            cur.execute(db.norm(f"""
                INSERT INTO {schema}.ppto_usuarios
                    (username, password_hash, activo, creado_en)
                VALUES (?, ?, 1, ?)
            """), (username, phash, now))
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/api/usuarios/<int:uid>', methods=['DELETE'])
@login_required
def delete_usuario(uid):
    # No permitir que el usuario se elimine a sí mismo
    if uid == session.get('user_id'):
        return jsonify({'error': 'No puedes eliminar tu propio usuario.'}), 400

    schema = CFG['DB_SCHEMA']
    db     = get_db()

    # Verificar que quede al menos 1 usuario activo después
    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(db.norm(
            f"SELECT COUNT(*) FROM {schema}.ppto_usuarios WHERE activo=? AND id<>?"
        ), (1, uid))
        count = cur.fetchone()[0]
        if count == 0:
            return jsonify({'error': 'Debe quedar al menos un usuario activo.'}), 400
        cur.execute(db.norm(
            f"DELETE FROM {schema}.ppto_usuarios WHERE id=?"
        ), (uid,))
    return jsonify({'ok': True})


@bp.route('/api/usuarios/<int:uid>/password', methods=['PUT'])
@login_required
def change_password(uid):
    data     = request.json or {}
    password = data.get('password') or ''

    if len(password) < 4:
        return jsonify({'error': 'La contraseña debe tener al menos 4 caracteres.'}), 400

    schema = CFG['DB_SCHEMA']
    db     = get_db()
    phash  = generate_password_hash(password)

    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(db.norm(
            f"UPDATE {schema}.ppto_usuarios SET password_hash=? WHERE id=?"
        ), (phash, uid))
    return jsonify({'ok': True})


# ── Helper interno ────────────────────────────────────────────────────
def _get_user_by_username(username: str) -> dict | None:
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    try:
        with db.get_conn() as conn:
            cur = conn.cursor()
            cur.execute(db.norm(f"""
                SELECT id, username, password_hash, activo
                FROM {schema}.ppto_usuarios
                WHERE username = ?
            """), (username.strip().lower(),))
            row = cur.fetchone()
            if row:
                return dict(zip(['id','username','password_hash','activo'], row))
    except Exception:
        pass
    return None


def ensure_default_user() -> None:
    """
    Si no existe ningún usuario en la BD, crea el usuario por defecto:
      usuario:    admin
      contraseña: admin1234

    Imprime un aviso en consola para que el operador cambie la clave.
    """
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    try:
        with db.get_conn() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {schema}.ppto_usuarios")
            count = cur.fetchone()[0]
            if count == 0:
                phash = generate_password_hash('admin1234')
                cur.execute(db.norm(f"""
                    INSERT INTO {schema}.ppto_usuarios
                        (username, password_hash, activo, creado_en)
                    VALUES (?, ?, 1, ?)
                """), ('admin', phash, datetime.now()))
                print("\n" + "="*55)
                print("  USUARIO INICIAL CREADO")
                print("  Usuario:    admin")
                print("  Contraseña: admin1234")
                print("  ⚠  Cámbiala desde ⚙️ Configuración → Usuarios")
                print("="*55 + "\n")
    except Exception as e:
        print(f"[WARN] No se pudo verificar usuarios iniciales: {e}")
