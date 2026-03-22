"""
routes/unitarios.py — Endpoints de Precios Unitarios.

  GET  /api/unitarios → lista todas las especies con sus precios
  POST /api/unitarios → guarda precios (array de {especie, packing, frio})
"""

from datetime import datetime
from flask import Blueprint, request, jsonify

from config   import CFG
from routes.auth import login_required
from database import get_db

bp = Blueprint('unitarios', __name__, url_prefix='/api/unitarios')


@login_required
@bp.route('', methods=['GET'])
def get_unitarios():
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT id, especie, precio_usd_packing, precio_usd_frio,
                   CAST(actualizado_en AS VARCHAR(10)) AS actualizado_en
            FROM {schema}.ppto_unitarios
            ORDER BY especie
        """)
        rows = db.fetchall_dicts(cur)
    for r in rows:
        if r.get('actualizado_en') and not isinstance(r['actualizado_en'], str):
            r['actualizado_en'] = str(r['actualizado_en'])[:10]
    return jsonify(rows)


@login_required
@bp.route('', methods=['POST'])
def save_unitarios():
    items  = request.json or []
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    now    = datetime.now()
    with db.get_conn() as conn:
        cur = conn.cursor()
        for item in items:
            db.upsert_unitario(
                cur, schema,
                item['especie'],
                float(item.get('precio_usd_packing', 0)),
                float(item.get('precio_usd_frio',    0)),
                now,
            )
    return jsonify({'ok': True})
