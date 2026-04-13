"""
routes/exportable.py — Endpoints de % Exportable por Exportadora + Especie.

  GET  /api/exportable             → lista todos los registros
  POST /api/exportable             → guarda porcentajes (array de {exportadora, especie, porcentaje})
  GET  /api/exportable/exportadoras → combinaciones exportadora+especie desde estimacion con % actual
"""

from datetime import datetime
from flask import Blueprint, request, jsonify

from config      import CFG
from routes.auth import login_required
from database    import get_db

bp = Blueprint('exportable', __name__, url_prefix='/api/exportable')


@bp.route('', methods=['GET'])
@login_required
def get_exportable():
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT id, exportadora, especie, porcentaje,
                   CAST(actualizado_en AS VARCHAR(10)) AS actualizado_en
            FROM {schema}.ppto_exportable_pct
            ORDER BY exportadora, especie
        """)
        rows = db.fetchall_dicts(cur)
    for r in rows:
        if r.get('actualizado_en') and not isinstance(r['actualizado_en'], str):
            r['actualizado_en'] = str(r['actualizado_en'])[:10]
    return jsonify(rows)


@bp.route('', methods=['POST'])
@login_required
def save_exportable():
    """
    Recibe array de { exportadora, especie, porcentaje }.
    Hace upsert por (exportadora, especie).
    """
    items  = request.json or []
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    now    = datetime.now()
    with db.get_conn() as conn:
        cur = conn.cursor()
        for item in items:
            exportadora = (item.get('exportadora') or '').strip()
            especie     = (item.get('especie')     or '').strip()
            if not exportadora or not especie:
                continue
            db.upsert_exportable(
                cur, schema,
                exportadora,
                especie,
                float(item.get('porcentaje', 0)),
                now,
            )
    return jsonify({'ok': True})


@bp.route('/exportadoras', methods=['GET'])
@login_required
def get_exportadoras():
    """
    Devuelve combinaciones exportadora+especie desde la estimacion activa
    enriquecidas con el % exportable actual de cada combinacion.
    """
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()

        cur.execute(f"""
            SELECT DISTINCT exportadora, especie
            FROM {schema}.ppto_estimacion
            WHERE exportadora IS NOT NULL AND especie IS NOT NULL
            ORDER BY exportadora, especie
        """)
        combinaciones = db.fetchall_dicts(cur)

        cur.execute(f"""
            SELECT exportadora, especie, porcentaje
            FROM {schema}.ppto_exportable_pct
        """)
        pcts = {
            (r['exportadora'], r['especie']): r['porcentaje']
            for r in db.fetchall_dicts(cur)
        }

    resultado = []
    for c in combinaciones:
        key = (c['exportadora'], c['especie'])
        resultado.append({
            'exportadora': c['exportadora'],
            'especie':     c['especie'],
            'porcentaje':  pcts.get(key, 0.8) or 0,
        })

    return jsonify(resultado)
