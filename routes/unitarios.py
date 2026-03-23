"""
routes/unitarios.py — Precios Unitarios por Exportadora + Especie.

  GET  /api/unitarios              → lista todos los registros
  POST /api/unitarios              → guarda precios (upsert por exportadora+especie)
  GET  /api/unitarios/exportadoras → lista exportadoras disponibles (desde estimacion)
"""

from datetime import datetime
from flask import Blueprint, request, jsonify

from config      import CFG
from routes.auth import login_required
from database    import get_db

bp = Blueprint('unitarios', __name__, url_prefix='/api/unitarios')


@bp.route('', methods=['GET'])
@login_required
def get_unitarios():
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT id, exportadora, especie,
                   precio_usd_packing, precio_usd_frio,
                   CAST(actualizado_en AS VARCHAR(10)) AS actualizado_en
            FROM {schema}.ppto_unitarios
            ORDER BY exportadora, especie
        """)
        rows = db.fetchall_dicts(cur)
    for r in rows:
        if r.get('actualizado_en') and not isinstance(r['actualizado_en'], str):
            r['actualizado_en'] = str(r['actualizado_en'])[:10]
    return jsonify(rows)


@bp.route('', methods=['POST'])
@login_required
def save_unitarios():
    """
    Recibe array de { exportadora, especie, precio_usd_packing, precio_usd_frio }.
    Hace upsert por (exportadora, especie).
    """
    items  = request.json or []
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    now    = datetime.now()
    engine = CFG['DB_ENGINE'].lower()

    with db.get_conn() as conn:
        cur = conn.cursor()
        for item in items:
            exportadora = (item.get('exportadora') or '').strip()
            especie     = (item.get('especie')     or '').strip()
            packing     = float(item.get('precio_usd_packing', 0))
            frio        = float(item.get('precio_usd_frio',    0))

            if not exportadora or not especie:
                continue

            if engine == 'sqlserver':
                cur.execute(db.norm(f"""
                    MERGE {schema}.ppto_unitarios AS t
                    USING (SELECT ? AS exportadora, ? AS especie) AS s
                      ON t.exportadora=s.exportadora AND t.especie=s.especie
                    WHEN MATCHED THEN
                        UPDATE SET precio_usd_packing=?, precio_usd_frio=?, actualizado_en=?
                    WHEN NOT MATCHED THEN
                        INSERT (exportadora, especie, precio_usd_packing, precio_usd_frio, actualizado_en)
                        VALUES (?, ?, ?, ?, ?);
                """), (exportadora, especie, packing, frio, now,
                       exportadora, especie, packing, frio, now))

            elif engine == 'postgresql':
                cur.execute(db.norm(f"""
                    INSERT INTO {schema}.ppto_unitarios
                        (exportadora, especie, precio_usd_packing, precio_usd_frio, actualizado_en)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (exportadora, especie) DO UPDATE SET
                        precio_usd_packing = EXCLUDED.precio_usd_packing,
                        precio_usd_frio    = EXCLUDED.precio_usd_frio,
                        actualizado_en     = EXCLUDED.actualizado_en
                """), (exportadora, especie, packing, frio, now))

            elif engine == 'mysql':
                cur.execute(db.norm(f"""
                    INSERT INTO {schema}.ppto_unitarios
                        (exportadora, especie, precio_usd_packing, precio_usd_frio, actualizado_en)
                    VALUES (?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE
                        precio_usd_packing = VALUES(precio_usd_packing),
                        precio_usd_frio    = VALUES(precio_usd_frio),
                        actualizado_en     = VALUES(actualizado_en)
                """), (exportadora, especie, packing, frio, now))

    return jsonify({'ok': True})


@bp.route('/exportadoras', methods=['GET'])
@login_required
def get_exportadoras():
    """Devuelve exportadoras y especies disponibles desde la estimacion activa."""
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()
        # Combinaciones exportadora+especie existentes en la estimacion
        cur.execute(f"""
            SELECT DISTINCT exportadora, especie
            FROM {schema}.ppto_estimacion
            WHERE exportadora IS NOT NULL AND especie IS NOT NULL
            ORDER BY exportadora, especie
        """)
        combinaciones = db.fetchall_dicts(cur)

        # Precios actuales
        cur.execute(f"""
            SELECT exportadora, especie, precio_usd_packing, precio_usd_frio
            FROM {schema}.ppto_unitarios
            ORDER BY exportadora, especie
        """)
        precios = {
            (r['exportadora'], r['especie']): r
            for r in db.fetchall_dicts(cur)
        }

    # Enriquecer combinaciones con precios existentes
    resultado = []
    for c in combinaciones:
        key   = (c['exportadora'], c['especie'])
        precio = precios.get(key, {})
        resultado.append({
            'exportadora':      c['exportadora'],
            'especie':          c['especie'],
            'precio_usd_packing': precio.get('precio_usd_packing', 0) or 0,
            'precio_usd_frio':    precio.get('precio_usd_frio',    0) or 0,
        })

    return jsonify(resultado)
