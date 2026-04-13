"""
routes/ingreso.py — Ingreso manual de montos USD por mes/especie/exportadora.

  GET  /api/ingreso                  → lista todos los registros
  POST /api/ingreso                  → guarda o actualiza un registro
  DELETE /api/ingreso/<id>           → elimina un registro
  GET  /api/ingreso/resumen          → totales agrupados (igual que ppto pero desde ingreso manual)
  GET  /api/ingreso/opciones         → lista de exportadoras, especies y meses disponibles
"""

from datetime import datetime
from flask import Blueprint, request, jsonify
from routes.auth import login_required
from config      import CFG
from database    import get_db
from services.calculos import MESES_NOMBRE, ORDEN_MESES_TEMPORADA, sort_mes_temporada

bp = Blueprint('ingreso', __name__, url_prefix='/api/ingreso')


@bp.route('', methods=['GET'])
@login_required
def get_ingresos():
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT id, temporada, exportadora, especie, mes,
                   usd_packing, usd_frio, usd_total, tc,
                   CAST(actualizado_en AS VARCHAR(19)) AS actualizado_en,
                   usuario
            FROM {schema}.ppto_ingreso_usd
            ORDER BY exportadora, especie, mes
        """)
        rows = db.fetchall_dicts(cur)
    # Normalizar fechas
    for r in rows:
        if r.get('actualizado_en') and not isinstance(r['actualizado_en'], str):
            r['actualizado_en'] = str(r['actualizado_en'])[:19]
    return jsonify(rows)


@bp.route('', methods=['POST'])
@login_required
def save_ingreso():
    """
    Guarda o actualiza un registro de ingreso manual.
    Si ya existe (exportadora + especie + mes + temporada), actualiza.
    Si no existe, inserta.
    Body: { temporada, exportadora, especie, mes, usd_packing, usd_frio }
    """
    data        = request.json or {}
    temporada   = (data.get('temporada')   or '').strip()
    exportadora = (data.get('exportadora') or '').strip()
    especie     = (data.get('especie')     or '').strip()
    mes         = data.get('mes')

    if not exportadora:
        return jsonify({'error': 'exportadora es requerida'}), 400
    if not especie:
        return jsonify({'error': 'especie es requerida'}), 400
    if mes is None:
        return jsonify({'error': 'mes es requerido'}), 400

    usd_packing = float(data.get('usd_packing') or 0)
    usd_frio    = float(data.get('usd_frio')    or 0)
    usd_total   = usd_packing + usd_frio
    tc_raw      = data.get('tc')
    tc          = float(tc_raw) if tc_raw not in (None, '', 0, '0') else None

    schema  = CFG['DB_SCHEMA']
    db      = get_db()
    now     = datetime.now()
    engine  = CFG['DB_ENGINE'].lower()

    from flask import session
    usuario = session.get('username', 'sistema')

    try:
        with db.get_conn() as conn:
            cur = conn.cursor()

            # Verificar si ya existe registro para esa combinacion
            cur.execute(db.norm(f"""
                SELECT id FROM {schema}.ppto_ingreso_usd
                WHERE exportadora=? AND especie=? AND mes=? AND temporada=?
            """), (exportadora, especie, int(mes), temporada))
            existing = cur.fetchone()

            if existing:
                cur.execute(db.norm(f"""
                    UPDATE {schema}.ppto_ingreso_usd
                    SET usd_packing=?, usd_frio=?, usd_total=?, tc=?,
                        actualizado_en=?, usuario=?
                    WHERE id=?
                """), (usd_packing, usd_frio, usd_total, tc, now, usuario, existing[0]))
                record_id = existing[0]
            else:
                if engine == 'sqlserver':
                    cur.execute(db.norm(f"""
                        INSERT INTO {schema}.ppto_ingreso_usd
                            (temporada, exportadora, especie, mes,
                             usd_packing, usd_frio, usd_total, tc, actualizado_en, usuario)
                        OUTPUT INSERTED.id
                        VALUES (?,?,?,?,?,?,?,?,?,?)
                    """), (temporada, exportadora, especie, int(mes),
                           usd_packing, usd_frio, usd_total, tc, now, usuario))
                    row = cur.fetchone()
                    record_id = int(row[0]) if row and row[0] is not None else 0
                elif engine == 'mysql':
                    cur.execute(db.norm(f"""
                        INSERT INTO {schema}.ppto_ingreso_usd
                            (temporada, exportadora, especie, mes,
                             usd_packing, usd_frio, usd_total, tc, actualizado_en, usuario)
                        VALUES (?,?,?,?,?,?,?,?,?,?)
                    """), (temporada, exportadora, especie, int(mes),
                           usd_packing, usd_frio, usd_total, tc, now, usuario))
                    cur.execute("SELECT LAST_INSERT_ID()")
                    row = cur.fetchone()
                    record_id = int(row[0]) if row and row[0] is not None else 0
                else:
                    # PostgreSQL
                    cur.execute(db.norm(f"""
                        INSERT INTO {schema}.ppto_ingreso_usd
                            (temporada, exportadora, especie, mes,
                             usd_packing, usd_frio, usd_total, tc, actualizado_en, usuario)
                        VALUES (?,?,?,?,?,?,?,?,?,?)
                        RETURNING id
                    """), (temporada, exportadora, especie, int(mes),
                           usd_packing, usd_frio, usd_total, tc, now, usuario))
                    row = cur.fetchone()
                    record_id = int(row[0]) if row and row[0] is not None else 0

        return jsonify({
            'ok':        True,
            'id':        record_id,
            'usd_total': usd_total,
        })

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return jsonify({'error': str(e)}), 500


@bp.route('/<int:rid>', methods=['DELETE'])
@login_required
def delete_ingreso(rid):
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(db.norm(
            f"DELETE FROM {schema}.ppto_ingreso_usd WHERE id=?"
        ), (rid,))
    return jsonify({'ok': True})


@bp.route('/opciones', methods=['GET'])
@login_required
def get_opciones():
    """
    Devuelve listas de exportadoras, especies, meses y temporadas
    disponibles en ppto_estimacion para poblar los selectores del formulario.
    """
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()

        cur.execute(f"""
            SELECT DISTINCT exportadora FROM {schema}.ppto_estimacion
            WHERE exportadora IS NOT NULL ORDER BY exportadora
        """)
        exportadoras = [r[0] for r in cur.fetchall()]

        cur.execute(f"""
            SELECT DISTINCT especie FROM {schema}.ppto_estimacion
            WHERE especie IS NOT NULL ORDER BY especie
        """)
        especies = [r[0] for r in cur.fetchall()]

        cur.execute(f"""
            SELECT DISTINCT mes FROM {schema}.ppto_estimacion
            WHERE mes IS NOT NULL ORDER BY mes
        """)
        meses_raw = [r[0] for r in cur.fetchall()]

        cur.execute(f"""
            SELECT DISTINCT temporada FROM {schema}.ppto_estimacion
            WHERE temporada IS NOT NULL ORDER BY temporada DESC
        """)
        temporadas = [r[0] for r in cur.fetchall()]

    # Ordenar meses por temporada Nov→Oct
    meses_raw.sort(key=sort_mes_temporada)
    meses = [{'numero': m, 'nombre': MESES_NOMBRE.get(m, str(m))} for m in meses_raw]

    return jsonify({
        'exportadoras': exportadoras,
        'especies':     especies,
        'meses':        meses,
        'temporadas':   temporadas,
    })


@bp.route('/resumen', methods=['GET'])
@login_required
def get_resumen():
    """
    Devuelve los ingresos manuales agrupados.
    ?vista=exportadora_especie (default) — filas por exp+esp, columnas por mes
    ?vista=mes                           — totales por mes
    """
    schema = CFG['DB_SCHEMA']
    db     = get_db()

    # Filtros opcionales
    filtro_exp = request.args.get('exportadora', '')
    filtro_esp = request.args.get('especie', '')
    filtro_tmp = request.args.get('temporada', '')

    conditions = []
    params     = []
    if filtro_exp:
        conditions.append("exportadora=?")
        params.append(filtro_exp)
    if filtro_esp:
        conditions.append("especie=?")
        params.append(filtro_esp)
    if filtro_tmp:
        conditions.append("temporada=?")
        params.append(filtro_tmp)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(db.norm(f"""
            SELECT id, temporada, exportadora, especie, mes,
                   usd_packing, usd_frio, usd_total, tc,
                   CAST(actualizado_en AS VARCHAR(19)) AS actualizado_en,
                   usuario
            FROM {schema}.ppto_ingreso_usd
            {where}
            ORDER BY exportadora, especie, mes
        """), tuple(params))
        rows = db.fetchall_dicts(cur)

    # Normalizar fechas
    for r in rows:
        if r.get('actualizado_en') and not isinstance(r['actualizado_en'], str):
            r['actualizado_en'] = str(r['actualizado_en'])[:19]

    # Ordenar meses disponibles por temporada Nov→Oct
    meses_disponibles = sorted(
        {r['mes'] for r in rows if r.get('mes')},
        key=sort_mes_temporada
    )

    return jsonify({
        'rows':   rows,
        'meses':  meses_disponibles,
        'totales': {
            'usd_packing': sum(r.get('usd_packing') or 0 for r in rows),
            'usd_frio':    sum(r.get('usd_frio')    or 0 for r in rows),
            'usd_total':   sum(r.get('usd_total')   or 0 for r in rows),
        }
    })
