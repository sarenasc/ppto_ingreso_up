"""
routes/estimacion.py — Endpoints de Estimación.

  POST /api/estimacion/upload          → carga Excel, full refresh
  GET  /api/estimacion/versions        → historial de versiones
  GET  /api/estimacion/pivot           → datos para tabla dinámica
  GET  /api/estimacion/download-ultimo → descarga el último Excel subido
"""

import io
from datetime import datetime
from flask import Blueprint, request, jsonify, send_file
import pandas as pd

from config          import CFG
from database        import get_db
from services.calculos import next_version

from routes.auth import login_required

bp = Blueprint('estimacion', __name__, url_prefix='/api/estimacion')


@login_required
@bp.route('/upload', methods=['POST'])
def upload():
    file        = request.files.get('file')
    observacion = request.form.get('observacion', '')

    if not file:
        return jsonify({'error': 'No se recibió archivo'}), 400

    try:
        file_bytes = file.read()
        df = pd.read_excel(io.BytesIO(file_bytes))
        df.columns = [
            'temporada', 'exportadora', 'especie', 'variedad', 'productor', 'grupo',
            'packing', 'fecha', 'semana', 'mes', 'envase', 'bultos', 'kgs_a_proc',
            'estatus', 'tipo', 'enfriado_aire', 'hidrocoler',
        ]

        version = next_version()
        now     = datetime.now()
        schema  = CFG['DB_SCHEMA']
        db      = get_db()

        def ss(v): return str(v)   if pd.notna(v) else None
        def si(v): return int(v)   if pd.notna(v) else None
        def sf(v): return float(v) if pd.notna(v) else None

        rows = []
        for _, row in df.iterrows():
            fv = row.get('fecha')
            if pd.notna(fv):
                try:    fv = pd.to_datetime(fv).date()
                except: fv = None
            else:
                fv = None
            rows.append((
                ss(row.get('temporada')), ss(row.get('exportadora')),
                ss(row.get('especie')),   ss(row.get('variedad')),
                ss(row.get('productor')), ss(row.get('grupo')),
                ss(row.get('packing')),   fv,
                si(row.get('semana')),    si(row.get('mes')),
                ss(row.get('envase')),    sf(row.get('bultos')),
                sf(row.get('kgs_a_proc')),ss(row.get('estatus')),
                ss(row.get('tipo')),      si(row.get('enfriado_aire')),
                si(row.get('hidrocoler')),version, now, observacion,
            ))

        sql_ins = db.norm(f"""
            INSERT INTO {schema}.ppto_estimacion (
                temporada,exportadora,especie,variedad,productor,grupo,
                packing,fecha,semana,mes,envase,bultos,kgs_a_proc,
                estatus,tipo,enfriado_aire,hidrocoler,
                numero_version,fecha_actualizacion,observacion
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """)

        with db.get_conn() as conn:
            cur = conn.cursor()
            cur.execute(f'DELETE FROM {schema}.ppto_estimacion')
            cur.fast_executemany = True if CFG['DB_ENGINE'] == 'sqlserver' else False
            cur.executemany(sql_ins, rows)
            cur.execute(db.norm(f"""
                INSERT INTO {schema}.ppto_version_log
                    (numero_version, fecha_actualizacion, observacion, filas_cargadas)
                VALUES (?, ?, ?, ?)
            """), (version, now, observacion, len(rows)))

            # Crear unitarios por combinacion exportadora+especie
            combos = df[['exportadora','especie']].dropna().drop_duplicates()
            for _, combo_row in combos.iterrows():
                exp_val = combo_row.get('exportadora')
                esp_val = combo_row.get('especie')
                if exp_val and esp_val:
                    cur.execute(db.norm(f"""
                        INSERT INTO {schema}.ppto_unitarios
                            (exportadora, especie, precio_usd_packing, precio_usd_frio, actualizado_en)
                        SELECT ?, ?, 0, 0, ?
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {schema}.ppto_unitarios
                            WHERE exportadora=? AND especie=?
                        )
                    """), (str(exp_val), str(esp_val), now, str(exp_val), str(esp_val)))
                db.ensure_exportable_exists(cur, schema, str(exp_val), str(esp_val), now)

        # Guardar copia física del archivo
        try:
            with open(CFG['ULTIMO_EXCEL'], 'wb') as f_out:
                f_out.write(file_bytes)
        except Exception as e:
            import warnings; warnings.warn(f'No se guardó copia física: {e}')

        return jsonify({'ok': True, 'version': version, 'filas': len(rows)})

    except Exception as exc:
        import traceback
        return jsonify({'error': str(exc), 'trace': traceback.format_exc()}), 500


@login_required
@bp.route('/versions')
def versions():
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT id, numero_version,
                   CAST(fecha_actualizacion AS VARCHAR(19)) AS fecha_actualizacion,
                   observacion, filas_cargadas
            FROM {schema}.ppto_version_log
            ORDER BY id DESC
        """)
        rows = db.fetchall_dicts(cur)
    # Normalizar fecha a string (para motores que devuelven datetime)
    for r in rows:
        if r.get('fecha_actualizacion') and not isinstance(r['fecha_actualizacion'], str):
            r['fecha_actualizacion'] = str(r['fecha_actualizacion'])[:19]
    return jsonify(rows)


@login_required
@bp.route('/pivot')
def pivot():
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT exportadora, especie, mes, semana,
                   SUM(kgs_a_proc) AS kgs_total
            FROM {schema}.ppto_estimacion
            GROUP BY exportadora, especie, mes, semana
            ORDER BY mes, semana, exportadora, especie
        """)
        rows = db.fetchall_dicts(cur)
    return jsonify(rows)


@login_required
@bp.route('/download-ultimo')
def download_ultimo():
    import os
    path = CFG['ULTIMO_EXCEL']
    if not os.path.exists(path):
        return jsonify({'error': 'Aún no se ha subido ningún archivo de estimación.'}), 404
    return send_file(
        path,
        as_attachment=True,
        download_name='ultima_estimacion.xlsx',
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )
