"""
services/calculos.py — Lógica de cálculo de presupuesto.

Contiene:
  - Constantes de meses
  - get_tasa_for_row()  → busca la tasa CLP/USD con la prioridad correcta
  - calcular_fila()     → calcula todos los valores derivados de una fila
  - load_calc_data()    → carga estimación + parámetros desde BD
  - next_version()      → siguiente número de versión
"""

from datetime import datetime
from database import get_db
from config   import CFG

# ── Constantes ────────────────────────────────────────────────────────
MESES_NOMBRE: dict[int, str] = {
    1: "Enero",    2: "Febrero",  3: "Marzo",     4: "Abril",
    5: "Mayo",     6: "Junio",    7: "Julio",      8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
}
MESES_ABREV: dict[int, str] = {k: v[:3].upper() for k, v in MESES_NOMBRE.items()}


# ── Helpers de BD ─────────────────────────────────────────────────────
def next_version() -> int:
    """Devuelve el siguiente número de versión de estimación."""
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()
        cur.execute(db.norm(
            f"SELECT MAX(numero_version) FROM {schema}.ppto_version_log"
        ))
        val = cur.fetchone()[0]
    return (val or 0) + 1


def load_calc_data() -> tuple[list, list, dict, dict]:
    """
    Carga desde BD:
      rows       → filas de ppto_estimacion
      tasas      → tasas de cambio activas
      unitarios  → {especie: {packing: x, frio: y}}
      exportable → {especie: porcentaje_decimal}
    """
    schema = CFG['DB_SCHEMA']
    db     = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()

        cur.execute(f"SELECT * FROM {schema}.ppto_estimacion")
        rows = db.fetchall_dicts(cur)

        cur.execute(db.norm(
            f"SELECT * FROM {schema}.ppto_tasas_cambio WHERE activo=?"
        ), (1,))
        tasas = db.fetchall_dicts(cur)

        cur.execute(
            f"SELECT especie, precio_usd_packing, precio_usd_frio "
            f"FROM {schema}.ppto_unitarios"
        )
        unitarios = {
            r['especie']: {'packing': r['precio_usd_packing'], 'frio': r['precio_usd_frio']}
            for r in db.fetchall_dicts(cur)
        }

        cur.execute(
            f"SELECT especie, porcentaje FROM {schema}.ppto_exportable_pct"
        )
        exportable = {r['especie']: r['porcentaje'] for r in db.fetchall_dicts(cur)}

    return rows, tasas, unitarios, exportable


# ── Cálculo de tasa ───────────────────────────────────────────────────
def get_tasa_for_row(
    tasas: list[dict],
    fecha_str,
    semana,
    mes,
    moneda: str = 'CLP',
) -> float | None:
    """
    Busca la tasa de cambio aplicable con orden de prioridad:
      rango > semanal > mensual > anual

    Filtra por moneda antes de buscar.
    """
    tasas_m = [t for t in tasas if (t.get('moneda') or 'CLP').upper() == moneda.upper()]

    fecha = None
    if fecha_str:
        try:
            fecha = datetime.strptime(str(fecha_str)[:10], '%Y-%m-%d')
        except (ValueError, TypeError):
            pass

    prioridad = {'rango': 0, 'semanal': 1, 'mensual': 2, 'anual': 3}

    for t in sorted(tasas_m, key=lambda x: prioridad.get(x.get('tipo', ''), 9)):
        tipo = t.get('tipo', '')
        if tipo == 'rango' and fecha and t.get('fecha_inicio') and t.get('fecha_fin'):
            try:
                fi = datetime.strptime(str(t['fecha_inicio'])[:10], '%Y-%m-%d')
                ff = datetime.strptime(str(t['fecha_fin'])[:10],    '%Y-%m-%d')
                if fi <= fecha <= ff:
                    return float(t['valor'])
            except (ValueError, TypeError):
                pass
        elif tipo == 'semanal' and t.get('semana') is not None and t.get('semana') == semana:
            return float(t['valor'])
        elif tipo == 'mensual' and t.get('mes') is not None and t.get('mes') == mes:
            return float(t['valor'])
        elif tipo == 'anual':
            return float(t['valor'])

    return None


# ── Cálculo de una fila ───────────────────────────────────────────────
def calcular_fila(
    row: dict,
    unitarios: dict,
    exportable: dict,
    tasas: list,
    moneda: str = 'CLP',
) -> dict:
    """
    Calcula todos los valores derivados de una fila de estimación.

    Retorna:
      kgs            → kilos a procesar
      pct            → % exportable (decimal)
      kg_export      → kgs × pct
      precio_packing → USD/kg (packing)
      precio_frio    → USD/kg (frío)
      usd_packing    → kgs × precio_packing
      usd_frio       → kg_export × precio_frio
      usd_total      → usd_packing + usd_frio
      tasa           → unidades de moneda por 1 USD (o None si no hay tasa)
      clp_total      → usd_total × tasa (valor en la moneda seleccionada)
      moneda         → moneda usada en el cálculo
    """
    esp    = row.get('especie') or ''
    kgs    = float(row.get('kgs_a_proc') or 0)
    semana = row.get('semana')
    mes    = row.get('mes')
    fecha  = str(row['fecha'])[:10] if row.get('fecha') else None

    u              = unitarios.get(esp, {})
    pct            = float(exportable.get(esp) or 0)
    precio_packing = float(u.get('packing') or 0)
    precio_frio    = float(u.get('frio')    or 0)
    tasa           = get_tasa_for_row(tasas, fecha, semana, mes, moneda)

    kg_export   = kgs * pct
    usd_packing = kgs       * precio_packing
    usd_frio    = kg_export * precio_frio
    usd_total   = usd_packing + usd_frio
    clp_total   = usd_total * tasa if tasa is not None else 0.0

    return {
        'kgs':            kgs,
        'pct':            pct,
        'kg_export':      kg_export,
        'precio_packing': precio_packing,
        'precio_frio':    precio_frio,
        'usd_packing':    usd_packing,
        'usd_frio':       usd_frio,
        'usd_total':      usd_total,
        'tasa':           tasa,
        'clp_total':      clp_total,
        'moneda':         moneda,
    }
