# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Ejecutar la aplicación

```bash
# Desde el directorio del proyecto
python app.py
```

El servidor arranca en `http://0.0.0.0:5050` (o el puerto definido en `.env`). Al iniciar, `app.py` ejecuta automáticamente `init_db()`, la migración de unitarios y `ensure_default_user()` antes de crear la app Flask.

## Configuración de entorno

Copiar uno de los archivos de entorno de ejemplo y renombrarlo a `.env`:

- `env.sqlserver` → SQL Server (motor por defecto)
- `env.postgresql` → PostgreSQL
- `env.mysql` → MySQL/MariaDB

Variables clave:

| Variable | Descripción |
|---|---|
| `DB_ENGINE` | `sqlserver` \| `postgresql` \| `mysql` |
| `DB_SCHEMA` | Schema SQL (en MySQL equivale al nombre de la BD) |
| `DB_WINDOWS_AUTH` | `True` para auth integrada (solo SQL Server) |
| `APP_SECRET_KEY` | Clave para sesiones Flask; si está vacía se genera aleatoria en cada reinicio |
| `FLASK_PORT` | Puerto del servidor (default 5050) |

La BD se puede reconfigurar en caliente desde la UI (`/api/config/db`). Tras `save_config()`, siempre llamar a `reset_db()` para destruir el singleton y recriar el adaptador.

## Instalación de dependencias

```bash
pip install -r requirements.txt
# Para PostgreSQL, descomentar psycopg2-binary
# Para MySQL, descomentar PyMySQL
```

## Arquitectura general

### Patrón de base de datos (adaptador por motor)

`database/base.py` define la interfaz `BaseAdapter` (ABC). Cada motor implementa la misma interfaz:

- `database/sqlserver.py` — pyodbc, placeholder `?`, `fast_executemany`
- `database/postgresql.py` — psycopg2, placeholder `%s`
- `database/mysql.py` — PyMySQL, placeholder `%s`

`database/__init__.py` expone un **singleton** mediante `get_db()`. Cuando cambia la configuración de BD, llamar `reset_db()` para forzar recreación.

El método `db.norm(sql)` convierte los `?` de pyodbc a `%s` para los otros motores. Siempre usar `?` como placeholder en el SQL y pasar por `db.norm()` antes de ejecutar.

### Flujo de cálculo del presupuesto

1. **Upload Excel** (`POST /api/estimacion/upload`): reemplaza completamente `ppto_estimacion` con los datos del Excel. Columnas esperadas en orden fijo (sin cabecera alternativa): `temporada, exportadora, especie, variedad, productor, grupo, packing, fecha, semana, mes, envase, bultos, kgs_a_proc, estatus, tipo, enfriado_aire, hidrocoler`.

2. **Cálculo** (`services/calculos.py`):
   - `calcular_fila()`: para cada fila, busca precios unitarios por `(exportadora, especie)` en `ppto_unitarios`, aplica porcentaje exportable de `ppto_exportable_pct`, y tasa de cambio con prioridad: `rango > semanal > mensual > anual`.
   - `aplicar_ingreso_manual()`: si existe un registro en `ppto_ingreso_usd` para `(exportadora, especie, mes)`, sus montos USD **reemplazan** los calculados automáticamente. Los kgs siempre vienen del Excel.

3. **Temporada agrícola**: el orden de meses es Nov→Oct (11,12,1,2,...,10). Usar `sort_mes_temporada()` para ordenar.

### Tablas de BD

| Tabla | Propósito |
|---|---|
| `ppto_estimacion` | Datos de estimación del Excel (se trunca en cada upload) |
| `ppto_version_log` | Historial de uploads con número de versión |
| `ppto_tasas_cambio` | Tasas de cambio por tipo/período y moneda |
| `ppto_unitarios` | Precios USD por `(exportadora, especie)` |
| `ppto_exportable_pct` | % exportable por especie |
| `ppto_ingreso_usd` | Ingresos manuales USD por `(temporada, exportadora, especie, mes)` |
| `ppto_usuarios` | Usuarios del sistema con hash de contraseña (werkzeug) |

`services/ddl.py` crea todas las tablas y aplica migraciones. Es idempotente (puede ejecutarse múltiples veces). `migrar_unitarios_por_exportadora()` convierte la clave única de `especie` a `(exportadora, especie)`.

### Autenticación

Sesiones Flask con `login_required` (decorador en `routes/auth.py`). Usuario inicial: `admin` / `admin1234` (se crea solo si no existe ningún usuario). Sesión válida por 8 horas. La clave de sesión se regenera en cada reinicio si `APP_SECRET_KEY` está vacía.

### Blueprints registrados

| Blueprint | Prefijo | Responsabilidad |
|---|---|---|
| `auth` | `/login`, `/logout`, `/api/usuarios/*`, `/api/me` | Autenticación y usuarios |
| `estimacion` | `/api/estimacion/*` | Upload Excel, versiones, pivot |
| `tasas` | `/api/tasas/*` | Tasas de cambio |
| `unitarios` | `/api/unitarios/*` | Precios por exportadora+especie |
| `exportable` | `/api/exportable/*` | % exportable por especie |
| `presupuesto` | `/api/ppto/*` | Resumen, árbol jerárquico, exportar Excel |
| `ingreso` | `/api/ingreso/*` | Ingresos manuales USD |
| `configuracion` | `/api/config/*` | Configuración de BD en caliente |

### Exportación Excel

`services/exportar.py` → `build_excel()` genera el archivo con `xlsxwriter`. Recibe las mismas estructuras que los endpoints de presupuesto y aplica los mismos overrides de ingreso manual.

## Convenciones importantes

- Todo el SQL usa `?` como placeholder; pasar siempre por `db.norm()` antes de ejecutar.
- `fetchall_dicts(cursor)` convierte las filas en `list[dict]` usando los nombres de columna del cursor.
- Los upserts (`upsert_unitario`, `upsert_exportable`, `ensure_unitario_exists`, `ensure_exportable_exists`) están definidos como métodos abstractos en `BaseAdapter` porque la sintaxis varía por motor (MERGE / ON CONFLICT / ON DUPLICATE KEY).
- El archivo `uploads/ultima_estimacion.xlsx` se sobreescribe en cada upload y permite descargar la última estimación cargada.
