"""
services/ddl.py — Creación y migración de tablas por motor de BD.

Cada motor tiene su propia sintaxis:
  SQL Server  → IF NOT EXISTS (SELECT ...) CREATE TABLE ...
  PostgreSQL  → CREATE TABLE IF NOT EXISTS ...
  MySQL       → CREATE TABLE IF NOT EXISTS ...  (tipos distintos)
"""

from database import get_db


# ─────────────────────────────────────────────────────────────────────
# SQL SERVER
# ─────────────────────────────────────────────────────────────────────
def _ddl_sqlserver(s: str) -> list[str]:
    return [
        f"""
        IF NOT EXISTS (SELECT * FROM sys.tables
                       WHERE name='ppto_estimacion' AND schema_id=SCHEMA_ID('{s}'))
        CREATE TABLE {s}.ppto_estimacion (
            id                  INT IDENTITY(1,1) PRIMARY KEY,
            temporada           NVARCHAR(20),
            exportadora         NVARCHAR(100),
            especie             NVARCHAR(100),
            variedad            NVARCHAR(100),
            productor           NVARCHAR(200),
            grupo               NVARCHAR(100),
            packing             NVARCHAR(100),
            fecha               DATE,
            semana              INT,
            mes                 INT,
            envase              NVARCHAR(50),
            bultos              FLOAT,
            kgs_a_proc          FLOAT,
            estatus             NVARCHAR(50),
            tipo                NVARCHAR(50),
            enfriado_aire       INT,
            hidrocoler          INT,
            numero_version      INT,
            fecha_actualizacion DATETIME2,
            observacion         NVARCHAR(500)
        )""",
        f"""
        IF NOT EXISTS (SELECT * FROM sys.tables
                       WHERE name='ppto_version_log' AND schema_id=SCHEMA_ID('{s}'))
        CREATE TABLE {s}.ppto_version_log (
            id                  INT IDENTITY(1,1) PRIMARY KEY,
            numero_version      INT,
            fecha_actualizacion DATETIME2,
            observacion         NVARCHAR(500),
            filas_cargadas      INT
        )""",
        f"""
        IF NOT EXISTS (SELECT * FROM sys.tables
                       WHERE name='ppto_tasas_cambio' AND schema_id=SCHEMA_ID('{s}'))
        CREATE TABLE {s}.ppto_tasas_cambio (
            id           INT IDENTITY(1,1) PRIMARY KEY,
            nombre       NVARCHAR(200),
            valor        FLOAT,
            tipo         NVARCHAR(20),
            anio         INT,
            mes          INT,
            semana       INT,
            fecha_inicio DATE,
            fecha_fin    DATE,
            activo       INT DEFAULT 1,
            creado_en    DATETIME2,
            moneda       NVARCHAR(20) NOT NULL DEFAULT 'CLP'
        )""",
        # Migración: agregar columna moneda si tabla ya existía sin ella
        f"""
        IF NOT EXISTS (
            SELECT * FROM sys.columns
            WHERE object_id = OBJECT_ID('{s}.ppto_tasas_cambio') AND name='moneda'
        )
        ALTER TABLE {s}.ppto_tasas_cambio
            ADD moneda NVARCHAR(20) NOT NULL DEFAULT 'CLP'
        """,
        f"""
        IF NOT EXISTS (SELECT * FROM sys.tables
                       WHERE name='ppto_unitarios' AND schema_id=SCHEMA_ID('{s}'))
        CREATE TABLE {s}.ppto_unitarios (
            id                  INT IDENTITY(1,1) PRIMARY KEY,
            especie             NVARCHAR(100) UNIQUE,
            precio_usd_packing  FLOAT NOT NULL DEFAULT 0,
            precio_usd_frio     FLOAT NOT NULL DEFAULT 0,
            actualizado_en      DATETIME2
        )""",
        f"""
        IF NOT EXISTS (
            SELECT * FROM sys.columns
            WHERE object_id = OBJECT_ID('{s}.ppto_unitarios') AND name='precio_usd_packing'
        )
        ALTER TABLE {s}.ppto_unitarios
            ADD precio_usd_packing FLOAT NOT NULL DEFAULT 0,
                precio_usd_frio    FLOAT NOT NULL DEFAULT 0
        """,
        f"""
        IF NOT EXISTS (SELECT * FROM sys.tables
                       WHERE name='ppto_exportable_pct' AND schema_id=SCHEMA_ID('{s}'))
        CREATE TABLE {s}.ppto_exportable_pct (
            id             INT IDENTITY(1,1) PRIMARY KEY,
            especie        NVARCHAR(100) UNIQUE,
            porcentaje     FLOAT,
            actualizado_en DATETIME2
        )""",
    ]


# ─────────────────────────────────────────────────────────────────────
# POSTGRESQL
# ─────────────────────────────────────────────────────────────────────
def _ddl_postgresql(s: str) -> list[str]:
    return [
        f"""CREATE SCHEMA IF NOT EXISTS {s}""",
        f"""
        CREATE TABLE IF NOT EXISTS {s}.ppto_estimacion (
            id                  SERIAL PRIMARY KEY,
            temporada           VARCHAR(20),
            exportadora         VARCHAR(100),
            especie             VARCHAR(100),
            variedad            VARCHAR(100),
            productor           VARCHAR(200),
            grupo               VARCHAR(100),
            packing             VARCHAR(100),
            fecha               DATE,
            semana              INT,
            mes                 INT,
            envase              VARCHAR(50),
            bultos              FLOAT,
            kgs_a_proc          FLOAT,
            estatus             VARCHAR(50),
            tipo                VARCHAR(50),
            enfriado_aire       INT,
            hidrocoler          INT,
            numero_version      INT,
            fecha_actualizacion TIMESTAMP,
            observacion         VARCHAR(500)
        )""",
        f"""
        CREATE TABLE IF NOT EXISTS {s}.ppto_version_log (
            id                  SERIAL PRIMARY KEY,
            numero_version      INT,
            fecha_actualizacion TIMESTAMP,
            observacion         VARCHAR(500),
            filas_cargadas      INT
        )""",
        f"""
        CREATE TABLE IF NOT EXISTS {s}.ppto_tasas_cambio (
            id           SERIAL PRIMARY KEY,
            nombre       VARCHAR(200),
            valor        FLOAT,
            tipo         VARCHAR(20),
            anio         INT,
            mes          INT,
            semana       INT,
            fecha_inicio DATE,
            fecha_fin    DATE,
            activo       INT DEFAULT 1,
            creado_en    TIMESTAMP,
            moneda       VARCHAR(20) NOT NULL DEFAULT 'CLP'
        )""",
        # Migración columna moneda
        f"""
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='{s}' AND table_name='ppto_tasas_cambio'
                  AND column_name='moneda'
            ) THEN
                ALTER TABLE {s}.ppto_tasas_cambio
                    ADD COLUMN moneda VARCHAR(20) NOT NULL DEFAULT 'CLP';
            END IF;
        END $$""",
        f"""
        CREATE TABLE IF NOT EXISTS {s}.ppto_unitarios (
            id                  SERIAL PRIMARY KEY,
            especie             VARCHAR(100) UNIQUE,
            precio_usd_packing  FLOAT NOT NULL DEFAULT 0,
            precio_usd_frio     FLOAT NOT NULL DEFAULT 0,
            actualizado_en      TIMESTAMP
        )""",
        f"""
        CREATE TABLE IF NOT EXISTS {s}.ppto_exportable_pct (
            id             SERIAL PRIMARY KEY,
            especie        VARCHAR(100) UNIQUE,
            porcentaje     FLOAT,
            actualizado_en TIMESTAMP
        )""",
    ]


# ─────────────────────────────────────────────────────────────────────
# MYSQL / MARIADB
# ─────────────────────────────────────────────────────────────────────
def _ddl_mysql(s: str) -> list[str]:
    # En MySQL, s = nombre de la base de datos
    return [
        f"""
        CREATE TABLE IF NOT EXISTS {s}.ppto_estimacion (
            id                  INT AUTO_INCREMENT PRIMARY KEY,
            temporada           VARCHAR(20),
            exportadora         VARCHAR(100),
            especie             VARCHAR(100),
            variedad            VARCHAR(100),
            productor           VARCHAR(200),
            grupo               VARCHAR(100),
            packing             VARCHAR(100),
            fecha               DATE,
            semana              INT,
            mes                 INT,
            envase              VARCHAR(50),
            bultos              DOUBLE,
            kgs_a_proc          DOUBLE,
            estatus             VARCHAR(50),
            tipo                VARCHAR(50),
            enfriado_aire       INT,
            hidrocoler          INT,
            numero_version      INT,
            fecha_actualizacion DATETIME,
            observacion         VARCHAR(500)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
        f"""
        CREATE TABLE IF NOT EXISTS {s}.ppto_version_log (
            id                  INT AUTO_INCREMENT PRIMARY KEY,
            numero_version      INT,
            fecha_actualizacion DATETIME,
            observacion         VARCHAR(500),
            filas_cargadas      INT
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
        f"""
        CREATE TABLE IF NOT EXISTS {s}.ppto_tasas_cambio (
            id           INT AUTO_INCREMENT PRIMARY KEY,
            nombre       VARCHAR(200),
            valor        DOUBLE,
            tipo         VARCHAR(20),
            anio         INT,
            mes          INT,
            semana       INT,
            fecha_inicio DATE,
            fecha_fin    DATE,
            activo       INT DEFAULT 1,
            creado_en    DATETIME,
            moneda       VARCHAR(20) NOT NULL DEFAULT 'CLP'
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
        # Migración columna moneda (MySQL 8+)
        f"""
        ALTER TABLE {s}.ppto_tasas_cambio
            ADD COLUMN IF NOT EXISTS moneda VARCHAR(20) NOT NULL DEFAULT 'CLP'
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {s}.ppto_unitarios (
            id                  INT AUTO_INCREMENT PRIMARY KEY,
            especie             VARCHAR(100) UNIQUE,
            precio_usd_packing  DOUBLE NOT NULL DEFAULT 0,
            precio_usd_frio     DOUBLE NOT NULL DEFAULT 0,
            actualizado_en      DATETIME
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
        f"""
        CREATE TABLE IF NOT EXISTS {s}.ppto_exportable_pct (
            id             INT AUTO_INCREMENT PRIMARY KEY,
            especie        VARCHAR(100) UNIQUE,
            porcentaje     DOUBLE,
            actualizado_en DATETIME
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    ]


# ─────────────────────────────────────────────────────────────────────
# Función principal
# ─────────────────────────────────────────────────────────────────────
_DDL_BUILDERS = {
    'sqlserver':  _ddl_sqlserver,
    'postgresql': _ddl_postgresql,
    'mysql':      _ddl_mysql,
}


def init_db(engine: str, schema: str) -> None:
    """
    Crea/verifica todas las tablas del sistema para el motor indicado.
    Es idempotente: puede ejecutarse múltiples veces sin problema.
    """
    builder = _DDL_BUILDERS.get(engine)
    if builder is None:
        raise ValueError(f"Motor no soportado: {engine}")

    stmts = builder(schema)
    db    = get_db()
    with db.get_conn() as conn:
        cur = conn.cursor()
        for stmt in stmts:
            try:
                cur.execute(stmt.strip())
            except Exception as e:
                # Algunas migraciones pueden fallar si ya están aplicadas
                # (ej: ALTER TABLE en MySQL si la columna ya existe en versiones
                # más antiguas que no soportan IF NOT EXISTS).
                # Solo se advierte, no se interrumpe.
                import warnings
                warnings.warn(f"DDL skipped: {e}")

    print(f"[OK] Tablas verificadas/creadas — motor: {engine}, schema: {schema}")
