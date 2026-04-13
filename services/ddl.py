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


# ─────────────────────────────────────────────────────────────────────
# DDL para tabla de usuarios — se agrega a cada motor
# ─────────────────────────────────────────────────────────────────────

def _usuarios_sqlserver(s: str) -> list[str]:
    return [f"""
        IF NOT EXISTS (SELECT * FROM sys.tables
                       WHERE name='ppto_usuarios' AND schema_id=SCHEMA_ID('{s}'))
        CREATE TABLE {s}.ppto_usuarios (
            id            INT IDENTITY(1,1) PRIMARY KEY,
            username      NVARCHAR(50)  NOT NULL UNIQUE,
            password_hash NVARCHAR(256) NOT NULL,
            activo        INT           NOT NULL DEFAULT 1,
            creado_en     DATETIME2     NOT NULL DEFAULT GETDATE()
        )"""]


def _usuarios_postgresql(s: str) -> list[str]:
    return [f"""
        CREATE TABLE IF NOT EXISTS {s}.ppto_usuarios (
            id            SERIAL        PRIMARY KEY,
            username      VARCHAR(50)   NOT NULL UNIQUE,
            password_hash VARCHAR(256)  NOT NULL,
            activo        INT           NOT NULL DEFAULT 1,
            creado_en     TIMESTAMP     NOT NULL DEFAULT NOW()
        )"""]


def _usuarios_mysql(s: str) -> list[str]:
    return [f"""
        CREATE TABLE IF NOT EXISTS {s}.ppto_usuarios (
            id            INT AUTO_INCREMENT PRIMARY KEY,
            username      VARCHAR(50)  NOT NULL UNIQUE,
            password_hash VARCHAR(256) NOT NULL,
            activo        INT          NOT NULL DEFAULT 1,
            creado_en     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""]


# Inyectar en los builders existentes
_original_builders = dict(_DDL_BUILDERS)

def _wrap(engine_key, usuario_fn):
    original = _original_builders[engine_key]
    def builder(s):
        return original(s) + usuario_fn(s)
    return builder

_DDL_BUILDERS['sqlserver']  = _wrap('sqlserver',  _usuarios_sqlserver)
_DDL_BUILDERS['postgresql'] = _wrap('postgresql', _usuarios_postgresql)
_DDL_BUILDERS['mysql']      = _wrap('mysql',      _usuarios_mysql)


# ─────────────────────────────────────────────────────────────────────
# DDL para tabla de ingreso manual USD
# ─────────────────────────────────────────────────────────────────────

def _ingreso_sqlserver(s: str) -> list[str]:
    return [
        f"""
        IF NOT EXISTS (SELECT * FROM sys.tables
                       WHERE name='ppto_ingreso_usd' AND schema_id=SCHEMA_ID('{s}'))
        CREATE TABLE {s}.ppto_ingreso_usd (
            id            INT IDENTITY(1,1) PRIMARY KEY,
            temporada     NVARCHAR(20)  NOT NULL DEFAULT '',
            exportadora   NVARCHAR(100) NOT NULL,
            especie       NVARCHAR(100) NOT NULL,
            mes           INT           NOT NULL,
            usd_packing   FLOAT         NOT NULL DEFAULT 0,
            usd_frio      FLOAT         NOT NULL DEFAULT 0,
            usd_total     FLOAT         NOT NULL DEFAULT 0,
            tc            FLOAT         NULL,
            actualizado_en DATETIME2    NOT NULL DEFAULT GETDATE(),
            usuario       NVARCHAR(50)  NOT NULL DEFAULT 'sistema',
            CONSTRAINT uq_ingreso UNIQUE (temporada, exportadora, especie, mes)
        )""",
        # Migración: agregar tc a tablas existentes
        f"""
        IF NOT EXISTS (
            SELECT * FROM sys.columns
            WHERE object_id = OBJECT_ID('{s}.ppto_ingreso_usd') AND name='tc'
        )
        ALTER TABLE {s}.ppto_ingreso_usd ADD tc FLOAT NULL
        """,
    ]


def _ingreso_postgresql(s: str) -> list[str]:
    return [
        f"""
        CREATE TABLE IF NOT EXISTS {s}.ppto_ingreso_usd (
            id             SERIAL        PRIMARY KEY,
            temporada      VARCHAR(20)   NOT NULL DEFAULT '',
            exportadora    VARCHAR(100)  NOT NULL,
            especie        VARCHAR(100)  NOT NULL,
            mes            INT           NOT NULL,
            usd_packing    FLOAT         NOT NULL DEFAULT 0,
            usd_frio       FLOAT         NOT NULL DEFAULT 0,
            usd_total      FLOAT         NOT NULL DEFAULT 0,
            tc             FLOAT         NULL,
            actualizado_en TIMESTAMP     NOT NULL DEFAULT NOW(),
            usuario        VARCHAR(50)   NOT NULL DEFAULT 'sistema',
            UNIQUE (temporada, exportadora, especie, mes)
        )""",
        # Migración
        f"""
        DO $$ BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='{s}' AND table_name='ppto_ingreso_usd'
                  AND column_name='tc'
            ) THEN
                ALTER TABLE {s}.ppto_ingreso_usd ADD COLUMN tc FLOAT NULL;
            END IF;
        END $$""",
    ]


def _ingreso_mysql(s: str) -> list[str]:
    return [
        f"""
        CREATE TABLE IF NOT EXISTS {s}.ppto_ingreso_usd (
            id             INT AUTO_INCREMENT PRIMARY KEY,
            temporada      VARCHAR(20)  NOT NULL DEFAULT '',
            exportadora    VARCHAR(100) NOT NULL,
            especie        VARCHAR(100) NOT NULL,
            mes            INT          NOT NULL,
            usd_packing    DOUBLE       NOT NULL DEFAULT 0,
            usd_frio       DOUBLE       NOT NULL DEFAULT 0,
            usd_total      DOUBLE       NOT NULL DEFAULT 0,
            tc             DOUBLE       NULL,
            actualizado_en DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            usuario        VARCHAR(50)  NOT NULL DEFAULT 'sistema',
            UNIQUE KEY uq_ingreso (temporada, exportadora, especie, mes)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
        # Migración
        f"ALTER TABLE {s}.ppto_ingreso_usd ADD COLUMN IF NOT EXISTS tc DOUBLE NULL",
    ]


# Inyectar tabla ingreso en los builders
_orig2 = dict(_DDL_BUILDERS)

def _wrap2(engine_key, ingreso_fn):
    original = _orig2[engine_key]
    def builder(s):
        return original(s) + ingreso_fn(s)
    return builder

_DDL_BUILDERS['sqlserver']  = _wrap2('sqlserver',  _ingreso_sqlserver)
_DDL_BUILDERS['postgresql'] = _wrap2('postgresql', _ingreso_postgresql)
_DDL_BUILDERS['mysql']      = _wrap2('mysql',      _ingreso_mysql)


# ─────────────────────────────────────────────────────────────────────
# Migracion ppto_unitarios: agregar columna exportadora
# Cambia la clave unica de (especie) a (exportadora, especie)
# ─────────────────────────────────────────────────────────────────────

def migrar_unitarios_por_exportadora(engine: str, schema: str) -> None:
    """
    Migra ppto_unitarios para soportar precio por exportadora+especie.
    - Agrega columna exportadora (default '' para registros existentes)
    - Elimina UNIQUE antiguo (especie) y crea UNIQUE (exportadora, especie)
    - Borra los registros sin exportadora (datos previos sin exportadora)
    Idempotente: puede ejecutarse varias veces sin error.
    """
    db = get_db()

    if engine == 'sqlserver':
        stmts = [
            # 1. Agregar columna exportadora si no existe
            f"""
            IF NOT EXISTS (
                SELECT * FROM sys.columns
                WHERE object_id=OBJECT_ID('{schema}.ppto_unitarios') AND name='exportadora'
            )
            ALTER TABLE {schema}.ppto_unitarios
                ADD exportadora NVARCHAR(100) NOT NULL DEFAULT ''
            """,
            # 2. Eliminar UNIQUE antiguo sobre especie sola
            f"""
            DECLARE @con NVARCHAR(200);
            SELECT @con = name FROM sys.key_constraints
            WHERE parent_object_id = OBJECT_ID('{schema}.ppto_unitarios')
              AND type = 'UQ'
              AND name NOT LIKE '%exportadora%';
            IF @con IS NOT NULL
                EXEC('ALTER TABLE {schema}.ppto_unitarios DROP CONSTRAINT [' + @con + ']');
            """,
            # 3. Crear UNIQUE (exportadora, especie) si no existe
            f"""
            IF NOT EXISTS (
                SELECT * FROM sys.key_constraints
                WHERE parent_object_id=OBJECT_ID('{schema}.ppto_unitarios')
                  AND type='UQ' AND name='uq_unitarios_exp_esp'
            )
            ALTER TABLE {schema}.ppto_unitarios
                ADD CONSTRAINT uq_unitarios_exp_esp UNIQUE (exportadora, especie)
            """,
            # 4. Borrar registros sin exportadora (datos viejos sin exportadora)
            f"DELETE FROM {schema}.ppto_unitarios WHERE exportadora = ''",
        ]

    elif engine == 'postgresql':
        stmts = [
            f"""
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema='{schema}' AND table_name='ppto_unitarios'
                      AND column_name='exportadora'
                ) THEN
                    ALTER TABLE {schema}.ppto_unitarios
                        ADD COLUMN exportadora VARCHAR(100) NOT NULL DEFAULT '';
                END IF;
            END $$
            """,
            # Eliminar unique antiguo
            f"""
            DO $$ DECLARE con TEXT;
            BEGIN
                SELECT constraint_name INTO con
                FROM information_schema.table_constraints
                WHERE table_schema='{schema}' AND table_name='ppto_unitarios'
                  AND constraint_type='UNIQUE' AND constraint_name NOT LIKE '%exp%';
                IF con IS NOT NULL THEN
                    EXECUTE 'ALTER TABLE {schema}.ppto_unitarios DROP CONSTRAINT ' || con;
                END IF;
            END $$
            """,
            f"""
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname='uq_unitarios_exp_esp'
                ) THEN
                    ALTER TABLE {schema}.ppto_unitarios
                        ADD CONSTRAINT uq_unitarios_exp_esp UNIQUE (exportadora, especie);
                END IF;
            END $$
            """,
            f"DELETE FROM {schema}.ppto_unitarios WHERE exportadora = ''",
        ]

    elif engine == 'mysql':
        stmts = [
            f"ALTER TABLE {schema}.ppto_unitarios ADD COLUMN IF NOT EXISTS exportadora VARCHAR(100) NOT NULL DEFAULT ''",
            f"""
            ALTER TABLE {schema}.ppto_unitarios
                DROP INDEX IF EXISTS especie
            """,
            f"""
            ALTER TABLE {schema}.ppto_unitarios
                ADD UNIQUE KEY IF NOT EXISTS uq_unitarios_exp_esp (exportadora, especie)
            """,
            f"DELETE FROM {schema}.ppto_unitarios WHERE exportadora = ''",
        ]
    else:
        return

    with db.get_conn() as conn:
        cur = conn.cursor()
        for stmt in stmts:
            try:
                cur.execute(stmt.strip())
            except Exception as e:
                import warnings
                warnings.warn(f'migrar_unitarios step skipped: {e}')

    print(f'[OK] Migracion ppto_unitarios completada — motor: {engine}')


# ─────────────────────────────────────────────────────────────────────
# Migracion ppto_exportable_pct: agregar columna exportadora
# Cambia la clave unica de (especie) a (exportadora, especie)
# ─────────────────────────────────────────────────────────────────────

def migrar_exportable_por_exportadora(engine: str, schema: str) -> None:
    """
    Migra ppto_exportable_pct para soportar porcentaje por exportadora+especie.
    - Agrega columna exportadora (default '' para registros existentes)
    - Elimina UNIQUE antiguo (especie) y crea UNIQUE (exportadora, especie)
    - Borra los registros sin exportadora (datos previos sin exportadora)
    Idempotente: puede ejecutarse varias veces sin error.
    """
    db = get_db()

    if engine == 'sqlserver':
        stmts = [
            f"""
            IF NOT EXISTS (
                SELECT * FROM sys.columns
                WHERE object_id=OBJECT_ID('{schema}.ppto_exportable_pct') AND name='exportadora'
            )
            ALTER TABLE {schema}.ppto_exportable_pct
                ADD exportadora NVARCHAR(100) NOT NULL DEFAULT ''
            """,
            f"""
            DECLARE @con NVARCHAR(200);
            SELECT @con = name FROM sys.key_constraints
            WHERE parent_object_id = OBJECT_ID('{schema}.ppto_exportable_pct')
              AND type = 'UQ'
              AND name NOT LIKE '%exportadora%';
            IF @con IS NOT NULL
                EXEC('ALTER TABLE {schema}.ppto_exportable_pct DROP CONSTRAINT [' + @con + ']');
            """,
            f"""
            IF NOT EXISTS (
                SELECT * FROM sys.key_constraints
                WHERE parent_object_id=OBJECT_ID('{schema}.ppto_exportable_pct')
                  AND type='UQ' AND name='uq_exportable_exp_esp'
            )
            ALTER TABLE {schema}.ppto_exportable_pct
                ADD CONSTRAINT uq_exportable_exp_esp UNIQUE (exportadora, especie)
            """,
            f"DELETE FROM {schema}.ppto_exportable_pct WHERE exportadora = ''",
        ]

    elif engine == 'postgresql':
        stmts = [
            f"""
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema='{schema}' AND table_name='ppto_exportable_pct'
                      AND column_name='exportadora'
                ) THEN
                    ALTER TABLE {schema}.ppto_exportable_pct
                        ADD COLUMN exportadora VARCHAR(100) NOT NULL DEFAULT '';
                END IF;
            END $$
            """,
            f"""
            DO $$ DECLARE con TEXT;
            BEGIN
                SELECT constraint_name INTO con
                FROM information_schema.table_constraints
                WHERE table_schema='{schema}' AND table_name='ppto_exportable_pct'
                  AND constraint_type='UNIQUE' AND constraint_name NOT LIKE '%exp%';
                IF con IS NOT NULL THEN
                    EXECUTE 'ALTER TABLE {schema}.ppto_exportable_pct DROP CONSTRAINT ' || con;
                END IF;
            END $$
            """,
            f"""
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname='uq_exportable_exp_esp'
                ) THEN
                    ALTER TABLE {schema}.ppto_exportable_pct
                        ADD CONSTRAINT uq_exportable_exp_esp UNIQUE (exportadora, especie);
                END IF;
            END $$
            """,
            f"DELETE FROM {schema}.ppto_exportable_pct WHERE exportadora = ''",
        ]

    elif engine == 'mysql':
        stmts = [
            f"ALTER TABLE {schema}.ppto_exportable_pct ADD COLUMN IF NOT EXISTS exportadora VARCHAR(100) NOT NULL DEFAULT ''",
            f"ALTER TABLE {schema}.ppto_exportable_pct DROP INDEX IF EXISTS especie",
            f"ALTER TABLE {schema}.ppto_exportable_pct ADD UNIQUE KEY IF NOT EXISTS uq_exportable_exp_esp (exportadora, especie)",
            f"DELETE FROM {schema}.ppto_exportable_pct WHERE exportadora = ''",
        ]
    else:
        return

    with db.get_conn() as conn:
        cur = conn.cursor()
        for stmt in stmts:
            try:
                cur.execute(stmt.strip())
            except Exception as e:
                import warnings
                warnings.warn(f'migrar_exportable step skipped: {e}')

    print(f'[OK] Migracion ppto_exportable_pct completada — motor: {engine}')
