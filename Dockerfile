# ──────────────────────────────────────────────────────────────
#  Dockerfile — Sistema Presupuesto de Ingresos
#  Python 3.12 + ODBC Driver 17 for SQL Server (Debian 12)
# ──────────────────────────────────────────────────────────────
FROM python:3.12-slim-bookworm

# Evitar prompts interactivos durante apt
ENV DEBIAN_FRONTEND=noninteractive

# ── 1. Instalar ODBC Driver 17 para SQL Server ────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl gnupg2 apt-transport-https ca-certificates && \
    # Clave Microsoft
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
        | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    # Repositorio Debian 12
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] \
https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql17 unixodbc-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# ── 2. Dependencias Python ─────────────────────────────────────
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── 3. Código de la aplicación ────────────────────────────────
COPY . .

# Carpeta de uploads (se monta como volumen en producción)
RUN mkdir -p uploads

# ── 4. Puerto y arranque ──────────────────────────────────────
EXPOSE 5050
CMD ["python", "app.py"]
