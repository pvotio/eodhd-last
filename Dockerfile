# Start with a slim Python 3.13 base image (Debian 12 / bookworm)
FROM python:3.13.7-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install system packages and MS ODBC Driver 18 (no apt-key; use signed-by keyring)
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      curl ca-certificates gnupg \
      unixodbc unixodbc-dev \
 && mkdir -p /usr/share/keyrings \
 && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
    | gpg --dearmor -o /usr/share/keyrings/msprod.gpg \
 && echo "deb [signed-by=/usr/share/keyrings/msprod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
    > /etc/apt/sources.list.d/mssql-release.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
 && rm -rf /var/lib/apt/lists/*

# Set a working directory
WORKDIR /app

# Copy and install Python dependencies
COPY requirements.txt .
# If you expect source builds on 3.13, add build-essential+python3-dev before this step and purge after
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Python script into the container
COPY script.py .

# Run the script when the container starts
CMD ["python", "script.py"]
