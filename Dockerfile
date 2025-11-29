FROM python:3.13-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV LANG=en_US.UTF-8
ENV PATH="/app/.venv/bin:$PATH"

# Sistem bağımlılıkları
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        git \
        curl \
        bash \
        locales \
        ca-certificates && \
    locale-gen en_US.UTF-8 && \
    rm -rf /var/lib/apt/lists/*

# Çalışma dizini
WORKDIR /app
COPY . .

# Sanal ortam ve bağımlılıklar
RUN python3 -m venv .venv \
    && . .venv/bin/activate \
    && pip install --upgrade pip setuptools wheel \
    && pip install "https://github.com/ssut/py-googletrans/archive/refs/tags/v4.0.0.tar.gz" \
    && pip install -r requirements.txt

# UV bağımlılıkları (lockfile yok sayılıyor)
RUN . .venv/bin/activate \
    && uv sync --no-lock

# Start script
RUN chmod +x start.sh
CMD ["bash", "start.sh"]
