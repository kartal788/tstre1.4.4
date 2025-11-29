FROM ghcr.io/astral-sh/uv:debian-slim

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
        ca-certificates \
        locales && \
    locale-gen en_US.UTF-8 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Projeyi kopyala
COPY . .

# uv.lock oluştur
RUN uv lock

# Sanal ortamı kur ve bağımlılıkları yükle
RUN uv sync

# start.sh çalıştırılabilir yap
RUN chmod +x start.sh

# Container başlatıldığında çalışacak komut
CMD ["bash", "start.sh"]
