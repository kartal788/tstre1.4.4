FROM ghcr.io/astral-sh/uv:debian-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV LANG=en_US.UTF-8
ENV PATH="/app/.venv/bin:$PATH"

# Sistem bağımlılıklarını yükle
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        bash \
        git \
        curl \
        ca-certificates \
        locales && \
    locale-gen en_US.UTF-8 && \
    rm -rf /var/lib/apt/lists/*

# Çalışma dizini
WORKDIR /app

# Kodları kopyala
COPY . .

# UV ortamını kur ve bağımlılıkları yükle
RUN uv sync --no-lock

# start.sh çalıştırılabilir yap
RUN chmod +x start.sh

# Başlatma komutu
CMD ["bash", "start.sh"]
