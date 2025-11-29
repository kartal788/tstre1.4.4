# Temel imaj
FROM python:3.11-slim

# Sistem bağımlılıkları
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    locales \
    xauth \
    && locale-gen en_US.UTF-8 \
    && rm -rf /var/lib/apt/lists/*

# Locale ayarı
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

# Çalışma dizini
WORKDIR /app

# Poetry kurulumu
RUN curl -sSf https://install.python-poetry.org | python3 - \
    && export PATH="/root/.local/bin:$PATH" \
    && ln -s /root/.local/bin/poetry /usr/local/bin/poetry \
    && ln -s /root/.local/bin/uv /usr/local/bin/uv

# Proje dosyalarını kopyala
COPY . .

# UV ortamını kur ve bağımlılıkları yükle
ENV PATH="/root/.local/bin:$PATH"
RUN uv sync

# start.sh dosyasına çalıştırma izni ver
RUN chmod +x start.sh

# Container başladığında çalışacak komut
CMD ["./start.sh"]
