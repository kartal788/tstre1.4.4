# Temel imaj
FROM python:3.11-slim

# Sistemi güncelle ve gerekli paketleri kur
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    locales \
    && rm -rf /var/lib/apt/lists/*

# Locale ayarları
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# Çalışma dizini
WORKDIR /app

# Proje dosyalarını kopyala
COPY . .

# UV ortamını kur ve bağımlılıkları yükle
# Eğer uv.lock güncel değilse --locked bayrağını kaldırıyoruz
RUN uv sync

# start.sh dosyasına çalıştırma izni ver
RUN chmod +x start.sh

# Varsayılan komut
CMD ["./start.sh"]
