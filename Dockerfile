# Temel imaj
FROM python:3.13-slim

# Çalışma dizini
WORKDIR /app

# Dosyaları kopyala
COPY . .

# Sanal ortam oluştur ve bağımlılıkları yükle
RUN python3 -m venv .venv \
    && . .venv/bin/activate \
    && pip install --upgrade pip setuptools wheel \
    && pip install "https://github.com/ssut/py-googletrans/archive/refs/tags/v4.0.0.tar.gz" \
    && pip install -r requirements.txt

# start.sh çalıştırılabilir yap (eğer varsa)
RUN chmod +x start.sh

# Konteyner çalıştırıldığında start.sh çalıştır
CMD ["./start.sh"]
