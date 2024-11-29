FROM bitnami/spark:latest

# Définir le répertoire de travail
WORKDIR /app

USER root

# Recréer le répertoire manquant et installer les dépendances système
RUN mkdir -p /var/lib/apt/lists/partial && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    build-essential \
    python3-dev \
    libffi-dev \
    libssl-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copier les fichiers nécessaires
COPY data_transformer.py .
COPY requirements.txt .
COPY ../wait-for-it.sh .

# Mise à jour de pip et installation des dépendances Python
RUN pip install --upgrade pip setuptools && \
    pip install --no-cache-dir -r ./requirements.txt

# Rendre le script wait-for-it.sh exécutable
RUN chmod +x ./wait-for-it.sh

# Commande pour lancer le script
CMD ["/bin/bash", "-c", "./wait-for-it.sh namenode:9870 --timeout=60 --strict -- spark-submit --master local[*] /app/data_transformer.py"]
