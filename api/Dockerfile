# Utiliser une image Spark prête à l'emploi
FROM bitnami/spark:latest

USER root

# Définir le répertoire de travail
WORKDIR /app

# Recréer le répertoire manquant et installer Python
RUN mkdir -p /var/lib/apt/lists/partial && \
    apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copier les fichiers nécessaires
COPY requirements.txt .
COPY wait-for-it.sh .
COPY . .

# Installer les dépendances Python
RUN pip3 install --no-cache-dir -r requirements.txt

# Rendre le script wait-for-it.sh exécutable
RUN chmod +x wait-for-it.sh

# Exposer le port pour l'API Flask
EXPOSE 5000

# Commande par défaut
CMD ["./wait-for-it.sh", "namenode:9870", "--timeout=60", "--strict", "--", "python3", "api.py"]
