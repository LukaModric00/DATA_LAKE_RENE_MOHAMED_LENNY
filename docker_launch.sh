#!/bin/bash

# Stopper les conteneurs existants
docker-compose down --remove-orphans

# Supprimer les volumes existants uniquement si nécessaire
VOLUMES=$(docker volume ls -q | grep 'namenode_data\|datanode_data')
if [ -n "$VOLUMES" ]; then
  echo "Suppression des volumes existants..."
  docker volume rm $VOLUMES
fi

# Recréer les conteneurs et reconstruire les images locales modifiées
echo "Reconstruction des conteneurs Docker..."
docker-compose up --build -d

MAX_RETRIES=30
RETRY_INTERVAL=10

echo "Vérification de la disponibilité du NameNode HDFS sur le port 9870..."
# Attendre que le NameNode soit prêt
for ((i=1; i<=$MAX_RETRIES; i++)); do
  if docker exec namenode curl -s "http://localhost:9870" > /dev/null; then
    echo "Le NameNode est prêt après $i tentatives."
    break
  else
    echo "Le NameNode n'est pas encore prêt. Nouvelle tentative dans $RETRY_INTERVAL secondes... (Tentative $i/$MAX_RETRIES)"
    sleep $RETRY_INTERVAL
  fi

  if [ $i -eq $MAX_RETRIES ]; then
    echo "Erreur : Le NameNode n'a pas démarré après $MAX_RETRIES tentatives."
    exit 1
  fi
done

# Sortir du mode safe mode si le NameNode est prêt
echo "Sortie du Safe Mode de HDFS..."
docker exec namenode hdfs dfsadmin -safemode leave

# Appliquer les permissions au HDFS
echo "Appliquer les permissions au HDFS..."
docker exec namenode hdfs dfs -chmod -R 777 /
docker exec namenode hdfs dfs -chown -R hadoop:hadoop /

# Vérification des fichiers dans HDFS pour s'assurer que tout est opérationnel
docker exec namenode hdfs dfs -ls /

# Finaliser l'initialisation
echo "HDFS est prêt et les permissions sont appliquées."
