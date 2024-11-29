import csv
import json
import logging
from hdfs import InsecureClient
from datetime import datetime

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

HDFS_URL = 'http://namenode:9870'
HDFS_USER = 'hadoop'

TRANSACTIONS_DIR = '/raw_zone/transactions'
LOGS_DIR = '/raw_zone/logs'
SOCIAL_LOGS_DIR = '/raw_zone/social_logs'
ADS_DIR = '/raw_zone/ads'

# Créer un client HDFS
client = InsecureClient(HDFS_URL, user=HDFS_USER)


def create_directory_hdfs(path):
    """
    Créer un répertoire dans HDFS s'il n'existe pas déjà.
    """
    if not client.status(path, strict=False):
        client.makedirs(path)
        logging.info(f"Répertoire HDFS créé : {path}")
    else:
        logging.info(f"Répertoire HDFS déjà existant : {path}")


def write_transaction_to_hdfs(data):
    """
    Écrit les transactions dans HDFS en CSV.
    """
    date_value = data['date'].split(' ')[0]
    dir_path = f'{TRANSACTIONS_DIR}/{date_value}'
    create_directory_hdfs(dir_path)
    file_path = f'{dir_path}/transactions.csv'
    mode = 'a' if client.status(file_path, strict=False) else 'w'
    with client.write(file_path, mode=mode, encoding='utf-8', overwrite=(mode == 'w')) as file:
        writer = csv.writer(file)
        if mode == 'w':
            writer.writerow(data.keys())
        writer.writerow(data.values())


def write_social_post_to_hdfs(data):
    """
    Écrit les posts sociaux dans HDFS en JSON.
    """
    date_value = datetime.fromtimestamp(
        data["timestamp"] / 1000).strftime('%Y-%m-%d')
    dir_path = f'{SOCIAL_LOGS_DIR}/{date_value}'
    create_directory_hdfs(dir_path)
    file_path = f'{dir_path}/social_posts.json'
    if not client.status(file_path, strict=False):
        with client.write(file_path, encoding='utf-8') as file:
            json.dump([data], file, ensure_ascii=False, indent=4)
            logging.info(
                f"Nouveau fichier créé pour les posts sociaux dans HDFS : {file_path}")
    else:
        with client.read(file_path, encoding='utf-8') as file:
            existing_posts = json.load(file)
        existing_posts.append(data)
        with client.write(file_path, encoding='utf-8', overwrite=True) as file:
            json.dump(existing_posts, file, ensure_ascii=False, indent=4)
            logging.info(f"Post ajouté au fichier HDFS : {file_path}")


def write_ads_to_hdfs(data):
    """
    Écrit les annonces publicitaires dans HDFS en CSV.
    """
    date_value = data['start_date'].split(' ')[0]
    dir_path = f'{ADS_DIR}/{date_value}'
    create_directory_hdfs(dir_path)
    file_path = f'{dir_path}/ads.csv'
    mode = 'a' if client.status(file_path, strict=False) else 'w'
    with client.write(file_path, mode=mode, encoding='utf-8', overwrite=(mode == 'w')) as file:
        writer = csv.writer(file)
        if mode == 'w':
            writer.writerow(data.keys())
        writer.writerow(data.values())


def write_log_to_hdfs(data):
    """
    Écrit les logs du serveur dans HDFS en fichier texte.
    """
    date_value = data['date']
    dir_path = f'{LOGS_DIR}/{date_value}'
    create_directory_hdfs(dir_path)
    file_path = f'{dir_path}/logs.txt'
    mode = 'a' if client.status(file_path, strict=False) else 'w'
    with client.write(file_path, mode=mode, encoding='utf-8', overwrite=(mode == 'w')) as file:
        file.write(json.dumps(data) + "\n")
        logging.info(f"Log ajouté au fichier HDFS : {file_path}")
