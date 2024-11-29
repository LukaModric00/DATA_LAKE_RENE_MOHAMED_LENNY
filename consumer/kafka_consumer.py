import json
import logging
from kafka import KafkaConsumer
from hdfs import InsecureClient
import csv
import datetime
import time
from kafka.admin import KafkaAdminClient
from kafka.errors import NoBrokersAvailable
import requests
from multiprocessing import Process
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
KAFKA_BROKER = 'kafka:9092'
TRANSACTION_TOPIC = 'transactions'
SOCIAL_POSTS_TOPIC = 'social_posts'
ADS_TOPIC = 'ads'
LOGS_TOPIC = 'logs'
HDFS_URL = 'http://namenode:9870'
HDFS_USER = 'hadoop'
TRANSACTIONS_DIR = '/raw_zone/transactions'
LOGS_DIR = '/raw_zone/logs'
SOCIAL_LOGS_DIR = '/raw_zone/social_logs'
ADS_DIR = '/raw_zone/ads'
client = InsecureClient(HDFS_URL, user=HDFS_USER)
TOPICS = [TRANSACTION_TOPIC, SOCIAL_POSTS_TOPIC, ADS_TOPIC, LOGS_TOPIC]


def ensure_hdfs_ready():
    while True:
        try:
            client.status('/', strict=False)
            logging.info("HDFS est prêt.")
            break
        except requests.exceptions.ConnectionError:
            logging.info("HDFS n'est pas encore prêt, attente...")
            time.sleep(10)


def ensure_topics_exist():
    while True:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
            existing_topics = admin_client.list_topics()
            if all(topic in existing_topics for topic in TOPICS):
                logging.info("Tous les topics sont disponibles.")
                break
            else:
                logging.info("Attente de la disponibilité des topics...")
                time.sleep(10)
        except NoBrokersAvailable:
            logging.info("Kafka n'est pas encore prêt, attente...")
            time.sleep(10)


def create_directory_hdfs(path):
    try:
        if not client.status(path, strict=False):
            client.makedirs(path)
            logging.info(f"Répertoire HDFS créé : {path}")
    except Exception as e:
        logging.error(
            f"Erreur lors de la création du répertoire HDFS {path}: {e}")


def consume_messages_for_topic(topic):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        enable_auto_commit=True,
        group_id="my-group",
        max_poll_records=500,
        auto_offset_reset='earliest'
    )
    for message in consumer:
        logging.info(
            f"Message consommé du topic {message.topic}: {message.value}")
        topic = message.topic
        data = message.value
        if topic == TRANSACTION_TOPIC:
            handle_transaction_message(data)
        elif topic == SOCIAL_POSTS_TOPIC:
            handle_social_post_message(data)
        elif topic == ADS_TOPIC:
            handle_ads_message(data)
        elif topic == LOGS_TOPIC:
            handle_log_message(data)


def handle_transaction_message(data):
    try:
        date_value = data['date'].split(' ')[0]
        dir_path = f'{TRANSACTIONS_DIR}/{date_value}'
        create_directory_hdfs(dir_path)
        file_path = f'{dir_path}/transactions.csv'
        file_exists = client.status(file_path, strict=False)
        with client.write(file_path, encoding='utf-8', overwrite=not file_exists, append=file_exists) as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(data.keys())
            writer.writerow(data.values())
    except Exception as e:
        logging.error(
            f"Erreur lors du traitement du message de transaction: {e}")
        logging.error(f"Données: {data}")


def handle_social_post_message(data):
    try:
        date_value = data['timestamp'].split(' ')
        dir_path = f'{SOCIAL_LOGS_DIR}/{date_value}'
        create_directory_hdfs(dir_path)
        file_path = f'{dir_path}/social_posts.json'
        file_exists = client.status(file_path, strict=False)
        if not file_exists:
            with client.write(file_path, encoding='utf-8', overwrite=True) as file:
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
    except Exception as e:
        logging.error(
            f"Erreur lors du traitement du message des posts sociaux: {e}")
        logging.error(f"Données: {data}")


def handle_ads_message(data):
    try:
        date_value = data['start_date'].split(' ')[0]
        dir_path = f'{ADS_DIR}/{date_value}'
        create_directory_hdfs(dir_path)
        file_path = f'{dir_path}/ads.csv'
        file_exists = client.status(file_path, strict=False)
        with client.write(file_path, encoding='utf-8', overwrite=not file_exists, append=file_exists) as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(data.keys())
            writer.writerow(data.values())
    except Exception as e:
        logging.error(
            f"Erreur lors du traitement du message des publicités: {e}")
        logging.error(f"Données: {data}")


def handle_log_message(data):
    try:
        date_value = data['date']
        dir_path = f'{LOGS_DIR}/{date_value}'
        create_directory_hdfs(dir_path)
        file_path = f'{dir_path}/logs.csv'
        file_exists = client.status(file_path, strict=False)

        with client.write(file_path, encoding='utf-8', overwrite=not file_exists, append=file_exists) as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(data.keys())
            writer.writerow(data.values())
    except Exception as e:
        logging.error(f"Erreur lors du traitement du message des logs: {e}")
        logging.error(f"Données: {data}")


def start_consuming():
    processes = []
    for topic in TOPICS:
        p = Process(target=consume_messages_for_topic, args=(topic,))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()


if __name__ == "__main__":
    try:
        ensure_topics_exist()
        ensure_hdfs_ready()
        start_consuming()
    except KeyboardInterrupt:
        logging.info("Arrêt du consommateur de messages Kafka.")
