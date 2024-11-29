import os
import time
import pandas as pd
import json
import logging
from kafka import KafkaProducer
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from multiprocessing import Process

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def start_producer(file_path, topic=None):
    logger.info("Démarrage du Kafka Producer pour le fichier %s", file_path)
    if not os.path.exists(file_path):
        logger.error(f"Erreur : Le fichier {file_path} n'existe pas.")
        return

    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            batch_size=16384,
            linger_ms=10,
            compression_type='gzip',
            acks=1,
        )
    except Exception as e:
        logger.error(f"Erreur lors de la connexion à Kafka: {e}")
        return

    topic_name = topic if topic else get_topic_from_file(file_path)
    if topic_name is not False:
        logger.info(f"Topic choisi : {topic_name}")
        try:
            if file_path.endswith(".csv") or file_path.endswith(".json"):
                logger.info(
                    f"Lecture des données à partir du fichier {file_path}")
                data = pd.read_csv(file_path) if file_path.endswith(
                    ".csv") else pd.read_json(file_path)
                for _, record in data.iterrows():
                    try:
                        message = record.to_dict()
                        producer.send(topic_name, message)
                        logger.info(
                            f"Message envoyé au topic {topic_name} : {message}")
                    except Exception as e:
                        logger.error(
                            f"Erreur lors de l'envoi du message CSV/JSON : {e}")
                        logger.error(
                            f"Données problématiques : {record.to_dict()}")
            elif file_path.endswith(".txt"):
                logger.info(
                    f"Lecture des logs à partir du fichier {file_path}")
                with open(file_path, "r") as f:
                    for line in f:
                        try:
                            log_message = {"date": line.split(
                                ' ')[0][1:], "content": line.strip()}
                            producer.send(topic_name, log_message)
                            logger.info(
                                f"Log envoyé au topic {topic_name} : {log_message}")
                        except Exception as e:
                            logger.error(
                                f"Erreur lors de l'envoi du log : {e}")
                            logger.error(f"Log problématique : {line.strip()}")
            elif file_path.endswith(".json"):
                logger.info(
                    f"Lecture des données JSON à partir du fichier {file_path}")
                try:
                    # Charger les données avec Pandas
                    data = pd.read_json(file_path, convert_dates=False)

                    # Convertir la colonne timestamp si elle existe
                    if "timestamp" in data.columns:
                        data["timestamp"] = pd.to_datetime(
                            data["timestamp"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

                    # Envoyer chaque ligne comme un message Kafka
                    for _, record in data.iterrows():
                        try:
                            message = record.to_dict()
                            producer.send(topic_name, json.dumps(
                                message).encode("utf-8"))
                            logger.info(
                                f"Message JSON envoyé au topic {topic_name} : {message}")
                        except Exception as e:
                            logger.error(
                                f"Erreur lors de l'envoi du message JSON : {e}")
                            logger.error(f"Données problématiques : {message}")
                except Exception as e:
                    logger.error(
                        f"Erreur lors du chargement ou du traitement du fichier JSON : {e}")
        except Exception as e:
            logger.error(
                f"Erreur générale lors du traitement des messages : {e}")
        finally:
            producer.close()
            logger.info("Kafka Producer terminé")


def get_topic_from_file(file_path):
    if "transactions" in file_path:
        return "transactions"
    elif "social_posts" in file_path:
        return "social_posts"
    elif "ads" in file_path:
        return "ads"
    elif "server_logs" in file_path:
        return "logs"
    else:
        logger.warning("Aucun topic reconnu, utilisation du topic 'unknown'")
        return False


class FileWatcherHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.src_path.endswith((".csv", ".json", ".txt")):
            logger.info(f"Changement détecté dans : {event.src_path}")
            p = Process(target=start_producer, args=(event.src_path,))
            p.start()


def start_producers_for_existing_files(path):
    processes = []
    for file_name in os.listdir(path):
        full_path = os.path.join(path, file_name)
        if os.path.isfile(full_path) and full_path.endswith((".csv", ".json", ".txt")):
            p = Process(target=start_producer, args=(full_path,))
            p.start()
            processes.append(p)
    for p in processes:
        p.join()


if __name__ == "__main__":
    observer = Observer()
    handler = FileWatcherHandler()
    path = "./raw_data"
    observer.schedule(handler, path=path, recursive=True)
    observer.start()
    start_producers_for_existing_files(path)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
