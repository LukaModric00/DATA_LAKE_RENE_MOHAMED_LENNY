import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from kafka_producer import start_producer
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FileWatcherHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith((".csv", ".json", ".txt")):
            logger.info(f"Changement détecté dans : {event.src_path}")
            start_producer(event.src_path)


if __name__ == "__main__":
    path = "./raw_data"

    if not os.path.exists(path):
        os.makedirs(path)

    logger.info("Traitement des fichiers existants au démarrage")
    for file_name in os.listdir(path):
        file_path = os.path.join(path, file_name)
        if os.path.isfile(file_path) and file_path.endswith((".csv", ".json", ".txt")):
            logger.info(f"Traitement du fichier existant : {file_path}")
            start_producer(file_path)

    observer = Observer()
    handler = FileWatcherHandler()
    observer.schedule(handler, path=path, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
