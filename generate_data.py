import pandas as pd
import os
import csv
import json
from datetime import datetime, timedelta
import logging
import schedule
import time
import random
import uuid

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

RAW_DATA_DIR = '../raw_data'
TRANSACTIONS_DIR = '../raw_zone/transactions'
LOGS_DIR = '../raw_zone/logs'
SOCIAL_LOGS_DIR = '../raw_zone/social_logs'
ADS_DIR = '../raw_zone/ads'

# Fonction pour créer un répertoire s'il n'existe pas


def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)
        logging.info(f"Répertoire créé : {path}")
    else:
        logging.info(f"Répertoire déjà existant : {path}")

# Fonction pour générer un nombre aléatoire d'éléments


def generate_random_quantity():
    return random.randint(5, 15)

# Fonction pour générer des transactions


def generate_transactions():
    logging.info("Génération de nouvelles transactions.")

    current_date = datetime.now().date()
    transaction_file_path = f"{TRANSACTIONS_DIR}/{current_date.strftime('%Y-%m-%d')}/transactions.csv"
    create_directory(os.path.dirname(transaction_file_path))

    if os.path.exists(transaction_file_path):
        logging.info(
            "Les transactions du jour existent déjà, aucune nouvelle transaction ajoutée.")
        return

    for _ in range(generate_random_quantity()):
        new_transaction = {
            "transaction_id": str(uuid.uuid4()),
            "user_id": random.randint(1, 10),
            "product_id": random.randint(1, 20),
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "quantity": random.randint(1, 5),
            "total_amount": round(random.uniform(10, 500), 2),
            "payment_method": random.choice(["Cash", "Credit Card", "PayPal"])
        }
        mode = 'a' if os.path.exists(transaction_file_path) else 'w'
        with open(transaction_file_path, mode, newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=new_transaction.keys())
            if mode == 'w':
                writer.writeheader()
            writer.writerow(new_transaction)
        logging.info(f"Nouvelle transaction ajoutée : {new_transaction}")

    logging.info("Fin de la génération des transactions.")

# Fonction pour générer des publications sociales


def generate_social_posts():
    logging.info("Génération de nouvelles publications sociales.")

    current_date = datetime.now().date()
    post_file_path = f"{SOCIAL_LOGS_DIR}/{current_date.strftime('%Y-%m-%d')}/social_posts.json"
    create_directory(os.path.dirname(post_file_path))

    if os.path.exists(post_file_path):
        logging.info(
            "Les publications sociales du jour existent déjà, aucune nouvelle publication ajoutée.")
        return

    new_posts = []
    for _ in range(generate_random_quantity()):
        new_post = {
            "post_id": str(uuid.uuid4()),
            "user_id": random.randint(1, 10),
            "content": f"Achetez un produit exceptionnel {random.randint(1, 100)} !",
            "hashtags": [random.choice(["#deal", "#promo", "#new"])],
            "mentions": [random.randint(1, 10)],
            "timestamp": int(datetime.now().timestamp() * 1000)
        }
        new_posts.append(new_post)

    with open(post_file_path, 'w', encoding='utf-8') as file:
        json.dump(new_posts, file, ensure_ascii=False, indent=4)
    logging.info(f"Nouvelles publications sociales ajoutées : {new_posts}")

    logging.info("Fin de la génération des publications sociales.")

# Fonction pour générer des annonces publicitaires


def generate_ads():
    logging.info("Génération de nouvelles annonces publicitaires.")

    current_date = datetime.now().date()
    ad_file_path = f"{ADS_DIR}/{current_date.strftime('%Y-%m-%d')}/ads.csv"
    create_directory(os.path.dirname(ad_file_path))

    if os.path.exists(ad_file_path):
        logging.info(
            "Les annonces publicitaires du jour existent déjà, aucune nouvelle annonce ajoutée.")
        return

    for _ in range(generate_random_quantity()):
        new_ad = {
            "campaign_id": str(uuid.uuid4()),
            "campaign_name": f"Campagne Générée {current_date.strftime('%Y-%m-%d')}",
            "ad_set_name": f"Groupe Généré {random.choice(['A', 'B', 'C'])}",
            "ad_id": str(uuid.uuid4()),
            "ad_name": f"Annonce Générée {random.randint(1, 100)}",
            "platform": random.choice(["Facebook", "Instagram"]),
            "objective": random.choice(["Conversions", "Engagement", "Traffic"]),
            "start_date": current_date.strftime("%Y-%m-%d"),
            "end_date": (current_date + timedelta(days=random.randint(5, 15))).strftime("%Y-%m-%d"),
            "budget": random.randint(300, 1000),
            "impressions": random.randint(5000, 50000),
            "clicks": random.randint(500, 5000),
            "ctr": round(random.uniform(1.0, 15.0), 2),
            "cpc": round(random.uniform(0.2, 1.0), 2),
            "conversions": random.randint(50, 500),
            "conversion_rate": round(random.uniform(5.0, 20.0), 2),
            "cost_per_conversion": round(random.uniform(1.0, 10.0), 2)
        }
        mode = 'a' if os.path.exists(ad_file_path) else 'w'
        with open(ad_file_path, mode, newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=new_ad.keys())
            if mode == 'w':
                writer.writeheader()
            writer.writerow(new_ad)
        logging.info(f"Nouvelle annonce publicitaire ajoutée : {new_ad}")

    logging.info("Fin de la génération des annonces publicitaires.")

# Fonction pour générer des logs utilisateur


def generate_logs():
    logging.info("Génération de nouveaux logs utilisateur.")

    current_date = datetime.now().date()
    log_file_path = f"{LOGS_DIR}/{current_date.strftime('%Y-%m-%d')}/logs.csv"
    create_directory(os.path.dirname(log_file_path))

    if os.path.exists(log_file_path):
        logging.info(
            "Les logs utilisateur du jour existent déjà, aucun nouveau log ajouté.")
        return

    for _ in range(generate_random_quantity()):
        new_log = {
            "log_id": str(uuid.uuid4()),
            "user_id": random.randint(1, 10),
            "action": random.choice(["login", "logout", "view_product", "add_to_cart", "purchase"]),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        mode = 'a' if os.path.exists(log_file_path) else 'w'
        with open(log_file_path, mode, newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=new_log.keys())
            if mode == 'w':
                writer.writeheader()
            writer.writerow(new_log)
        logging.info(f"Nouveau log utilisateur ajouté : {new_log}")

    logging.info("Fin de la génération des logs utilisateur.")

# Fonction principale


def main():
    logging.info("Début du processus d'organisation des données.")
    generate_transactions()
    generate_social_posts()
    generate_ads()
    generate_logs()
    logging.info("Fin du processus d'organisation des données.")


# Planificateur avec `schedule`
if __name__ == "__main__":
    schedule.every(10).minutes.do(main)

    logging.info("Démarrage du planificateur.")

    while True:
        schedule.run_pending()
        time.sleep(1)
