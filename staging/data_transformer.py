from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, to_date
import logging

# Configuration des logs
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("Data Cleaner")

# Initialiser Spark
spark = SparkSession.builder \
    .appName("Data Cleaning Pipeline Updated") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()

# Chemins HDFS
RAW_PATHS = {
    "transactions": "hdfs://namenode:8020/raw_zone/transactions",
    "ads": "hdfs://namenode:8020/raw_zone/ads",
    "logs": "hdfs://namenode:8020/raw_zone/logs"
}

CLEANSED_PATHS = {
    "transactions": "hdfs://namenode:8020/cleansed_zone/transactions",
    "ads": "hdfs://namenode:8020/cleansed_zone/ads",
    "logs": "hdfs://namenode:8020/cleansed_zone/logs"
}

# Fonctions de nettoyage spécifiques


def clean_transactions(df):
    return df.withColumn(
        "date", to_date(col("date"), "yyyy-MM-dd")
    ).fillna({
        "user_id": -1,
        "total_amount": 0.0,
        "payment_method": "Unknown"
    }).dropna(subset=["date"]).withColumn(
        "is_incomplete",
        col("product_id").isNull() | col("quantity").isNull()
    ).dropDuplicates(["transaction_id"])


def clean_ads(df):
    return df.fillna({
        "ad_name": "Unknown Ad",
        "platform": "Unknown",
        "objective": "Unknown",
        "impressions": 0,
        "clicks": 0,
        "conversions": 0,
        "ctr": 0.0,
        "cpc": 0.0,
        "conversion_rate": 0.0
    }).dropDuplicates(["ad_name", "platform", "objective"])


def clean_logs(df):
    return df.withColumn(
        "response_time",
        regexp_extract(
            col("content"), r"response_time=([\d.]+)s", 1).cast("float")
    ).withColumn(
        "user_id",
        regexp_extract(col("content"), r"user_id=([\d.]+)", 1).cast("float")
    ).drop("content").dropDuplicates()


# Pipeline principal
for data_type, raw_path in RAW_PATHS.items():
    logger.info(f"Traitement des fichiers pour {data_type}...")
    cleansed_path = CLEANSED_PATHS[data_type]

    try:
        # Lire tous les fichiers dans le dossier HDFS
        logger.info(f"Chargement des données depuis {raw_path}...")
        df_raw = spark.read.format("csv").option(
            "header", True).load(f"{raw_path}/*/*.csv")
        if df_raw.rdd.isEmpty():
            logger.warning(
                f"Aucune donnée trouvée dans {raw_path}, passage au suivant.")
            continue

        # Appliquer le nettoyage spécifique
        df_cleaned = (
            clean_transactions(df_raw) if data_type == "transactions"
            else clean_ads(df_raw) if data_type == "ads"
            else clean_logs(df_raw)
        )

        # Sauvegarder les données nettoyées sur HDFS
        logger.info(
            f"Sauvegarde des données nettoyées dans {cleansed_path}...")
        df_cleaned.write.mode("overwrite").partitionBy(
            "date").csv(cleansed_path, header=True)
        logger.info(
            f"Données nettoyées sauvegardées avec succès dans {cleansed_path}")

    except Exception as e:
        logger.error(
            f"Erreur lors du traitement des données pour {data_type} : {e}")
        continue

logger.info("Pipeline terminé.")
