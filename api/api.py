from flask import Flask, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

app = Flask(__name__)

# Initialiser Spark
spark = SparkSession.builder \
    .appName("API HDFS Data Access") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()

HDFS_PATHS = {
    "transactions": "/cleansed_zone/transactions",
    "ads": "/cleansed_zone/ads",
    "logs": "/cleansed_zone/logs"
}

# Définir un schéma explicite pour les transactions (exemple)
transaction_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("purchase_amount", DoubleType(), True),
    StructField("purchase_date", StringType(), True)
])

# Définir un schéma explicite pour les ads (exemple)
ads_schema = StructType([
    StructField("ad_name", StringType(), True),
    StructField("ctr", DoubleType(), True),
    StructField("impressions", IntegerType(), True),
    StructField("clicks", IntegerType(), True)
])

# Définir un schéma explicite pour les logs (exemple)
logs_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("response_time", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])


@app.route('/api/top-products', methods=['GET'])
def get_top_products():
    try:
        df = spark.read.format("csv").option("header", True).schema(
            transaction_schema).load(HDFS_PATHS["transactions"])
        top_products = df.groupBy("product_id").count().orderBy(
            desc("count")).limit(10)
        result = [row.asDict() for row in top_products.collect()]
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/ads-performance', methods=['GET'])
def get_ads_performance():
    try:
        df = spark.read.format("csv").option("header", True).schema(
            ads_schema).load(HDFS_PATHS["ads"])
        top_ads = df.groupBy("ad_name").avg(
            "ctr").orderBy(desc("avg(ctr)")).limit(10)
        result = [row.asDict() for row in top_ads.collect()]
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/logs-summary', methods=['GET'])
def get_logs_summary():
    try:
        df = spark.read.format("csv").option("header", True).schema(
            logs_schema).load(HDFS_PATHS["logs"])
        logs_summary = df.groupBy("user_id").avg(
            "response_time").orderBy(desc("avg(response_time)")).limit(10)
        result = [row.asDict() for row in logs_summary.collect()]
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
