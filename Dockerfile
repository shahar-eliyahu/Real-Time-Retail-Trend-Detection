FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV PYSPARK_PYTHON=python \
    PYSPARK_DRIVER_PYTHON=python \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download the Spark-Kafka connector once at build time so containers start offline
RUN python -c "from pyspark.sql import SparkSession; SparkSession.builder.config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3').getOrCreate().stop()"

COPY producer_simulation.py spark_streaming_consumer.py product_trend_labels.csv ./
COPY cleaned_retail_data_with_Y.csv* ./
COPY dashboard ./dashboard
COPY trend_rf_model ./trend_rf_model
