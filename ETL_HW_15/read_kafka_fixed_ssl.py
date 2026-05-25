from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaReader") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.executor.extraJavaOptions", "-Djavax.net.ssl.trustStore=/home/www/truststore.jks -Djavax.net.ssl.trustStorePassword=changeit") \
    .config("spark.driver.extraJavaOptions", "-Djavax.net.ssl.trustStore=/home/www/truststore.jks -Djavax.net.ssl.trustStorePassword=changeit") \
    .getOrCreate()

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "rc1a-0lo3rq41roj7l669.mdb.yandexcloud.net:9091") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username='kafka' password='12345678';") \
    .option("kafka.ssl.truststore.location", "/home/www/truststore.jks") \
    .option("kafka.ssl.truststore.password", "changeit") \
    .load()

df.selectExpr("CAST(value AS STRING) as message").show(truncate=False)

spark.stop()
EOF
