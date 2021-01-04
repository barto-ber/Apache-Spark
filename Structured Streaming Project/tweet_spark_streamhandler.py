from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka import KafkaConsumer
from nltk.sentiment.vader import SentimentIntensityAnalyzer


KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'tweets'


# Define Sentiment Analyzer function as UDF to run on Spark (pyspark.sql.functions.udf)
def sent_analyzer(text: str):
    s_analyzer = SentimentIntensityAnalyzer()
    return s_analyzer.polarity_scores(text)
udf_sent_analyzer = udf(sent_analyzer)


if __name__ == "__main__":
    # Create Spark session
    spark = SparkSession\
        .builder\
        .appName("tweet_sentiment_analysis")\
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Input from Kafka
    # Pull data from Kafka topic
    consumer = KafkaConsumer(KAFKA_TOPIC)
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .load()

    # Convert data from Kafka into String type
    df_kafka_string = df_kafka\
        .selectExpr("CAST(value AS STRING) as value",
                    "CAST(timestamp AS LONG) as timestamp")

    # Compute sentiment score
    df_kafka_string_analyzer = df_kafka_string\
        .select('timestamp',
                'value',
                udf_sent_analyzer('value').alias('sentiment_scores'))

    # Split column with Sentiment scores with .select (creating a new df)
    # df_complete = df_kafka_string_analyzer\
    #     .select('timestamp',
    #             'value',
    #             split(column('sentiment_scores'), ',').getItem(0).alias('negative'),
    #             split(column('sentiment_scores'), ',').getItem(1).alias('positive'),
    #             split(column('sentiment_scores'), ',').getItem(2).alias('compound'))

    # Split column with Sentiment scores with .withColumn, completing the existing df.
    df_complete = (df_kafka_string_analyzer\
        .withColumn('negative', split(column('sentiment_scores'), ',').getItem(0))
        .withColumn('positive', split(column('sentiment_scores'), ',').getItem(1))
        .withColumn('compound', split(column('sentiment_scores'), ',').getItem(2)))

    # Print the stream analyze to console
    query = df_complete\
        .writeStream\
        .outputMode('append')\
        .format('console')\
        .start()

    query.awaitTermination()






