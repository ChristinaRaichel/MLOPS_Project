from google.cloud import bigquery
from google.cloud import storage
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp, concat, lit,  lower, regexp_replace, array, collect_list, udf, when
from dotenv import load_dotenv
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.types import ArrayType,StringType, IntegerType
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from textblob import TextBlob

load_dotenv()
credentials = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
project_id = os.getenv('GCS_PROJECT_ID')

nltk.download('wordnet') #1
nltk.download('omw-1.4')
nltk.download('stopwords')

spark = SparkSession.builder \
        .appName("GCS to BigQuery ETL") \
        .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.30.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.24-shaded') \
        .config('spark.sql.catalogImplementation', 'in-memory') \
        .config('spark.hadoop.fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem') \
        .config('spark.hadoop.fs.AbstractFileSystem.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS') \
        .config('google.cloud.auth.service.account.enable', 'true') \
        .config('google.cloud.auth.service.account.json.keyfile', os.environ['GOOGLE_APPLICATION_CREDENTIALS']) \
        .getOrCreate()


# Set up BigQuery clien
bigquery_client = bigquery.Client()
dataset_id = os.getenv('DATASET_ID')
table_id = os.getenv('TABLE_ID')

# GCS client
BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
GCS_FILE_PREFIX =  os.getenv('GCS_FILE_PREFIX')

storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

stop_words = set(stopwords.words('english'))
stop_words.discard('not')

# Load files from GCS to BigQuery
blobs = bucket.list_blobs(prefix=GCS_FILE_PREFIX)
#json_files = [f"gs://{BUCKET_NAME}/{blob.name}" for blob in blobs if blob.name.endswith('.json')]
json_files = 'gs://news_api_stream_bucket/news-articles/2024-07-25T23:35:03.000000Z_de8333a6-dac7-45aa-a035-c32df5e6af81.json'
#print(json_files)
df = spark.read.json(json_files)

def label_data(text):
    blob = TextBlob(text)
    if blob.sentiment.polarity >= 0: 
        return 'positive'
    else:
        return 'negative'

def process_df(df):
    df = df.filter(
        col("text").isNotNull() & (col("text") != "") 
        
    )
    #cleaning
    #lowcase #removepunct #removstop #lemm #token
    df = df.withColumn("text", lower(col("text")))
    df = df.withColumn("text", regexp_replace(col("text"),"[^a-zA-Z\s]", ""))

    df = df.filter(
        col("text").isNotNull() & (col("text") != "") 
        #col("last_name").isNotNull() & (col("last_name") != "") &
        
    )
    #cleaning
    #lowcase #removepunct #removstop #lemm #token``
    df = df.withColumn("text", lower(col("text")))
    df = df.withColumn("text", regexp_replace(col("text"),"[^a-zA-Z\s]", ""))

    # Tokenize the text
    tokenizer = Tokenizer(inputCol='text', outputCol='words')
    df = tokenizer.transform(df)

    remover = StopWordsRemover(inputCol="words", outputCol="words_no_stopwords")
    remover.setStopWords(list(stop_words))
    df = remover.transform(df)

    # Initialize the WordNetLemmatizer
    lemmatizer = WordNetLemmatizer()

    # Define a function to lemmatize a list of words
    def lemmatize_words(words):
        return [lemmatizer.lemmatize(word) for word in words]

    # Register the function as a UDF
    lemmatize_words_udf = udf(lemmatize_words, ArrayType(StringType()))

    df = df.withColumn("lemmatized", lemmatize_words_udf("words_no_stopwords"))

    df = df.withColumn("word", explode(col("lemmatized")))

    word_freq = df.groupBy("word", "label").count()

    pivot = word_freq.groupBy("word").pivot("label").sum("count").na.fill(0)

    word_freq_dict = pivot.rdd.map(lambda row: (row["word"], {'negative':row['negative'] if row['negative'] else 0, 'positive': row['positive'] if row['positive'] else 0})).collectAsMap()

    df = df.groupBy("text","label").agg(collect_list("word").alias("words"))


    def get_positive_freq(words):
        return sum(word_freq_dict.get(word)['positive'] for word in words)
    def get_negative_freq(words):
        return sum(word_freq_dict.get(word)['negative'] for word in words)

    positive_freq_udf = udf(get_positive_freq, IntegerType())
    negative_freq_udf = udf(get_negative_freq, IntegerType())

    # Apply UDFs to get positive and negative frequencies for each text
    df = df.withColumn("positive_freq", positive_freq_udf(col("words")))
    df = df.withColumn("negative_freq", negative_freq_udf(col("words")))
    df = df.withColumn("feature", array(lit(1), col("positive_freq"), col("negative_freq")))
    df = df.withColumn("actual_label", when(df.label == 'positive', 1).otherwise(0))
    df = df.select("text","words","feature","actual_label")

    return df

label_udf = udf(label_data, StringType())
df = df.withColumn("timestamp", to_timestamp('published_at',  "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("text", concat(col("title"), lit(" "), col("description"), lit(" "), col("snippet")))
df = df.withColumn("label", label_udf(col("text")))
df = process_df(df)
df.show()

#df = df.select(col("title"), col("description"), col("snippet"),col("language"),col("timestamp"),col("source"),col("categories"))
#df = df.withColumn("categories", explode(col("categories")))

# 1 timestamp

"""df.write \
.format("bigquery") \
.option("table", f"{project_id}:{dataset_id}.{table_id}") \
.option("temporaryGcsBucket", os.getenv('GCS_BUCKET_NAME')) \
.mode("append") \
.save()
"""        
spark.stop()
