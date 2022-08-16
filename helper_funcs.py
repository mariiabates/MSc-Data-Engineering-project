import pika
import logging
from sqlalchemy import create_engine

def setup_logger():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    return logger

## EXTERNAL SERVICES

class Postgres:
    def __init__(self, host):
        self.connection = self._setup_connection(host=host)

    def _setup_connection(self, host):
        return create_engine(host)

    def load_data(self, df, table_name, table_index):
        rows_inserted = df.to_sql(
            name=f'{table_name}', 
            con=self.connection, 
            if_exists='append',
            index_label=table_index
        )
        return rows_inserted
    def get_cursor(self):
        return self.connection.connect()

class RabbitMQ:
    def __init__(self, host, exchange):
        self.connection = self._setup_connection(host)
        self.channel = self._setup_exchange(exchange)

    def _setup_connection(self, host):
        return pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
    def _setup_exchange(self, exchange):
        channel = self.connection.channel()
        channel.exchange_declare(exchange=exchange, exchange_type='fanout')
        return channel

    def send_to_exchange(self, exchange, body):
        self.channel.basic_publish(exchange=exchange, routing_key='', body=body)
        return f" [x] Sent {body}"

    def setup_queue(self, queue, exchange):
        result = self.channel.queue_declare(queue=queue, exclusive=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange=exchange, queue=queue_name)  # attach to exchange

    def read_from_queue(self, queue, processing_func):
        self.channel.basic_consume(
            queue=queue, on_message_callback=processing_func, auto_ack=True)

        self.channel.start_consuming()

        print("--FINISHED--")

    def close(self):
        self.connection.close()

## MODEL TRAINING

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from dotenv import dotenv_values
import pandas as pd
def train_model():
    config = dotenv_values(".env")
    CONNECTION_STRING = config.get('PG_CONNECTION_STRING')
    cnx = Postgres(CONNECTION_STRING).get_cursor()

    df_train = pd.read_sql_table('training_data', cnx)
    df_train['text_clean'] = clean_text(df_train['text'])
    X_train = df_train["text_clean"]
    y_train = df_train["label_1"]

    from sklearn.preprocessing import LabelEncoder
    class_encoder = LabelEncoder()
    y_train = class_encoder.fit_transform(y_train)

    classifier_pipe = Pipeline([('tfidf', TfidfVectorizer(preprocessor=None, tokenizer=None)),
                ('regression', LogisticRegression()),
                ])
    classifier_pipe.fit(X_train,y_train)
    print("TRAINED")
    return classifier_pipe, class_encoder

## PRE-PROCESSING

import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
nltk.download('punkt')
from nltk.tokenize import word_tokenize
from nltk.stem.porter import PorterStemmer
import string
# HACK: https://stackoverflow.com/questions/38916452/nltk-download-ssl-certificate-verify-failed
import ssl
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context
def clean_text(column):
    column = column.str.lower()
    column = column.str.replace(f'[{string.punctuation}]', '', regex=True)
    column = column.str.replace('\n',' ', regex=True)
    column = column.apply(word_tokenize)
    stop_words = set(stopwords.words('english'))
    column = column.apply(lambda words: [w for w in words if not w in stop_words])
    column = column.apply(lambda words: [PorterStemmer().stem(word) for word in words]) 
    
    return column.astype(str)