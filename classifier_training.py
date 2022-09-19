### This model provides training and pre-processing functions for nlp_1.

from helper_funcs import setup_logger, Postgres


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


## MODEL TRAINING

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import LabelEncoder
from sklearn.pipeline import Pipeline
from dotenv import dotenv_values
import pandas as pd


def train_model():
    logger = setup_logger()
    config = dotenv_values(".env")
    CONNECTION_STRING = config.get('PG_CONNECTION_STRING')
    cnx = Postgres(CONNECTION_STRING).get_cursor()

    df_train = pd.read_sql_table('training_data', cnx)
    df_train['text_clean'] = clean_text(df_train['text'])
    X_train = df_train["text_clean"]
    y_train = df_train["label"]

    class_encoder = LabelEncoder()
    y_train = class_encoder.fit_transform(y_train)

    classifier_pipe = Pipeline([('tfidf', TfidfVectorizer(preprocessor=None, tokenizer=None)),
                ('regression', LogisticRegression()),
                ])
    classifier_pipe.fit(X_train,y_train)
    logger.info("TRAINED")

    return classifier_pipe, class_encoder
