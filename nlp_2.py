### 

from utilities import RabbitMQ, setup_logger
from textblob import TextBlob
from dotenv import dotenv_values
import pandas as pd
from io import BytesIO

config = dotenv_values(".env")
RABBITMQ_HOST = config.get("RABBITMQ_HOST")
EXCHANGE_NAME = config.get("EXCHANGE_NAME")
QUEUE_NAME = config.get("QUEUE_NAME_SENTIMENT")


def callback(ch, method, properties, body):
    df = pd.read_json(BytesIO(body), dtype={"text": "string",})
    df = df.set_index('id')

    trained_sentiment = TextBlob(df["text"].iloc[0]).sentiment
    predicted_subj_score = round(trained_sentiment.subjectivity, 2)
    # df["label_2"] = predicted_subj_score

    logger.info(f" [x-2] {df.index[0], predicted_subj_score}")


if __name__ == '__main__':
    logger = setup_logger()
    rabbit = RabbitMQ(RABBITMQ_HOST, EXCHANGE_NAME)
    rabbit.setup_queue(queue=QUEUE_NAME, exchange=EXCHANGE_NAME)

    logger.info(' [*] Waiting for logs. To exit press CTRL+C')

    rabbit.read_from_queue(QUEUE_NAME, callback)
