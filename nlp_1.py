### 

from io import BytesIO
from dotenv import dotenv_values
import pandas as pd
from helper_funcs import RabbitMQ, setup_logger, clean_text, train_model


config = dotenv_values(".env")
RABBITMQ_HOST = config.get("RABBITMQ_HOST")
EXCHANGE_NAME = config.get("EXCHANGE_NAME")
QUEUE_NAME = config.get("QUEUE_NAME_CLASSIFIER")
CONNECTION_STRING = config.get('PG_CONNECTION_STRING')


CLASS_PIPE, CLASS_ENC = train_model()

def callback(ch, method, properties, body):
    df = pd.read_json(BytesIO(body), dtype={"text": "string",})
    df = df.set_index('id')
    df['text_clean'] = clean_text(df['text'])

    predicted_class_label = CLASS_PIPE.predict(df['text_clean'])
    predicted_class_name = CLASS_ENC.inverse_transform(predicted_class_label)[0]  # single element list

    logger.info(f" [x-1] {df.index[0], predicted_class_name}")


if __name__ == '__main__':
    logger = setup_logger()
    rabbit = RabbitMQ(RABBITMQ_HOST, EXCHANGE_NAME)
    rabbit.setup_queue(queue=QUEUE_NAME, exchange=EXCHANGE_NAME)

    logger.info(' [*] Waiting for logs. To exit press CTRL+C')

    rabbit.read_from_queue(QUEUE_NAME, callback)
