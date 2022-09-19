### This module is a producer of data from Database.

import json
from sqlalchemy import create_engine
from helper_funcs import setup_logger, RabbitMQ
from dotenv import dotenv_values


config = dotenv_values(".env")
RABBITMQ_HOST = config.get('RABBITMQ_HOST', 'localhost')
NLP_EXCHANGE_NAME = config.get('NLP_EXCHANGE_NAME', 'NLP')
CONNECTION_STRING = config.get('PG_CONNECTION_STRING')
BBC_SOURCE_TABLE = config.get('BBC_SOURCE_TABLE')


if __name__ == '__main__':
    logger = setup_logger()

    rabbit_client = RabbitMQ(RABBITMQ_HOST)
    rabbit_client.setup_exchange(NLP_EXCHANGE_NAME, "fanout")

    pg_client = create_engine(CONNECTION_STRING)
    with pg_client.connect() as connection:
        for _ in range(1):
            result = connection.execute(f"SELECT * FROM {BBC_SOURCE_TABLE};")
            for row in result:
                json_dict = dict()
                json_dict["id"] = [row[0]]  # list to make it easier to convert to DF
                json_dict["text"] = [row[1]]
                json_dict["source"] = [BBC_SOURCE_TABLE]
                
                json_obj = json.dumps(json_dict)

                status = rabbit_client.send_to_exchange(NLP_EXCHANGE_NAME, json_obj)

                logger.info(status)

    rabbit_client.close()
