### This module posts labels to the destination table.
# Receive: id & clean text

import json
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from helper_funcs import RabbitMQ, setup_logger
from dotenv import dotenv_values


config = dotenv_values(".env")
RABBITMQ_HOST = config.get('RABBITMQ_HOST', 'localhost')
EXCHANGE_NAME = config.get('POST_EXCHANGE_NAME', 'Post')
CONNECTION_STRING = config.get('PG_CONNECTION_STRING')
QUEUE_NAME = config.get("QUEUE_NAME_POST")
CONTENTFUL = config.get('CONTENTFUL_ROUTING_KEY')
NEWSAPI = config.get('NEWSAPI_ROUTING_KEY')


def callback(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)

    data_target = method.routing_key

    data = json.loads(body)
    id = str(data.get('index', None))
    label_1 = str(data.get('class', ""))
    label_2 = str(data.get('score', ""))

    with pg_client.connect() as connection:
        col_name = 'label_1' if label_1 else 'label_2'
        label = label_1 if label_1 else label_2
        sql = text(
            f"""INSERT INTO {data_target}(id,{col_name}) 
                VALUES (:id, :label) 
                ON CONFLICT (id) 
                DO UPDATE 
                SET {col_name} = :label 
                WHERE {data_target}.id = :id
            """)
        connection.execute(sql, id=id, label=label)
        logger.info(f" [*] ({id}, {label}) inserted into {data_target}")

        # if label_1:
        #     print("label_1")
        #     # connection.execute(
        #     # f"INSERT INTO contentful_db (id, label_1) VALUES ({id}, '{label_1}') ON CONFLICT (id) DO UPDATE SET label_1 = '{label_1}' WHERE contentful_db.id = {id};"
        #     # )

        #     statement = text("""INSERT INTO news_db(id,label_1) VALUES (:id, :label) ON CONFLICT (id) DO UPDATE SET label_1 = :label WHERE news_db.id = :id""")
        #     connection.execute(statement, id=id, label=label_1)

        #     logger.info(f" [*] ({id}, '{label_1}') inserted")
        # elif label_2:
        #     print("label_2")
        #     # connection.execute(
        #     # f"INSERT INTO contentful_db (id, label_2) VALUES ({id}, '{label_2}') ON CONFLICT (id) DO UPDATE SET label_2 = '{label_2}' WHERE contentful_db.id = {id};"
        #     # ) 
            
        #     statement = text("""INSERT INTO news_db(id,label_2) VALUES (:id, :label) ON CONFLICT (id) DO UPDATE SET label_2 = :label WHERE news_db.id = :id""")  # VS VALUES ({id}, '{label_2}')
        #     connection.execute(statement, id=id, label=label_2)
        #     logger.info(f" [*] ({id}, '{label_2}') inserted")

        # else:
        #     pass          
  


if __name__ == '__main__':
    logger = setup_logger()

    rabbit_client = RabbitMQ(RABBITMQ_HOST)

    rabbit_client.setup_exchange(EXCHANGE_NAME, "direct")  # creates if does not exist
    rabbit_client.setup_queue(queue=QUEUE_NAME, exchange=EXCHANGE_NAME, routing_key=NEWSAPI)
    rabbit_client.setup_queue(queue=QUEUE_NAME, exchange=EXCHANGE_NAME, routing_key=CONTENTFUL)

    logger.info(' [*] Waiting for logs. To exit press CTRL+C')

    pg_client = create_engine(CONNECTION_STRING)
    rabbit_client.read_from_queue(QUEUE_NAME, callback)
