import pika
import logging
from sqlalchemy import create_engine


## LOGGER

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
    def __init__(self, host):
        self.channel = self._setup_channel(host)

    def _setup_channel(self, host):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host)
        )
        return connection.channel()

    def ack(self, delivery_tag):
        self.channel.basic_ack(delivery_tag=delivery_tag)

    def setup_exchange(self, exchange, exchange_type):
        self.channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)

    def send_to_exchange(self, exchange, body, routing_key=''):
        self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body)
        return f" [x] Sent {body}"

    def setup_queue(self, queue, exchange, routing_key=''):
        result = self.channel.queue_declare(queue=queue, exclusive=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange=exchange, routing_key=routing_key, queue=queue_name)  # attach to exchange

    def read_from_queue(self, queue, processing_func):
        self.channel.basic_consume(
            queue=queue, on_message_callback=processing_func
        )
        self.channel.start_consuming()

    def close(self):
        self.channel.close()
