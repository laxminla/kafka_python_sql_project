from confluent_kafka import Producer


class KafkaProducer:

    def __init__(self, bootstrap_server,):
        self.bootstrap_server = bootstrap_server
        self.producer = Producer({'bootstrap.servers': self.bootstrap_server})

    def produce_mess(self, topic, message):
        self.producer.produce(topic, message)

    def flush(self):
        self.producer.flush()