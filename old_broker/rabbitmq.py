import pika

class RabbitMQ(object):
    def __init__(self, host='localhost'):
        self.host = host

    def sendMessage(self, userId, message):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        channel = connection.channel()
        channel.queue_declare(queue=userId)

        print(self.__class__.__name__, 'Publishing', message)
        channel.basic_publish(exchange='', routing_key=userId, body=message)
        connection.close()
