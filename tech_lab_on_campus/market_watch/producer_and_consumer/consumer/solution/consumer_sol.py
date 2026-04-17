import pika
import os
from consumer_interface import mqConsumerInterface 
class mqConsumer(mqConsumerInterface):

    def __init__(self, binding_key: str, exchange_name: str, queue_name: str):
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.channel = None
        self.setupRMQConnection()


    
    def setupRMQConnection(self):
        #establish connection to the RabbitMQ service

        con_params =  pika.URLParameters(os.environ["AMQP_URL"])
        connection = pika.BlockingConnection(parameters = con_params)
    
        #declare a queue and exchange
        self.channel = connection.channel()
        exchange = self.channel.exchange_declare(exchange=self.exchange_name)
        
        self.channel.basic_publish(exchange=self.exchange_name, routing_key = self.binding_key, body = "Message",)
        self.channel.queue_declare(queue=self.queue_name,)
        self.channel.queue_bind(queue=self.queue_name, routing_key = self.binding_key , exchange = self.exchange_name)

        #setup callbaack function for queue
        self.channel.basic_consume(self.queue_name, self.on_message_callback, auto_ack=False)
    
    def on_message_callback(self, channel, method_frame, header_frame, body):
        self.channel.basic_ack(method_frame.delivery_tag, False)
        print(body)
        self.channel.close()




    def startConsuming(self):
        print (" [*] waiting for messages. To exit press Ctrl+C")
        self.channel.start_consuming()

    def __del__(self):
        print("Closing RMQ connection on destruction")
    

        

