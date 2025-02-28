from kafka import KafkaConsumer
# import os
# from dotenv import load_dotenv

# load_dotenv()

topic_name = "book_topic"
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers="192.168.124.125:9092",
    auto_offset_reset="latest",
    value_deserializer=bytes
)

print("connected", consumer.bootstrap_connected())
print("Receiving data from", topic_name)


def send_book_received_mail(message):
    messages = message.split(',')
    print(f"Sending mail to {messages[2]}")
    print(f"Mail sent to {messages[2]}")
    pass


for message in consumer:
    print("message received: ", message.value.decode('utf-8'))
    message = message.value.decode('utf-8')

    # send_book_received_mail()
    send_book_received_mail(message)
