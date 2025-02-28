from kafka import KafkaProducer

kafka_topic = "books"

def run_producer(message):
    producer = KafkaProducer(bootstrap_servers="192.168.124.125:9092")
    
    print("sending data to kafka_topic....")
    print(message)
    
    data = ",".join(message)
    
    producer.send(topic="book_topic", value=data.encode('utf-8'))
    
    print("message successfully sent to kafka_topic")
        
    producer.flush()

    producer.close()


run_producer(["macquena@gmail.com", "tumeric laporte", "12/03/24"])
