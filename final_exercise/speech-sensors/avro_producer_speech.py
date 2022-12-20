from datetime import datetime
import os
import random
from typing import Tuple, Union
from uuid import uuid4
from confluent_kafka import SerializingProducer, KafkaError, Message
from confluent_kafka.serialization import StringSerializer, SerializationContext
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv, find_dotenv, set_key
from dataclasses import dataclass
import time
import pandas as pd

@dataclass
class Transmission(object):
    sensor_identifier: str
    parsing_status: bool
    reception: int
    timestamp: Union[int, str, float]
    message_id: str
    parsed_sentence: Union[None, str] = None

sensor_identifiers = ['young-larrar', 'XÆA-12', 'volt', 'hope-center', 'yedd', 'zayad', 'metro', 'aartichoke', 'uvuv', 'agar.io', 'commander-bun-bun', 'teub','semantle']

def transmission_to_dict(transmission: Transmission, ctx: SerializationContext) -> dict:
    """
    Returns a dict representation of a transmission instance for serialization.
    Args:
        transmission (Transmission): transmission instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with transmission attributes to be serialized.
    """
    return dict(
        sensor_identifier = transmission.sensor_identifier,
        parsing_status=transmission.parsing_status,
        reception=transmission.reception,
        timestamp=transmission.timestamp,
        message_id = transmission.message_id,
        parsed_sentence=transmission.parsed_sentence
    )


def delivery_report(err: KafkaError, msg: Message):
    """
    Reports the failure or success of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """
    if err:
        print(f'Delivery failed for Transaction record {msg.key()}: {err}')
        return
    print(f'Message delivered to topic {msg.topic()} partition [{msg.partition()}] @ offset {msg.offset()}')

def fetch_sample(df : pd.DataFrame) -> Transmission:
    """
    Return a random sample from the dataset.
    """
    sensor_identifier = random.choice(sensor_identifiers)
    # parsing_status = random.random() > 0.2 # the chance to get the sentence right
    parsing_status = False
    reception = random.randint(1,10)
    
    parsed_sentence = df.sample()['sentence'] if parsing_status else None

    return Transmission(
        sensor_identifier=sensor_identifier,
        parsing_status=parsing_status,
        reception=reception,
        timestamp=time.time() if random.random()>0.5 else str(datetime.now()),
        message_id=str(uuid4()),
        parsed_sentence=parsed_sentence
    )

    pass

def register_schema(schema_registry_client : SchemaRegistryClient, topic: str) -> None:
    str_value_schema = """
    { "namespace": "confluent.io.examples.serialization.avro",
    "name": "Transmission",
    "type": "record",
    "fields" : [
        {
        "name" : "sensor_identifier",
        "type" : "string"
        },
        {
            "name" : "parsing_status",
            "type" : "boolean"
        },
        {
            "name" : "parsed_sentence",
            "type" : ["null", "string"]
        },
        {
            "name" : "reception",
            "type" : "int"
        },
        {
            "name" : "timestamp",
            "type" : ["int", "string", "float"]
        },
        {
            "name" : "message_id",
            "type" : "string"
        }
    ] 
    }
    """
    avro_value_schema = Schema(str_value_schema, 'AVRO')

    load_dotenv()
    x = os.getenv('BOOTSTRAP', 0)
    BOOTSTRAP = bool(int(os.getenv('BOOTSTRAP',0)))
    if not BOOTSTRAP:
        print('The topic and the schema already exist')
        return
    print('Creating the topic and the schema...')
    # create the topic
    admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})
    admin_client.create_topics([NewTopic(topic, len(sensor_identifiers), 1)]) # number of partitions and replicas, respectively
    # upload the schema to the topic
    schema_registry_client.register_schema(f"{topic}-value", avro_value_schema)
    set_key(find_dotenv(), 'BOOTSTRAP', '0') # change to false

def main():
    topic='speech-sensors'

    schema_registry_conf = {'url': 'http://localhost:8081'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    register_schema(schema_registry_client, topic)
    value_schema = schema_registry_client.get_latest_version(f"{topic}-value").schema.schema_str
    avro_serializer = AvroSerializer(schema_registry_client=schema_registry_client,
                                     schema_str=value_schema,
                                     to_dict=transmission_to_dict)
    producer_conf = {'bootstrap.servers': 'localhost:9092',
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': avro_serializer}
    producer = SerializingProducer(producer_conf)

    print('Reading the sentences...')
    df = pd.read_csv('./sentences.txt',names=['sentence', 'label'], sep='\t')

    print(f'Producing transaction records to topic {topic}. ^C to exit.')
    while True:
        # Serve on_delivery callbacks from previous calls to produce()
        # SIGINT can't be handled when polling, limit timeout to 1 second.
        producer.poll(1.0)
        try:
            transmission = fetch_sample(df)
            producer.produce(topic=topic, key=str(transmission.sensor_identifier), value=transmission,
                             on_delivery=delivery_report)
            time.sleep(1)
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input, discarding record...")
            continue

    print("Flushing records...")
    producer.flush()

if __name__ == '__main__':
    main()