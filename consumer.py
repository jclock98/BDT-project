import concurrent.futures
import json
import sys

import dns
import pymongo
from confluent_kafka import Consumer, KafkaError, KafkaException

from config import config

RUNNING = True
MIN_COMMIT_COUNT = 2

def msg_process(msg, mongo_session):
    json_to_insert = json.loads(msg.value().decode("utf-8"))
    new_id = "{}_{}_{}".format(str(json_to_insert["code"]),
                                str(json_to_insert["timestamp"]),
                                str(msg.topic()))
    json_to_insert["_id"] = new_id
    db = mongo_session["Cluster0"]
    collection = db[msg.topic()]
    result = collection.insert_one(json_to_insert)


def consume_loop(consumer, topics, mongo_session):
    try:
        consumer.subscribe(topics)

        msg_count = 0
        while RUNNING:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg, mongo_session)
                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(asynchronous=True)
    finally:
        # Close down consumer to commit final offsets.
        print("Close consumer!")
        mongo_session.close()
        consumer.close()


def read_kafka(topic):
    kafka_params = config(section="kafka")
    kafka_params["session.timeout.ms"] = 45000
    kafka_params['group.id'] = 'activities'
    kafka_params['auto.offset.reset'] = 'earliest'
    
    consumer = Consumer(kafka_params)
    
    mongo_params = config(section="mongo")
    mongo_client = pymongo.MongoClient("mongodb+srv://{}:{}@·{}/?retryWrites=true&w=majority".format(mongo_params["user"], 
                                                                                                     mongo_params["password"], 
                                                                                                     mongo_params["host"]))
    consume_loop(consumer, [topic], mongo_client)


def main():
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(read_kafka, ["activities", "facilities"])
    