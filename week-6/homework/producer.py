
from typing import Dict
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

import pandas as pd

import os
import json

class JsonProducer(KafkaProducer):
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)


    @staticmethod
    def read_records(resource_path: str):
        return (pd.read_csv(resource_path)
                .rename(columns={
                    'lpep_pickup_datetime' : 'pickup_datetime',
                    'lpep_dropoff_datetime' : 'dropOff_datetime',
                    'PULocationID': 'PUlocationID',
                    'DOLocationID': 'DOlocationID'
                    })
                .filter(['pickup_datetime','dropOff_datetime','PUlocationID','DOlocationID'])
                .dropna(subset=['PUlocationID', 'DOlocationID'])
                .assign(pickup_datetime=lambda X: pd.to_datetime(X['pickup_datetime']),
                        dropOff_datetime=lambda X: pd.to_datetime(X['dropOff_datetime']))
                .astype({x: 'int' for x in ['PUlocationID', 'DOlocationID']})
                .head(500_000)
        )

    def publish_rides(self, topic: str, df: pd.DataFrame):
        for ride in df.to_dict('records'):
            try:
                record = self.producer.send(topic=topic, key=ride['pickup_datetime'], value=ride)
            except KafkaTimeoutError as e:
                print(e.__str__())
        print(f"{len(df)} rows published")

if __name__ == '__main__':
    config = {
        'bootstrap_servers': 'pkc-lzoyy.europe-west6.gcp.confluent.cloud:9092',
        'key_serializer': lambda key: str(key).encode(),
        'value_serializer': lambda x: json.dumps(x, default=str).encode('utf-8'),
        'security_protocol':"SASL_SSL",
        'sasl_mechanism': 'PLAIN',
        'sasl_plain_username' : os.environ['SASL_PLAIN_USERNAME'],
        'sasl_plain_password' : os.environ['SASL_PLAIN_PASSWORD'],        
    }
    files = {
        'rides_green': './resources/green_tripdata_2019-01.csv.gz',
        'rides_fhv': './resources/fhv_tripdata_2019-01.csv.gz'
    }
    producer = JsonProducer(props=config)

    for topic, path in files.items():
        print(f'producing file {path}, into topic {topic}')
        df = producer.read_records(resource_path=path)
        producer.publish_rides(topic=topic, df=df)